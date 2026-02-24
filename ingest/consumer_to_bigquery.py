import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, List

from kafka import KafkaConsumer
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

PROJECT_ID = "crypto-volatility-platform"
DATASET = "crypto_raw"
TABLE = "trades"
TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE}"

BOOTSTRAP = "127.0.0.1:9092"
TOPIC = "trades"

BATCH_SIZE = 200          # BigQuery streaming için batch iyi olur
FLUSH_SECONDS = 2         # 2 saniyede bir flush
MAX_RETRIES = 5

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def normalize(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    BigQuery schema'na uygun hale getir.
    Schema:
      event_ts TIMESTAMP
      symbol STRING
      price NUMERIC
      quantity NUMERIC
      trade_id INT64
      is_buyer_maker BOOL
      ingest_ts TIMESTAMP
    """
    # event_ts formatı: ISO8601 string önerilir (Z/offset olabilir)
    event_ts = msg.get("event_ts")
    if not event_ts:
        raise ValueError("event_ts missing")

    return {
        "event_ts": event_ts,
        "symbol": str(msg.get("symbol", "")),
        "price": float(msg.get("price")),
        "quantity": float(msg.get("quantity")),
        "trade_id": int(msg.get("trade_id")),
        "is_buyer_maker": bool(msg.get("is_buyer_maker")),
        "ingest_ts": utc_now_iso(),
    }

def stream_insert(client: bigquery.Client, rows: List[Dict[str, Any]]) -> None:
    """
    insert_rows_json ile streaming insert.
    insertId vererek (trade_id) duplicate riskini azaltır.
    """
    # BigQuery streaming: insertId = idempotency için kritik
    row_ids = [str(r["trade_id"]) for r in rows]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            errors = client.insert_rows_json(TABLE_ID, rows, row_ids=row_ids)
            if errors:
                # errors: [{index: ..., errors: [...]}]
                print("❌ BigQuery insert errors:", errors)
            else:
                print(f"✅ Inserted {len(rows)} rows. last_trade_id={rows[-1]['trade_id']}")
            return
        except GoogleAPIError as e:
            wait = min(2 ** attempt, 20)
            print(f"⚠️ BigQuery API error (attempt {attempt}/{MAX_RETRIES}): {e} -> retry in {wait}s")
            time.sleep(wait)

    print("❌ Giving up after retries.")

def main():
    client = bigquery.Client(project=PROJECT_ID)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="bq-stream-group",
        consumer_timeout_ms=1000,  # poll loop’un dönmesi için
    )

    print("🚀 Kafka -> BigQuery Streaming started")
    print(f"   topic={TOPIC} bootstrap={BOOTSTRAP} table={TABLE_ID}")

    buffer: List[Dict[str, Any]] = []
    last_flush = time.time()

    while True:
        flushed = False

        for message in consumer:
            try:
                row = normalize(message.value)
                buffer.append(row)
            except Exception as e:
                print("⚠️ Bad message skipped:", e, "| raw=", message.value)

            now = time.time()
            if len(buffer) >= BATCH_SIZE or (now - last_flush) >= FLUSH_SECONDS:
                stream_insert(client, buffer)
                buffer.clear()
                last_flush = now
                flushed = True

        # consumer_timeout_ms ile buraya düşer
        now = time.time()
        if buffer and (now - last_flush) >= FLUSH_SECONDS:
            stream_insert(client, buffer)
            buffer.clear()
            last_flush = now
            flushed = True

        if not flushed:
            time.sleep(0.2)

if __name__ == "__main__":
    main()