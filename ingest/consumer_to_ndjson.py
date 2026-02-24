import json, os, time
from datetime import datetime, timezone
from kafka import KafkaConsumer

OUT_DIR = os.path.join("ingest", "out")
os.makedirs(OUT_DIR, exist_ok=True)

consumer = KafkaConsumer(
    "trades",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="trades-consumer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

BATCH_SIZE = 20
BATCH_SECONDS = 10

batch = []
last_flush = time.time()

def flush(rows):
    if not rows:
        return
    now = datetime.now(timezone.utc)
    date_part = now.strftime("%Y-%m-%d")
    ts_part = now.strftime("%Y%m%dT%H%M%S")
    path = os.path.join(OUT_DIR, f"trades_{date_part}_{ts_part}.ndjson")
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"✅ flushed {len(rows)} rows -> {path}")

print("Consuming... Ctrl+C to stop")
for msg in consumer:
    batch.append(msg.value)
    if len(batch) >= BATCH_SIZE or (time.time() - last_flush) >= BATCH_SECONDS:
        flush(batch)
        batch = []
        last_flush = time.time()