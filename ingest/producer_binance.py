"""
Binance WebSocket Trade Stream → Kafka Producer
Topic: trades
Stream: wss://stream.binance.com:9443/ws/<symbol>@trade

Binance trade event fields:
  e: event type
  E: event time (ms)
  s: symbol
  t: trade id
  p: price
  q: quantity
  T: trade time (ms)
  m: is_buyer_maker
"""

import json
import time
import logging
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Ayarlar ────────────────────────────────────────────────────────────────────
SYMBOLS = ["btcusdt", "ethusdt"]          # istediğin coinleri ekle
KAFKA_BOOTSTRAP = "127.0.0.1:9092"
KAFKA_TOPIC = "trades"
RECONNECT_DELAY = 5                        # bağlantı kopunca kaç sn bekle
# ───────────────────────────────────────────────────────────────────────────────

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
)


def ms_to_iso(ms: int) -> str:
    """Binance millisecond timestamp → ISO-8601 string (BigQuery TIMESTAMP uyumlu)."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def on_open(ws):
    log.info("WebSocket bağlantısı kuruldu.")


def on_message(ws, raw: str):
    try:
        data = json.loads(raw)

        # Sadece 'trade' event'lerini işle
        if data.get("e") != "trade":
            return

        msg = {
            "event_ts":       ms_to_iso(data["T"]),          # trade time
            "symbol":         data["s"],                      # "BTCUSDT"
            "price":          float(data["p"]),
            "quantity":       float(data["q"]),
            "trade_id":       int(data["t"]),
            "is_buyer_maker": bool(data["m"]),
            "ingest_ts":      datetime.now(timezone.utc).isoformat(),
        }

        future = producer.send(KAFKA_TOPIC, msg)
        future.add_errback(lambda exc: log.error("Kafka send error: %s", exc))

        log.info("✓ %s | trade_id=%s | price=%s | qty=%s",
                 msg["symbol"], msg["trade_id"], msg["price"], msg["quantity"])

    except Exception as exc:
        log.exception("on_message error: %s", exc)


def on_error(ws, error):
    log.error("WebSocket error: %s", error)


def on_close(ws, close_status_code, close_msg):
    log.warning("WebSocket kapatıldı (code=%s msg=%s)", close_status_code, close_msg)


def build_stream_url(symbols: list[str]) -> str:
    """
    Tekli:   wss://stream.binance.com:9443/ws/btcusdt@trade
    Çoklu:   wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
    """
    streams = "/".join(f"{s}@trade" for s in symbols)
    if len(symbols) == 1:
        return f"wss://stream.binance.com:9443/ws/{streams}"
    return f"wss://stream.binance.com:9443/stream?streams={streams}"


def run():
    url = build_stream_url(SYMBOLS)
    log.info("Bağlanıyor: %s", url)

    while True:
        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=20, ping_timeout=10)

        log.warning("%s sn sonra yeniden bağlanıyor...", RECONNECT_DELAY)
        time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    run()
