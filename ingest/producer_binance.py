"""
Crypto WebSocket Trade Stream → Kafka Producer
Topic: trades

Desteklenen exchange'ler (EXCHANGE env değişkeni ile seç):
  kraken  →  wss://ws.kraken.com/v2                  — USD çiftleri, Türkiye erişimli (varsayılan)
  bybit   →  wss://stream.bybit.com/v5/public/spot   — USDT, Türkiye'de engellenebilir
  binance →  wss://stream.binance.com:9443            — USDT, VPN gerekebilir

Kraken WebSocket V2 trade event fields:
  symbol:     trading pair (e.g. "BTC/USD")
  side:       "buy" / "sell"
  price:      float
  qty:        float
  trade_id:   int
  timestamp:  ISO8601 UTC string

Bybit trade event fields:
  T: trade time (ms)
  s: symbol
  p: price
  v: quantity
  i: trade id
  S: side ("Buy"/"Sell")

Binance trade event fields:
  e: event type
  T: trade time (ms)
  s: symbol
  p: price
  q: quantity
  t: trade id
  m: is_buyer_maker
"""

import json
import os
import time
import logging
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Ayarlar ────────────────────────────────────────────────────────────────────
# Exchange seçimi:
#   set EXCHANGE=kraken   → Kraken (USD çiftleri, Türkiye'den erişilebilir) [varsayılan]
#   set EXCHANGE=bybit    → Bybit (USDT çiftleri, Türkiye'de engellenebilir)
#   set EXCHANGE=binance  → Binance Global (USDT çiftleri, VPN gerekebilir)
EXCHANGE = os.environ.get("EXCHANGE", "kraken").lower()

KAFKA_BOOTSTRAP = "127.0.0.1:9092"
KAFKA_TOPIC     = "trades"
RECONNECT_DELAY = 5

# Proxy ayarları (opsiyonel):
#   set HTTPS_PROXY=http://proxyhost:port
#   set HTTPS_PROXY=socks5://proxyhost:port
_raw_proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
PROXY_HOST = None
PROXY_PORT = None
PROXY_TYPE = None
if _raw_proxy:
    import urllib.parse
    _p = urllib.parse.urlparse(_raw_proxy)
    PROXY_TYPE = _p.scheme
    PROXY_HOST = _p.hostname
    PROXY_PORT = _p.port
    log.info("Proxy aktif: %s://%s:%s", PROXY_TYPE, PROXY_HOST, PROXY_PORT)
# ───────────────────────────────────────────────────────────────────────────────

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
)


def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


# ── Kraken ──────────────────────────────────────────────────────────────────────
KRAKEN_WS_URL = "wss://ws.kraken.com/v2"
# Kraken USD çiftlerini USDT karşılığına eşle (BigQuery'deki sembol şemasıyla uyum için)
KRAKEN_SYMBOL_MAP = {
    "BTC/USD": "BTCUSDT",
    "ETH/USD": "ETHUSDT",
}


def kraken_on_open(ws):
    log.info("Kraken WebSocket bağlantısı kuruldu.")
    subscribe_msg = {
        "method": "subscribe",
        "params": {
            "channel": "trade",
            "symbol": list(KRAKEN_SYMBOL_MAP.keys()),
        },
    }
    ws.send(json.dumps(subscribe_msg))
    log.info("Abone olundu: %s", list(KRAKEN_SYMBOL_MAP.keys()))


def kraken_on_message(ws, raw: str):
    try:
        data = json.loads(raw)

        # Sadece "trade" kanalındaki "snapshot" veya "update" mesajlarını işle
        if data.get("channel") != "trade" or data.get("type") not in ("snapshot", "update"):
            return

        for trade in data.get("data", []):
            raw_symbol = trade.get("symbol", "")
            symbol = KRAKEN_SYMBOL_MAP.get(raw_symbol, raw_symbol.replace("/", ""))

            # Kraken: side="buy" → taker alıcı → is_buyer_maker=False
            # Kraken: side="sell" → taker satıcı → is_buyer_maker=True
            is_buyer_maker = trade["side"] == "sell"

            # Kraken timestamp zaten ISO8601 ama "Z" suffix'i olabilir → normalize et
            ts = trade["timestamp"].replace("Z", "+00:00")

            msg = {
                "event_ts":       ts,
                "symbol":         symbol,
                "price":          float(trade["price"]),
                "quantity":       float(trade["qty"]),
                "trade_id":       int(trade["trade_id"]),
                "is_buyer_maker": is_buyer_maker,
                "ingest_ts":      datetime.now(timezone.utc).isoformat(),
            }

            future = producer.send(KAFKA_TOPIC, msg)
            future.add_errback(lambda exc: log.error("Kafka send error: %s", exc))

            log.info("✓ %s | trade_id=%s | price=%s | qty=%s",
                     msg["symbol"], msg["trade_id"], msg["price"], msg["quantity"])

    except Exception as exc:
        log.exception("kraken_on_message error: %s", exc)


# ── Bybit ───────────────────────────────────────────────────────────────────────
BYBIT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
BYBIT_WS_URL  = "wss://stream.bybit.com/v5/public/spot"


def bybit_on_open(ws):
    log.info("Bybit WebSocket bağlantısı kuruldu.")
    subscribe_msg = {
        "op": "subscribe",
        "args": [f"publicTrade.{s}" for s in BYBIT_SYMBOLS],
    }
    ws.send(json.dumps(subscribe_msg))
    log.info("Abone olundu: %s", BYBIT_SYMBOLS)


def bybit_on_message(ws, raw: str):
    try:
        data = json.loads(raw)

        # Abonelik onay mesajlarını atla
        if "op" in data or "topic" not in data:
            return

        for trade in data.get("data", []):
            # Bybit S: "Buy" → taker alıcı → is_buyer_maker=False
            # Bybit S: "Sell" → taker satıcı → is_buyer_maker=True
            is_buyer_maker = trade["S"] == "Sell"

            msg = {
                "event_ts":       ms_to_iso(int(trade["T"])),
                "symbol":         trade["s"],
                "price":          float(trade["p"]),
                "quantity":       float(trade["v"]),
                "trade_id":       trade["i"],
                "is_buyer_maker": is_buyer_maker,
                "ingest_ts":      datetime.now(timezone.utc).isoformat(),
            }

            future = producer.send(KAFKA_TOPIC, msg)
            future.add_errback(lambda exc: log.error("Kafka send error: %s", exc))

            log.info("✓ %s | trade_id=%s | price=%s | qty=%s",
                     msg["symbol"], msg["trade_id"], msg["price"], msg["quantity"])

    except Exception as exc:
        log.exception("bybit_on_message error: %s", exc)


# ── Binance ─────────────────────────────────────────────────────────────────────
BINANCE_SYMBOLS = ["btcusdt", "ethusdt"]
BINANCE_WS_BASE = "wss://stream.binance.com:9443"


def binance_build_url(symbols: list[str]) -> str:
    streams = "/".join(f"{s}@trade" for s in symbols)
    if len(symbols) == 1:
        return f"{BINANCE_WS_BASE}/ws/{streams}"
    return f"{BINANCE_WS_BASE}/stream?streams={streams}"


def binance_on_open(ws):
    log.info("Binance WebSocket bağlantısı kuruldu.")


def binance_on_message(ws, raw: str):
    try:
        data = json.loads(raw)
        if data.get("e") != "trade":
            return

        msg = {
            "event_ts":       ms_to_iso(data["T"]),
            "symbol":         data["s"],
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
        log.exception("binance_on_message error: %s", exc)


# ── Ortak ───────────────────────────────────────────────────────────────────────
def on_error(ws, error):
    log.error("WebSocket error: %s", error)


def on_close(ws, close_status_code, close_msg):
    log.warning("WebSocket kapatıldı (code=%s msg=%s)", close_status_code, close_msg)


def run():
    if EXCHANGE == "binance":
        url        = binance_build_url(BINANCE_SYMBOLS)
        open_fn    = binance_on_open
        message_fn = binance_on_message
    elif EXCHANGE == "bybit":
        url        = BYBIT_WS_URL
        open_fn    = bybit_on_open
        message_fn = bybit_on_message
    else:  # kraken (varsayılan)
        url        = KRAKEN_WS_URL
        open_fn    = kraken_on_open
        message_fn = kraken_on_message

    log.info("Exchange: %s | Bağlanıyor: %s", EXCHANGE.upper(), url)

    while True:
        ws = websocket.WebSocketApp(
            url,
            on_open=open_fn,
            on_message=message_fn,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(
            ping_interval=20,
            ping_timeout=10,
            http_proxy_host=PROXY_HOST,
            http_proxy_port=PROXY_PORT,
            proxy_type=PROXY_TYPE,
        )

        log.warning("%s sn sonra yeniden bağlanıyor...", RECONNECT_DELAY)
        time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    run()
