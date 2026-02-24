import json
from kafka import KafkaProducer
from datetime import datetime, timezone
import time
import random

producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

trade_id = 1

while True:
    msg = {
        "event_ts": datetime.now(timezone.utc).isoformat(),  # TIMESTAMP uyumlu
        "symbol": "BTCUSDT",
        "price": round(50000 + random.uniform(-500, 500), 2),
        "quantity": round(random.uniform(0.001, 0.02), 6),
        "trade_id": trade_id,
        "is_buyer_maker": bool(random.getrandbits(1)),
    }

    producer.send("trades", msg)
    producer.flush()

    print("sent", msg["trade_id"])
    trade_id += 1
    time.sleep(1)