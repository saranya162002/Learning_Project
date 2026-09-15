import json
import random
import time
import uuid

from datetime import datetime, timezone

from kafka import KafkaProducer

NUM_PRODUCTS = 50000
NUM_SELLERS = 5000
NUM_WAREHOUSES = 100
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC_MAP = {
    "inventory": "inventory_updates",
    "price": "price_updates",
    "seller": "seller_updates",
    "status": "product_status_updates",
}


def wait_for_kafka_connection(producer, timeout_seconds=20):
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        if producer.bootstrap_connected():
            return True
        time.sleep(0.5)

    return False


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: str(key).encode("utf-8"),
    acks="all",
    retries=3,
    linger_ms=10,
)

if not wait_for_kafka_connection(producer):
    raise SystemExit("Kafka connection failed: cannot reach broker at localhost:9092")

while True:

    inventory_event = {
        "event_id": str(uuid.uuid4()),
        "sku_id": f"SKU{random.randint(0, NUM_PRODUCTS-1):06}",
        "warehouse_id": f"WH{random.randint(0, NUM_WAREHOUSES-1):03}",
        "quantity": random.randint(0, 500),
        "event_time": datetime.now(timezone.utc).isoformat()
    }

    price_event = {
        "event_id": str(uuid.uuid4()),
        "sku_id": f"SKU{random.randint(0, NUM_PRODUCTS-1):06}",
        "new_price": round(
            random.uniform(300, 10000),
            2
        ),
        "event_time": datetime.now(timezone.utc).isoformat()
    }

    seller_event = {
        "event_id": str(uuid.uuid4()),
        "seller_id": f"SEL{random.randint(0, NUM_SELLERS-1):05}",
        "rating": round(
            random.uniform(1, 5),
            2
        ),
        "status": random.choice(
            ["ACTIVE", "SUSPENDED"]
        ),
        "event_time": datetime.now(timezone.utc).isoformat()
    }

    status_event = {
        "event_id": str(uuid.uuid4()),
        "sku_id": f"SKU{random.randint(0, NUM_PRODUCTS-1):06}",
        "status": random.choice([
            "ACTIVE",
            "OUT_OF_STOCK",
            "DISCONTINUED"
        ]),
        "event_time": datetime.now(timezone.utc).isoformat()
    }

    for event_name, event in [
        ("inventory", inventory_event),
        ("price", price_event),
        ("seller", seller_event),
        ("status", status_event),
    ]:
        key = event.get("sku_id") or event.get("seller_id")
        topic_name = TOPIC_MAP[event_name]
        future = producer.send(topic_name, key=str(key), value=event)
        future.get(timeout=10)

    producer.flush()
    time.sleep(1)