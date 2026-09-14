import json
import random
import time
import uuid

from datetime import datetime

NUM_PRODUCTS = 50000
NUM_SELLERS = 5000
NUM_WAREHOUSES = 100

while True:

    inventory_event = {
        "event_id": str(uuid.uuid4()),
        "sku_id": f"SKU{random.randint(0, NUM_PRODUCTS-1):06}",
        "warehouse_id": f"WH{random.randint(0, NUM_WAREHOUSES-1):03}",
        "quantity": random.randint(0, 500),
        "event_time": datetime.utcnow().isoformat()
    }

    price_event = {
        "event_id": str(uuid.uuid4()),
        "sku_id": f"SKU{random.randint(0, NUM_PRODUCTS-1):06}",
        "new_price": round(
            random.uniform(300, 10000),
            2
        ),
        "event_time": datetime.utcnow().isoformat()
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
        "event_time": datetime.utcnow().isoformat()
    }

    status_event = {
        "event_id": str(uuid.uuid4()),
        "sku_id": f"SKU{random.randint(0, NUM_PRODUCTS-1):06}",
        "status": random.choice([
            "ACTIVE",
            "OUT_OF_STOCK",
            "DISCONTINUED"
        ]),
        "event_time": datetime.utcnow().isoformat()
    }

    print(json.dumps(inventory_event))
    print(json.dumps(price_event))
    print(json.dumps(seller_event))
    print(json.dumps(status_event))

    time.sleep(1)