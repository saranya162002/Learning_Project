from pathlib import Path

BASE_DIR = Path("output")

BATCH_DIR = BASE_DIR / "batch"
STREAM_DIR = BASE_DIR / "stream"

BATCH_DIR.mkdir(parents=True, exist_ok=True)
STREAM_DIR.mkdir(parents=True, exist_ok=True)

NUM_PRODUCTS = 50000
NUM_BRANDS = 1000
NUM_SELLERS = 5000
NUM_SUPPLIERS = 1000
NUM_WAREHOUSES = 100