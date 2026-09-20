from pathlib import Path

BASE_DIR = Path("data/metadata/raw_data")

BATCH_DIR = BASE_DIR / "batch"
STREAM_DIR = BASE_DIR / "stream"

BATCH_DIR.mkdir(parents=True, exist_ok=True)
STREAM_DIR.mkdir(parents=True, exist_ok=True)

NUM_PRODUCTS = 5000
NUM_BRANDS = 100
NUM_SELLERS = 50
NUM_SUPPLIERS = 600
NUM_WAREHOUSES = 25