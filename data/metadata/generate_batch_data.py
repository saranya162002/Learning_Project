import random
import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

from data.metadata.config import *

fake = Faker()

# -----------------------------------------
# Categories
# -----------------------------------------

categories = [
    ("CAT001", "Men"),
    ("CAT002", "Women"),
    ("CAT003", "Footwear"),
    ("CAT004", "Accessories"),
    ("CAT005", "Electronics"),
]

categories_df = pd.DataFrame(
    categories,
    columns=["category_id", "category_name"]
)

# -----------------------------------------
# Brands
# -----------------------------------------

brands = []

for i in range(NUM_BRANDS):
    brands.append([
        f"BR{i:05}",
        fake.company(),
        fake.country()
    ])

brands_df = pd.DataFrame(
    brands,
    columns=[
        "brand_id",
        "brand_name",
        "country"
    ]
)

# -----------------------------------------
# Warehouses
# -----------------------------------------

warehouses = []

for i in range(NUM_WAREHOUSES):
    warehouses.append([
        f"WH{i:03}",
        f"Warehouse-{i}",
        fake.city(),
        fake.state(),
        random.randint(5000, 100000)
    ])

warehouses_df = pd.DataFrame(
    warehouses,
    columns=[
        "warehouse_id",
        "warehouse_name",
        "city",
        "state",
        "capacity"
    ]
)

# -----------------------------------------
# Sellers
# -----------------------------------------

seller_types = [
    "BRAND",
    "MARKETPLACE",
    "DISTRIBUTOR"
]

sellers = []

for i in range(NUM_SELLERS):
    sellers.append([
        f"SEL{i:05}",
        fake.company(),
        random.choice(seller_types),
        fake.city(),
        fake.state(),
        round(random.uniform(2.5, 5.0), 2)
    ])

sellers_df = pd.DataFrame(
    sellers,
    columns=[
        "seller_id",
        "seller_name",
        "seller_type",
        "city",
        "state",
        "rating"
    ]
)

# -----------------------------------------
# Suppliers
# -----------------------------------------

suppliers = []

for i in range(NUM_SUPPLIERS):
    suppliers.append([
        f"SUP{i:05}",
        fake.company(),
        fake.country(),
        random.randint(3, 60)
    ])

suppliers_df = pd.DataFrame(
    suppliers,
    columns=[
        "supplier_id",
        "supplier_name",
        "country",
        "lead_time_days"
    ]
)

# -----------------------------------------
# Products
# -----------------------------------------

product_names = [
    "Jeans",
    "T-Shirt",
    "Sneakers",
    "Laptop",
    "Headphones",
    "Jacket",
    "Watch",
    "Backpack",
    "Saree"
]

products = []
pricing = []
attributes = []
images = []
product_seller_map = []

colors = [
    "Black",
    "Blue",
    "White",
    "Red",
    "Green"
]

sizes = [
    "XS",
    "S",
    "M",
    "L",
    "XL",
    "XXL",
    "XXXL"
]

materials = [
    "Cotton",
    "Polyester",
    "Leather",
    "Denim"
]

for i in range(NUM_PRODUCTS):

    product_id = f"P{i:06}"
    sku_id = f"SKU{i:06}"

    category_id = random.choice(
        categories_df["category_id"].tolist()
    )

    brand_id = random.choice(
        brands_df["brand_id"].tolist()
    )

    product_name = (
        f"{random.choice(product_names)} "
        f"{random.randint(1,999)}"
    )

    products.append([
        product_id,
        sku_id,
        product_name,
        brand_id,
        category_id,
        fake.text(max_nb_chars=100),
        fake.date_between(
            start_date="-3y",
            end_date="today"
        ),
        "ACTIVE"
    ])

    mrp = random.randint(500, 15000)

    selling = round(
        mrp * random.uniform(0.5, 1.0),
        2
    )

    pricing.append([
        str(uuid.uuid4()),
        sku_id,
        mrp,
        selling,
        round(
            (mrp - selling) / mrp * 100,
            2
        ),
        datetime.today().date()
    ])

    attributes.append([
        str(uuid.uuid4()),
        sku_id,
        random.choice(colors),
        random.choice(sizes),
        random.choice(materials),
        random.choice(["Men", "Women"]),
        random.choice(
            ["Summer", "Winter", "All Season"]
        )
    ])

    images.append([
        str(uuid.uuid4()),
        sku_id,
        f"https://cdn.demo.com/{sku_id}.jpg",
        "PRIMARY"
    ])

    seller_count = random.randint(1, 5)

    sellers_selected = random.sample(
        sellers_df["seller_id"].tolist(),
        seller_count
    )

    for seller in sellers_selected:
        product_seller_map.append([
            sku_id,
            seller
        ])

products_df = pd.DataFrame(
    products,
    columns=[
        "product_id",
        "sku_id",
        "product_name",
        "brand_id",
        "category_id",
        "description",
        "launch_date",
        "status"
    ]
)

pricing_df = pd.DataFrame(
    pricing,
    columns=[
        "price_id",
        "sku_id",
        "mrp",
        "selling_price",
        "discount_pct",
        "effective_date"
    ]
)

attributes_df = pd.DataFrame(
    attributes,
    columns=[
        "attribute_id",
        "sku_id",
        "color",
        "size",
        "material",
        "gender",
        "season"
    ]
)

images_df = pd.DataFrame(
    images,
    columns=[
        "image_id",
        "sku_id",
        "image_url",
        "image_type"
    ]
)

product_seller_df = pd.DataFrame(
    product_seller_map,
    columns=[
        "sku_id",
        "seller_id"
    ]
)

# -----------------------------------------
# Write files
# -----------------------------------------

products_df.to_csv(
    BATCH_DIR / "products.csv",
    index=False
)

categories_df.to_csv(
    BATCH_DIR / "categories.csv",
    index=False
)

brands_df.to_csv(
    BATCH_DIR / "brands.csv",
    index=False
)

sellers_df.to_csv(
    BATCH_DIR / "sellers.csv",
    index=False
)

suppliers_df.to_csv(
    BATCH_DIR / "suppliers.csv",
    index=False
)

warehouses_df.to_csv(
    BATCH_DIR / "warehouses.csv",
    index=False
)

pricing_df.to_csv(
    BATCH_DIR / "pricing.csv",
    index=False
)

attributes_df.to_csv(
    BATCH_DIR / "product_attributes.csv",
    index=False
)

images_df.to_csv(
    BATCH_DIR / "product_images.csv",
    index=False
)

product_seller_df.to_csv(
    BATCH_DIR / "product_seller_mapping.csv",
    index=False
)

print("Batch data generated")