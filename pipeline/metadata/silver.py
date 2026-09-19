from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "metadata" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "metadata" / "silver"

TABLES = {
    "product_master": SILVER_DIR / "product_master.delta",
    "inventory_current": SILVER_DIR / "inventory_current.delta",
    "pricing_history": SILVER_DIR / "pricing_history.delta",
    "seller_current": SILVER_DIR / "seller_current.delta",
    "product_360": SILVER_DIR / "product_360.delta",
}

STREAM_SCHEMAS = {
    "inventory_updates": StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("sku_id", StringType(), True),
            StructField("warehouse_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    ),
    "price_updates": StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("sku_id", StringType(), True),
            StructField("new_price", DecimalType(18, 2), True),
            StructField("event_time", TimestampType(), True),
        ]
    ),
    "seller_updates": StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("seller_id", StringType(), True),
            StructField("rating", DecimalType(18, 2), True),
            StructField("status", StringType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    ),
    "product_status_updates": StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("sku_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    ),
}


def create_spark_session(app_name="metadata-silver"):
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def bronze_path(entity):
    return BRONZE_DIR / f"bronze_{entity}.delta"


def table_exists(path):
    return (Path(path) / "_delta_log").exists()


def read_bronze_file_table(spark, entity):
    path = bronze_path(entity)
    if not table_exists(path):
        raise FileNotFoundError(f"Missing Bronze table: {path}")

    df = spark.read.format("delta").load(str(path))
    return df.select("parsed_payload.*", "ingestion_ts", "source_file_name")


def read_optional_bronze_file_table(spark, entity):
    path = bronze_path(entity)
    if not table_exists(path):
        return None

    return read_bronze_file_table(spark, entity)


def read_bronze_stream_table(spark, entity):
    path = bronze_path(entity)
    if not table_exists(path):
        raise FileNotFoundError(f"Missing Bronze table: {path}")

    schema = STREAM_SCHEMAS[entity]
    df = spark.read.format("delta").load(str(path))
    return df.select(
        F.from_json("raw_payload", schema).alias("payload"),
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingestion_ts",
    ).select(
        "payload.*",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingestion_ts",
    )


def with_columns(df, defaults):
    for column_name, value in defaults.items():
        if column_name not in df.columns:
            df = df.withColumn(column_name, value)

    return df


def latest_by(df, keys, order_columns):
    window = Window.partitionBy(*keys).orderBy(
        *[F.col(column).desc_nulls_last() for column in order_columns]
    )
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def write_silver(df, table_name):
    (
        df.write.format("delta")
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .save(str(TABLES[table_name]))
    )


def build_product_master(spark):
    products = latest_by(
        read_bronze_file_table(spark, "products"),
        ["sku_id"],
        ["ingestion_ts"],
    )
    brands = latest_by(
        read_bronze_file_table(spark, "brands"),
        ["brand_id"],
        ["ingestion_ts"],
    )
    categories = latest_by(
        with_columns(
            read_bronze_file_table(spark, "categories"),
            {"parent_category_id": F.lit(None).cast("string")},
        ),
        ["category_id"],
        ["ingestion_ts"],
    )

    product_attributes = read_optional_bronze_file_table(spark, "product_attributes")
    product_images = read_optional_bronze_file_table(spark, "product_images")
    product_status = None

    if product_attributes is not None:
        product_attributes = latest_by(product_attributes, ["sku_id"], ["ingestion_ts"])

    if product_images is not None:
        product_images = latest_by(
            product_images.filter(F.col("image_type") == "PRIMARY"),
            ["sku_id"],
            ["ingestion_ts"],
        )

    if table_exists(bronze_path("product_status_updates")):
        product_status = latest_by(
            read_bronze_stream_table(spark, "product_status_updates"),
            ["sku_id"],
            ["event_time", "kafka_offset", "ingestion_ts"],
        )

    product_master = (
        products.alias("p")
        .join(brands.alias("b"), "brand_id", "left")
        .join(categories.alias("c"), "category_id", "left")
        .select(
            F.col("p.product_id").alias("product_id"),
            F.col("p.sku_id").alias("sku_id"),
            F.col("p.product_name").alias("product_name"),
            F.col("p.description").alias("description"),
            F.to_date("p.launch_date").alias("launch_date"),
            F.col("p.status").alias("product_status"),
            F.col("p.brand_id").alias("brand_id"),
            F.col("b.brand_name").alias("brand_name"),
            F.col("b.country").alias("brand_country"),
            F.col("p.category_id").alias("category_id"),
            F.col("c.category_name").alias("category_name"),
            F.col("c.parent_category_id").alias("parent_category_id"),
            F.col("p.ingestion_ts").alias("product_ingestion_ts"),
        )
    )

    if product_status is not None:
        product_master = (
            product_master.alias("p")
            .join(
                product_status.select("sku_id", "status", "event_time").alias("s"),
                "sku_id",
                "left",
            )
            .select(
                "p.*",
                F.coalesce(F.col("s.status"), F.col("p.product_status")).alias(
                    "current_product_status"
                ),
                F.col("s.event_time").alias("product_status_event_time"),
            )
            .drop("product_status")
            .withColumnRenamed("current_product_status", "product_status")
        )

    if product_attributes is not None:
        product_master = product_master.join(
            product_attributes.select("sku_id", "color", "size", "material", "gender", "season"),
            "sku_id",
            "left",
        )

    if product_images is not None:
        product_master = product_master.join(
            product_images.select("sku_id", "image_url", "image_type"),
            "sku_id",
            "left",
        )

    return product_master.withColumn("silver_updated_ts", F.current_timestamp())


def build_inventory_current(spark):
    inventory = read_bronze_stream_table(spark, "inventory_updates")
    warehouses = latest_by(
        read_bronze_file_table(spark, "warehouses"),
        ["warehouse_id"],
        ["ingestion_ts"],
    )
    current_inventory = latest_by(
        inventory,
        ["sku_id", "warehouse_id"],
        ["event_time", "kafka_offset", "ingestion_ts"],
    )

    return (
        current_inventory.alias("i")
        .join(warehouses.alias("w"), "warehouse_id", "left")
        .select(
            F.col("i.sku_id").alias("sku_id"),
            F.col("i.warehouse_id").alias("warehouse_id"),
            F.col("w.warehouse_name").alias("warehouse_name"),
            F.col("w.city").alias("warehouse_city"),
            F.col("w.state").alias("warehouse_state"),
            F.col("i.quantity").alias("inventory_qty"),
            F.col("i.event_id").alias("event_id"),
            F.col("i.event_time").alias("event_time"),
            F.col("i.kafka_topic").alias("kafka_topic"),
            F.col("i.kafka_partition").alias("kafka_partition"),
            F.col("i.kafka_offset").alias("kafka_offset"),
        )
        .withColumn("silver_updated_ts", F.current_timestamp())
    )


def batch_pricing_events(spark):
    pricing = read_bronze_file_table(spark, "pricing")
    return pricing.select(
        "sku_id",
        F.col("selling_price").cast(DecimalType(18, 2)).alias("price"),
        F.to_timestamp("effective_date").alias("effective_from"),
        F.lit("batch_pricing").alias("price_source"),
        "price_id",
        F.lit(None).cast("string").alias("event_id"),
        "ingestion_ts",
    )


def stream_pricing_events(spark):
    if not table_exists(bronze_path("price_updates")):
        return None

    price_updates = read_bronze_stream_table(spark, "price_updates")
    return price_updates.select(
        "sku_id",
        F.col("new_price").cast(DecimalType(18, 2)).alias("price"),
        F.col("event_time").alias("effective_from"),
        F.lit("price_updates").alias("price_source"),
        F.lit(None).cast("string").alias("price_id"),
        "event_id",
        "ingestion_ts",
    )


def build_pricing_history(spark):
    pricing_events = batch_pricing_events(spark)
    price_updates = stream_pricing_events(spark)

    if price_updates is not None:
        pricing_events = pricing_events.unionByName(price_updates)

    pricing_events = pricing_events.filter(
        F.col("sku_id").isNotNull() & F.col("effective_from").isNotNull()
    )
    window = Window.partitionBy("sku_id").orderBy("effective_from", "ingestion_ts")

    return (
        pricing_events.withColumn("effective_to", F.lead("effective_from").over(window))
        .withColumn("is_current", F.col("effective_to").isNull())
        .withColumn("price_version", F.row_number().over(window))
        .select(
            "sku_id",
            "price_version",
            "price",
            "effective_from",
            "effective_to",
            "is_current",
            "price_source",
            "price_id",
            "event_id",
        )
        .withColumn("silver_updated_ts", F.current_timestamp())
    )


def build_seller_current(spark):
    sellers = latest_by(
        read_bronze_file_table(spark, "sellers"),
        ["seller_id"],
        ["ingestion_ts"],
    )

    if table_exists(bronze_path("seller_updates")):
        seller_updates = latest_by(
            read_bronze_stream_table(spark, "seller_updates"),
            ["seller_id"],
            ["event_time", "kafka_offset", "ingestion_ts"],
        )
        sellers = (
            sellers.alias("s")
            .join(seller_updates.alias("u"), "seller_id", "left")
            .select(
                "seller_id",
                F.col("s.seller_name").alias("seller_name"),
                F.col("s.seller_type").alias("seller_type"),
                F.col("s.city").alias("city"),
                F.col("s.state").alias("state"),
                F.coalesce(F.col("u.rating"), F.col("s.rating"))
                .cast(DecimalType(18, 2))
                .alias("rating"),
                F.coalesce(F.col("u.status"), F.lit("ACTIVE")).alias("seller_status"),
                F.col("u.event_time").alias("seller_status_event_time"),
                F.col("s.ingestion_ts").alias("seller_ingestion_ts"),
            )
        )
    else:
        sellers = sellers.select(
            "seller_id",
            "seller_name",
            "seller_type",
            "city",
            "state",
            F.col("rating").cast(DecimalType(18, 2)).alias("rating"),
            F.lit("ACTIVE").alias("seller_status"),
            F.lit(None).cast(TimestampType()).alias("seller_status_event_time"),
            F.col("ingestion_ts").alias("seller_ingestion_ts"),
        )

    return sellers.withColumn("silver_updated_ts", F.current_timestamp())


def inventory_by_sku(inventory_current):
    return inventory_current.groupBy("sku_id").agg(
        F.sum("inventory_qty").alias("total_inventory_qty"),
        F.countDistinct("warehouse_id").alias("warehouse_count"),
        F.max("event_time").alias("latest_inventory_event_time"),
    )


def seller_metrics_by_sku(spark, seller_current):
    mapping = read_optional_bronze_file_table(spark, "product_seller_mapping")
    if mapping is None:
        return None

    mapping = mapping.select("sku_id", "seller_id").dropDuplicates()
    return (
        mapping.join(seller_current, "seller_id", "left")
        .groupBy("sku_id")
        .agg(
            F.countDistinct("seller_id").alias("seller_count"),
            F.sum(F.when(F.col("seller_status") == "ACTIVE", 1).otherwise(0)).alias(
                "active_seller_count"
            ),
            F.avg("rating").cast(DecimalType(18, 2)).alias("avg_seller_rating"),
        )
    )


def build_product_360(spark, product_master, inventory_current, pricing_history, seller_current):
    current_price = pricing_history.filter(F.col("is_current")).select(
        "sku_id",
        F.col("price").alias("current_price"),
        F.col("effective_from").alias("current_price_effective_from"),
    )
    product_360 = (
        product_master.join(inventory_by_sku(inventory_current), "sku_id", "left")
        .join(current_price, "sku_id", "left")
    )

    seller_metrics = seller_metrics_by_sku(spark, seller_current)
    if seller_metrics is not None:
        product_360 = product_360.join(seller_metrics, "sku_id", "left")

    return product_360.withColumn("silver_updated_ts", F.current_timestamp())


def run_silver():
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()

    product_master = build_product_master(spark)
    inventory_current = build_inventory_current(spark)
    pricing_history = build_pricing_history(spark)
    seller_current = build_seller_current(spark)
    product_360 = build_product_360(
        spark,
        product_master,
        inventory_current,
        pricing_history,
        seller_current,
    )

    outputs = {
        "product_master": product_master,
        "inventory_current": inventory_current,
        "pricing_history": pricing_history,
        "seller_current": seller_current,
        "product_360": product_360,
    }

    for table_name, df in outputs.items():
        write_silver(df, table_name)
        print(f"Wrote silver.{table_name} to {TABLES[table_name]}")


if __name__ == "__main__":
    run_silver()
