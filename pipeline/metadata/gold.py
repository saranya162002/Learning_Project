import logging
import sys
import time
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# -----------------------------------------------------------------------------
# Structured Logging Setup for Loki & Promtail Monitoring
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [GOLD_PIPELINE] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("GoldAggregation")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "data" / "metadata" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "metadata" / "gold"
GOLD_TABLE = GOLD_DIR / "product_business_snapshot.delta"

LOW_STOCK_THRESHOLD = 10

REQUIRED_SILVER_TABLES = {
    "product_360": SILVER_DIR / "product_360.delta",
    "inventory_current": SILVER_DIR / "inventory_current.delta",
    "pricing_history": SILVER_DIR / "pricing_history.delta",
}


def create_spark_session(app_name="metadata-gold"):
    logger.info(f"Initializing Spark Session for Gold layer (app_name='{app_name}')...")
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    logger.info("Spark Session created. Version: %s", spark.version)
    return spark


def table_exists(path):
    return (Path(path) / "_delta_log").exists()


def read_silver_table(spark, table_name):
    path = REQUIRED_SILVER_TABLES[table_name]
    if not table_exists(path):
        logger.error(f"Missing required Silver table: {path}")
        raise FileNotFoundError(f"Missing Silver table: {path}")

    return spark.read.format("delta").load(str(path))


def ensure_columns(df, columns):
    for column_name, data_type in columns.items():
        if column_name not in df.columns:
            df = df.withColumn(column_name, F.lit(None).cast(data_type))
    return df


def build_inventory_metrics(inventory_current):
    logger.info("Building Gold inventory aggregations...")
    inventory_current = ensure_columns(
        inventory_current,
        {
            "warehouse_name": T.StringType(),
            "warehouse_city": T.StringType(),
            "warehouse_state": T.StringType(),
        },
    )

    return inventory_current.groupBy("sku_id").agg(
        F.coalesce(F.sum("inventory_qty"), F.lit(0)).alias("total_inventory_qty"),
        F.countDistinct("warehouse_id").alias("warehouse_count"),
        F.sum(
            F.when(F.col("inventory_qty") <= LOW_STOCK_THRESHOLD, F.lit(1)).otherwise(
                F.lit(0)
            )
        ).alias("low_stock_warehouse_count"),
        F.max("event_time").alias("latest_inventory_event_time"),
        F.collect_list(
            F.struct(
                "warehouse_id",
                "warehouse_name",
                "warehouse_city",
                "warehouse_state",
                "inventory_qty",
                F.col("event_time").alias("inventory_event_time"),
            )
        ).alias("warehouse_inventory"),
    )


def build_pricing_metrics(pricing_history):
    logger.info("Building Gold pricing aggregations...")
    pricing_history = ensure_columns(
        pricing_history,
        {
            "price": T.DecimalType(18, 2),
            "effective_from": T.TimestampType(),
            "is_current": T.BooleanType(),
            "price_version": T.IntegerType(),
        },
    )
    current_window = Window.partitionBy("sku_id").orderBy(
        F.col("is_current").desc_nulls_last(),
        F.col("effective_from").desc_nulls_last(),
        F.col("price_version").desc_nulls_last(),
    )

    current_price = (
        pricing_history.withColumn("_row_number", F.row_number().over(current_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .select(
            "sku_id",
            F.col("price").alias("current_price"),
            F.col("effective_from").alias("current_price_effective_from"),
            F.col("price_version").alias("current_price_version"),
        )
    )

    price_summary = pricing_history.groupBy("sku_id").agg(
        F.count("*").alias("price_version_count"),
        F.min("price").alias("lowest_historical_price"),
        F.max("price").alias("highest_historical_price"),
        F.min("effective_from").alias("first_price_effective_from"),
        F.max("effective_from").alias("latest_price_effective_from"),
    )

    return current_price.join(price_summary, "sku_id", "left")


def build_gold_product_snapshot(spark):
    logger.info("Reading Silver tables for Gold product_business_snapshot...")
    product_360 = read_silver_table(spark, "product_360")
    inventory_current = read_silver_table(spark, "inventory_current")
    pricing_history = read_silver_table(spark, "pricing_history")

    product_360 = ensure_columns(
        product_360,
        {
            "product_id": T.StringType(),
            "product_name": T.StringType(),
            "description": T.StringType(),
            "product_status": T.StringType(),
            "brand_id": T.StringType(),
            "brand_name": T.StringType(),
            "category_id": T.StringType(),
            "category_name": T.StringType(),
            "color": T.StringType(),
            "size": T.StringType(),
            "material": T.StringType(),
            "gender": T.StringType(),
            "season": T.StringType(),
            "image_url": T.StringType(),
            "seller_count": T.LongType(),
            "active_seller_count": T.LongType(),
            "avg_seller_rating": T.DecimalType(18, 2),
        },
    )

    gold = (
        product_360.alias("p")
        .join(build_inventory_metrics(inventory_current).alias("i"), "sku_id", "left")
        .join(build_pricing_metrics(pricing_history).alias("pr"), "sku_id", "left")
        .select(
            F.col("p.sku_id").alias("sku_id"),
            F.col("p.product_id").alias("product_id"),
            F.col("p.product_name").alias("product_name"),
            F.col("p.description").alias("description"),
            F.col("p.product_status").alias("product_status"),
            F.col("p.brand_id").alias("brand_id"),
            F.col("p.brand_name").alias("brand_name"),
            F.col("p.category_id").alias("category_id"),
            F.col("p.category_name").alias("category_name"),
            F.col("p.color").alias("color"),
            F.col("p.size").alias("size"),
            F.col("p.material").alias("material"),
            F.col("p.gender").alias("gender"),
            F.col("p.season").alias("season"),
            F.col("p.image_url").alias("image_url"),
            F.coalesce(F.col("i.total_inventory_qty"), F.lit(0)).alias(
                "total_inventory_qty"
            ),
            F.coalesce(F.col("i.warehouse_count"), F.lit(0)).alias("warehouse_count"),
            F.coalesce(F.col("i.low_stock_warehouse_count"), F.lit(0)).alias(
                "low_stock_warehouse_count"
            ),
            F.col("i.latest_inventory_event_time").alias(
                "latest_inventory_event_time"
            ),
            F.col("i.warehouse_inventory").alias("warehouse_inventory"),
            F.col("pr.current_price").alias("current_price"),
            F.col("pr.current_price_effective_from").alias(
                "current_price_effective_from"
            ),
            F.col("pr.current_price_version").alias("current_price_version"),
            F.col("pr.price_version_count").alias("price_version_count"),
            F.col("pr.lowest_historical_price").alias("lowest_historical_price"),
            F.col("pr.highest_historical_price").alias("highest_historical_price"),
            F.col("pr.first_price_effective_from").alias("first_price_effective_from"),
            F.col("pr.latest_price_effective_from").alias("latest_price_effective_from"),
            F.coalesce(F.col("p.seller_count"), F.lit(0)).alias("seller_count"),
            F.coalesce(F.col("p.active_seller_count"), F.lit(0)).alias(
                "active_seller_count"
            ),
            F.col("p.avg_seller_rating").alias("avg_seller_rating"),
        )
    )

    return (
        gold.withColumn(
            "availability_status",
            F.when(F.col("total_inventory_qty") <= 0, F.lit("OUT_OF_STOCK"))
            .when(F.col("total_inventory_qty") <= LOW_STOCK_THRESHOLD, F.lit("LOW_STOCK"))
            .otherwise(F.lit("IN_STOCK")),
        )
        .withColumn("has_active_seller", F.col("active_seller_count") > 0)
        .withColumn(
            "search_text",
            F.concat_ws(
                " ",
                "product_name",
                "brand_name",
                "category_name",
                "description",
                "color",
                "size",
                "material",
                "gender",
                "season",
            ),
        )
        .withColumn("snapshot_date", F.current_date())
        .withColumn("gold_updated_ts", F.current_timestamp())
    )


def write_gold(df):
    start_time = time.time()
    logger.info(f"Writing Gold table to path: {GOLD_TABLE}...")

    count = df.count()
    (
        df.write.format("delta")
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .save(str(GOLD_TABLE))
    )

    elapsed = round(time.time() - start_time, 2)
    logger.info(f"SUCCESS: Saved gold.product_business_snapshot | Records={count} | Duration={elapsed}s | Path={GOLD_TABLE}")


def run_gold():
    logger.info("==================================================")
    logger.info("STARTING GOLD LAYER AGGREGATION PIPELINE")
    logger.info("==================================================")

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    try:
        gold_df = build_gold_product_snapshot(spark)
        write_gold(gold_df)

        logger.info("==================================================")
        logger.info("GOLD LAYER PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("==================================================")
    except Exception as e:
        logger.error(f"GOLD PIPELINE FAILED: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    run_gold()
