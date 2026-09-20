import logging
import sys
import time
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# -----------------------------------------------------------------------------
# Structured Logging Setup for Loki & Promtail Monitoring
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [BRONZE_PIPELINE] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("BronzeIngestion")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "metadata" / "raw_data"
BRONZE_DIR = PROJECT_ROOT / "data" / "metadata" / "bronze"
CHECKPOINT_DIR = BRONZE_DIR / "_checkpoints"

KAFKA_BOOTSTRAP_SERVERS = "kafka.ecommerce-platform.svc.cluster.local:9092"
CORRUPT_RECORD_COLUMN = "_corrupt_record"

CONFIG = {
    "products": {"source_type": "file", "file_format": "csv"},
    "categories": {"source_type": "file", "file_format": "csv"},
    "brands": {"source_type": "file", "file_format": "csv"},
    "sellers": {"source_type": "file", "file_format": "csv"},
    "suppliers": {"source_type": "file", "file_format": "csv"},
    "warehouses": {"source_type": "file", "file_format": "csv"},
    "pricing": {"source_type": "file", "file_format": "csv"},
    "inventory_updates": {"source_type": "kafka", "topic": "ecommerce.stream.inventory-updates"},
    "price_updates": {"source_type": "kafka", "topic": "ecommerce.stream.price-updates"},
    "seller_updates": {"source_type": "kafka", "topic": "ecommerce.stream.seller-updates"},
    "product_status_updates": {
        "source_type": "kafka",
        "topic": "ecommerce.stream.product-status-updates",
    },
}


def create_spark_session(app_name="metadata-bronze"):
    logger.info(f"Initializing Spark Session for Bronze layer (app_name='{app_name}')...")
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    logger.info("Spark Session created successfully. Spark Version: %s", spark.version)
    return spark


def source_path(entity, config):
    return Path(config.get("source_path", RAW_DATA_DIR / entity))


def bronze_path(entity):
    return BRONZE_DIR / f"bronze_{entity}.delta"


def checkpoint_path(entity):
    return CHECKPOINT_DIR / entity


def list_source_files(path, file_format):
    path = Path(path)
    extension = file_format.lower().lstrip(".")

    if path.is_file():
        return [str(path)]

    if not path.exists():
        logger.warning(f"Source path does not exist: {path}")
        return []

    files = [str(file) for file in sorted(path.rglob(f"*.{extension}")) if file.is_file()]
    logger.info(f"Discovered {len(files)} {file_format} file(s) in path: {path}")
    return files


def schema_with_corrupt_record(schema):
    if CORRUPT_RECORD_COLUMN in schema.fieldNames():
        return schema
    return schema.add(T.StructField(CORRUPT_RECORD_COLUMN, T.StringType(), True))


def read_json_files(spark, files):
    reader = (
        spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
    )
    schema = schema_with_corrupt_record(reader.json(files).schema)
    return reader.schema(schema).json(files)


def read_csv_files(spark, files):
    reader = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
    )
    schema = schema_with_corrupt_record(reader.option("inferSchema", True).csv(files).schema)
    return reader.schema(schema).csv(files)


def read_excel_files(spark, files):
    try:
        return (
            spark.read.format("excel")
            .option("header", True)
            .option("inferSchema", True)
            .load(files)
        )
    except Exception as exc:
        logger.error(f"Failed to read Excel files: {exc}")
        raise RuntimeError("Unable to read Excel files with Spark datasource.") from exc


def read_source_files(spark, files, file_format):
    file_format = file_format.lower().lstrip(".")
    if file_format == "json":
        return read_json_files(spark, files)
    if file_format == "csv":
        return read_csv_files(spark, files)
    if file_format == "xlsx":
        return read_excel_files(spark, files)
    raise ValueError(f"Unsupported file format: {file_format}")


def add_file_bronze_columns(df, entity):
    accepted_df = df.filter(F.col(CORRUPT_RECORD_COLUMN).isNull())
    payload_columns = [
        F.col(column)
        for column in accepted_df.columns
        if column != CORRUPT_RECORD_COLUMN
    ]

    return (
        accepted_df.select(
            F.to_json(
                F.struct(*payload_columns),
                {"ignoreNullFields": "false"},
            ).alias("raw_payload"),
            F.struct(*payload_columns).alias("parsed_payload"),
            F.input_file_name().alias("source_file_name"),
        )
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .withColumn("source_type", F.lit("file"))
        .withColumn("source_name", F.lit(entity))
    )


def add_kafka_bronze_columns(df, entity):
    return (
        df.select(
            F.col("value").cast("string").alias("raw_payload"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .withColumn("source_type", F.lit("kafka"))
        .withColumn("source_name", F.lit(entity))
    )


def write_delta(df, path):
    (
        df.write.format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .partitionBy("ingestion_date")
        .save(str(path))
    )


def load_file_to_bronze(spark, entity, config):
    start_time = time.time()
    file_format = config["file_format"].lower().lstrip(".")
    logger.info(f"Processing batch entity '{entity}' (format={file_format})...")

    files = list_source_files(source_path(entity, config), file_format)
    if not files:
        logger.warning(f"No {file_format} files found for entity '{entity}'. Skipping.")
        return

    df = read_source_files(spark, files, file_format)
    bronze_df = add_file_bronze_columns(df, entity)
    
    count = bronze_df.count()
    target_path = bronze_path(entity)
    write_delta(bronze_df, target_path)

    elapsed = round(time.time() - start_time, 2)
    logger.info(
        f"SUCCESS: Loaded entity='{entity}' | Records={count} | Duration={elapsed}s | Path={target_path}"
    )


def load_kafka_to_bronze(spark, entity, config):
    topic = config["topic"]
    broker = config.get("broker", KAFKA_BOOTSTRAP_SERVERS)
    logger.info(f"Starting Kafka stream ingestion for entity='{entity}' from topic='{topic}' @ {broker}...")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", config.get("starting_offsets", "latest"))
        .load()
    )

    target_path = bronze_path(entity)
    cp_path = checkpoint_path(entity)

    query = (
        add_kafka_bronze_columns(kafka_df, entity)
        .writeStream.format("delta")
        .outputMode("append")
        .option("mergeSchema", "true")
        .option("checkpointLocation", str(cp_path))
        .partitionBy("ingestion_date")
        .start(str(target_path))
    )

    logger.info(f"Kafka stream for '{entity}' started successfully (Query ID: {query.id}, Target: {target_path})")
    return query


def load_entity_to_bronze(spark, entity, config):
    source_type = config["source_type"].lower()
    try:
        if source_type == "file":
            load_file_to_bronze(spark, entity, config)
            return None

        if source_type == "kafka":
            return load_kafka_to_bronze(spark, entity, config)

        raise ValueError(f"Unsupported source_type for {entity}: {source_type}")
    except Exception as e:
        logger.error(f"FAILED to process entity '{entity}': {str(e)}", exc_info=True)
        raise


def run_bronze(config=CONFIG):
    logger.info("==================================================")
    logger.info("STARTING BRONZE LAYER INGESTION PIPELINE")
    logger.info("==================================================")
    
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    queries = []

    for entity, source_config in config.items():
        query = load_entity_to_bronze(spark, entity, source_config)
        if query is not None:
            queries.append(query)

    logger.info(f"Bronze ingestion dispatch complete. Active streaming queries: {len(queries)}")
    return queries


if __name__ == "__main__":
    stream_queries = run_bronze()
    for stream_query in stream_queries:
        stream_query.awaitTermination()
