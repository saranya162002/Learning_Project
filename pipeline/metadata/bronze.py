from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "metadata" / "raw_data"
BRONZE_DIR = PROJECT_ROOT / "data" / "metadata" / "bronze"
CHECKPOINT_DIR = BRONZE_DIR / "_checkpoints"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
CORRUPT_RECORD_COLUMN = "_corrupt_record"

# Add a new file source or Kafka topic here.
# File sources default to data/metadata/raw_data/<entity>/.
CONFIG = {
    "products": {"source_type": "file", "file_format": "csv"},
    "categories": {"source_type": "file", "file_format": "csv"},
    "brands": {"source_type": "file", "file_format": "csv"},
    "sellers": {"source_type": "file", "file_format": "csv"},
    "suppliers": {"source_type": "file", "file_format": "csv"},
    "warehouses": {"source_type": "file", "file_format": "csv"},
    "pricing": {"source_type": "file", "file_format": "csv"},
    "inventory_updates": {"source_type": "kafka", "topic": "inventory_updates"},
    "price_updates": {"source_type": "kafka", "topic": "price_updates"},
    "seller_updates": {"source_type": "kafka", "topic": "seller_updates"},
    "product_status_updates": {
        "source_type": "kafka",
        "topic": "product_status_updates",
    },
}


def create_spark_session(app_name="metadata-bronze"):
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


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
        raise FileNotFoundError(f"Source path does not exist: {path}")

    return [str(file) for file in sorted(path.rglob(f"*.{extension}")) if file.is_file()]


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
        raise RuntimeError(
            "Unable to read Excel files with Spark. Install the Spark Excel datasource "
            "(for example, com.crealytics:spark-excel_2.12:<version>) and verify "
            "that the workbook is not corrupt."
        ) from exc


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
    file_format = config["file_format"].lower().lstrip(".")
    files = list_source_files(source_path(entity, config), file_format)

    if not files:
        print(f"No {file_format} files found for {entity}")
        return

    df = read_source_files(spark, files, file_format)
    bronze_df = add_file_bronze_columns(df, entity)
    write_delta(bronze_df, bronze_path(entity))

    print(f"Appended {entity} raw data to {bronze_path(entity)}")


def load_kafka_to_bronze(spark, entity, config):
    topic = config["topic"]
    broker = config.get("broker", KAFKA_BOOTSTRAP_SERVERS)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", config.get("starting_offsets", "latest"))
        .load()
    )

    query = (
        add_kafka_bronze_columns(kafka_df, entity)
        .writeStream.format("delta")
        .outputMode("append")
        .option("mergeSchema", "true")
        .option("checkpointLocation", str(checkpoint_path(entity)))
        .partitionBy("ingestion_date")
        .start(str(bronze_path(entity)))
    )

    print(f"Started stream {entity} from topic {topic} to {bronze_path(entity)}")
    return query


def load_entity_to_bronze(spark, entity, config):
    source_type = config["source_type"].lower()

    if source_type == "file":
        load_file_to_bronze(spark, entity, config)
        return None

    if source_type == "kafka":
        return load_kafka_to_bronze(spark, entity, config)

    raise ValueError(f"Unsupported source_type for {entity}: {source_type}")


def run_bronze(config=CONFIG):
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    queries = []

    for entity, source_config in config.items():
        query = load_entity_to_bronze(spark, entity, source_config)
        if query is not None:
            queries.append(query)

    return queries


if __name__ == "__main__":
    stream_queries = run_bronze()

    for stream_query in stream_queries:
        stream_query.awaitTermination()
