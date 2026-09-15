from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "metadata" / "raw_data"
BRONZE_DIR = PROJECT_ROOT / "data" / "metadata" / "bronze"
CHECKPOINT_DIR = BRONZE_DIR / "_checkpoints"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

CONFIG = {
    "products": {
        "source_type": "file",
        "file_format": "json",
        "source_path": RAW_DATA_DIR / "products",
    },
    "brands": {
        "source_type": "file",
        "file_format": "xlsx",
        "source_path": RAW_DATA_DIR / "brands",
    },
    "inventory": {
        "source_type": "kafka",
        "topic": "inventory-topic",
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


def target_name(entity, config):
    return config.get("target_name", entity)


def bronze_path(entity, config):
    return BRONZE_DIR / f"bronze_{target_name(entity, config)}.delta"


def checkpoint_path(entity, config):
    return CHECKPOINT_DIR / target_name(entity, config)


def list_source_files(source_path, file_format):
    source_path = Path(source_path)
    extension = file_format.lower().lstrip(".")

    if source_path.is_file():
        return [str(source_path)]

    if not source_path.exists():
        raise FileNotFoundError(f"Source path does not exist: {source_path}")

    return [
        str(path)
        for path in sorted(source_path.rglob(f"*.{extension}"))
        if path.is_file()
    ]


def read_source_files(spark, files, file_format):
    file_format = file_format.lower().lstrip(".")

    if file_format == "json":
        return spark.read.json(files)

    if file_format == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(files)

    if file_format == "parquet":
        return spark.read.parquet(*files)

    if file_format == "xlsx":
        try:
            return (
                spark.read.format("excel")
                .option("header", True)
                .option("inferSchema", True)
                .load(files)
            )
        except Exception as exc:
            raise RuntimeError(
                "Spark Excel datasource is required to read .xlsx files. "
                "Start Spark with the spark-excel package, for example with "
                "--packages com.crealytics:spark-excel_2.12:<version>."
            ) from exc

    raise ValueError(f"Unsupported file format: {file_format}")


def add_file_bronze_columns(df, entity):
    return (
        df.withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_type", F.lit("file"))
        .withColumn("source_name", F.lit(entity))
        .withColumn("source_file_name", F.input_file_name())
    )


def add_kafka_bronze_columns(df, entity):
    return (
        df.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_type", F.lit("kafka"))
        .withColumn("source_name", F.lit(entity))
    )


def load_file_to_bronze(spark, entity, config):
    files = list_source_files(config["source_path"], config["file_format"])

    if not files:
        print(f"No {config['file_format']} files found for {entity}")
        return

    df = read_source_files(spark, files, config["file_format"])

    (
        add_file_bronze_columns(df, entity)
        .write.format("delta")
        .mode("append")
        .save(str(bronze_path(entity, config)))
    )

    print(f"Appended {entity} data to {bronze_path(entity, config)}")


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
        .option("checkpointLocation", str(checkpoint_path(entity, config)))
        .start(str(bronze_path(entity, config)))
    )

    print(f"Started stream {entity} from topic {topic} to {bronze_path(entity, config)}")
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
