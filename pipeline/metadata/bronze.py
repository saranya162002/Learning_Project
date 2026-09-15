from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

from pyspark.sql import functions as F

def read_source_file(spark, file_path):

    extension = Path(file_path).suffix.lower()

    if extension == ".json":
        return spark.read.json(file_path)

    elif extension == ".csv":
        return spark.read.option(
            "header",
            True
        ).csv(file_path)

    elif extension == ".parquet":
        return spark.read.parquet(file_path)

    elif extension == ".xlsx":

        pdf = pd.read_excel(file_path)

        return spark.createDataFrame(pdf)

    else:
        raise Exception(
            f"Unsupported file format {extension}"
        )
    
def load_batch_incremental(
    spark,
    entity,
    source_path,
    bronze_path,
    metadata_path
):

    metadata_df = spark.read.format("delta") \
        .load(metadata_path)

    already_processed = (
        metadata_df
        .select("file_name")
        .rdd
        .flatMap(lambda x: x)
        .collect()
    )

    all_files = 
    #dbutils.fs.ls(source_path)

    new_files = [
        file
        for file in all_files
        if file not in already_processed
    ]

    if not new_files:
        return

    df = spark.read.json(new_files)

    (
        df.withColumn(
            "ingestion_ts",
            F.current_timestamp()
        ).withColumn(
            "source_type",
            F.lit("file")
        )
        .withColumn(
            "source_name",
            F.lit(entity)
        )
        .withColumn(
            "source_file_name",
            F.input_file_name()
        )
        .write
        .format("delta")
        .mode("append")
        .save(bronze_path)
    )
    
def load_kafka_stream(
        spark,
        topic,
        broker,
        bronze_path,
        checkpoint_path):

    kafka_df = (
        spark.readStream
            .format("kafka")
            .option(broker)
            .option(
                "subscribe",
                topic
            )
            .load()
    )

    df = kafka_df.withColumn(
        "ingestion_ts",
        F.current_timestamp()
    ).withColumn(
        "source_type",
        F.lit("kafka")
    ).withColumn(
        "source_name",
        F.col("topic")
    )

    (
        df.writeStream
          .format("delta")
          .option(
              "checkpointLocation",
              checkpoint_path
          )
          .start(bronze_path)
    )