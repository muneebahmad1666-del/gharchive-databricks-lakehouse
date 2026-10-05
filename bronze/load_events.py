from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run_bronze_ingest(spark: SparkSession = None) -> None:
    spark = spark or SparkSession.builder.getOrCreate()

    source_path = "/Volumes/workspace/bronze/raw_landing/gharchive/"
    checkpoint_path = "/Volumes/workspace/bronze/raw_landing/_checkpoints/gharchive_events_raw"
    target_table = "workspace.bronze.gharchive_events_raw"

    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")  # one string column named "value"
        .load(source_path)
    )

    bronze = (
        raw.withColumnRenamed("value", "raw_content")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    (
        bronze.writeStream.option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)  # process new files, then stop
        .toTable(target_table)
        .awaitTermination()
    )


if __name__ == "__main__":
    run_bronze_ingest()
