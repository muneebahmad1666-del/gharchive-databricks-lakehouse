from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run_bronze_ingest(spark: SparkSession = None) -> None:
    spark = spark or SparkSession.builder.getOrCreate()

    SOURCE_PATH = "/Volumes/workspace/bronze/raw_landing/gharchive/"
    CHECKPOINT_PATH = "/Volumes/workspace/bronze/raw_landing/_checkpoints/gharchive_events_raw"
    SCHEMA_PATH = "/Volumes/workspace/bronze/raw_landing/_schemas/gharchive_events_raw"
    TARGET_TABLE = "workspace.bronze.gharchive_events_raw"

    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("pathGlobFilter", "*.json.gz")
        # ──► MEMORY GUARDRAIL FOR FREE TIERS ◄──
        # Limits Spark to reading 10 files at a time to protect cluster RAM
        .option("cloudFiles.maxFilesPerTrigger", 10)
        .load(SOURCE_PATH)
        .select(
            F.col("value").alias("raw_json"),
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingested_at"),
        )
        .writeStream.option("checkpointLocation", CHECKPOINT_PATH)
        .format("delta")
        .trigger(availableNow=True)
        .start(TARGET_TABLE)
        .awaitTermination()
    )


if __name__ == "__main__":
    run_bronze_ingest()
