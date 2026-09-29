# Databricks notebook source
# Incrementally loads raw GH Archive files from the Volume into the bronze table (Auto Loader).
from pyspark.sql import functions as F

SOURCE_PATH = "/Volumes/workspace/bronze/raw_landing/gharchive/"
CHECKPOINT_PATH = "/Volumes/workspace/bronze/raw_landing/_checkpoints/gharchive_events_raw"
TARGET_TABLE = "workspace.bronze.gharchive_events_raw"

(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "text")
    .option("pathGlobFilter", "*.json.gz")
    .load(SOURCE_PATH)
    .select(
        F.col("value").alias("raw_json"),
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
    )
    .writeStream.option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
    .awaitTermination()
)
