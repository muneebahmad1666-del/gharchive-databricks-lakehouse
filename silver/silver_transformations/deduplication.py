# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("silver_pipeline")

def deduplicate(df, table):
    # event_id is the unique key in every table; keep the most recently ingested copy
    w = Window.partitionBy("event_id").orderBy(
        F.col("ingested_at").desc_nulls_last(), F.col("created_at").desc()
    )
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    logger.info(f"{table}: deduplicate done")
    return df