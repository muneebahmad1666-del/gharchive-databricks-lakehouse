# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logger = logging.getLogger("silver_pipeline")

SKIP_TRIM = {"payload", "_source_file"}

def standardize(df, table):
    # 1. trim every string column
    for f in df.schema.fields:
        if isinstance(f.dataType, StringType) and f.name not in SKIP_TRIM:
            df = df.withColumn(f.name, F.trim(F.col(f.name)))

    # 2. lowercase the columns listed in the config
    for c in LOWERCASE.get(table, []):
        df = df.withColumn(c, F.lower(F.col(c)))

    # 3. types: a bad id becomes NULL and is quarantined by validation
    df = df.withColumn("event_id", F.expr("try_cast(event_id AS BIGINT)"))

    logger.info(f"{table}: standardize done")
    return df