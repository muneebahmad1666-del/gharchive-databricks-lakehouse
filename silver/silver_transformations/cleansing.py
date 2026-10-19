# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logger = logging.getLogger("silver_pipeline")

def cleansing(df, table):
    # 1. blank strings become real nulls (payload is left alone)
    str_cols = [f.name for f in df.schema.fields
                if isinstance(f.dataType, StringType) and f.name not in ("payload", "_source_file")]
    for c in str_cols:
        df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.col(c)))

    # 2. defaults for optional columns only
    defaults = DEFAULTS.get(table, {})
    if defaults:
        df = df.fillna(defaults)

    logger.info(f"{table}: cleansing done")
    return df