# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import logging
from pyspark.sql import functions as F

logger = logging.getLogger("silver_pipeline")

def filtering(df, table):
    if table == "events":
        df = df.filter(F.col("type").isin(*KNOWN_TYPES))
    logger.info(f"{table}: filtering done")
    return df