# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import logging
from pyspark.sql import functions as F

logger = logging.getLogger("silver_pipeline")

def validation(df, table):
    rules = COMMON_RULES + TABLE_RULES.get(table, [])

    # error-level checks: required columns not null + every error rule
    checks = [(f"null_{c}", f"{c} IS NULL") for c in REQUIRED[table]]
    checks += [(name, pred) for name, _, sev, pred in rules if sev == "error"]

    # one reason per broken rule; NULL predicate results count as "not broken"
    flags = [F.when(F.expr(f"coalesce({pred}, false)"), F.lit(name)) for name, pred in checks]
    df = df.withColumn("dq_reasons", F.array_compact(F.array(*flags)))

    good = df.filter(F.size("dq_reasons") == 0).drop("dq_reasons")
    bad = df.filter(F.size("dq_reasons") > 0)

    logger.info(f"{table}: validation done")
    return good, bad