# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ../silver_config

# COMMAND ----------

# MAGIC %run ./standardization

# COMMAND ----------

# MAGIC %run ./cleansing

# COMMAND ----------

# MAGIC %run ./validation

# COMMAND ----------

# MAGIC %run ./enrichment

# COMMAND ----------

# MAGIC %run ./deduplication

# COMMAND ----------

# MAGIC %run ./filtering

# COMMAND ----------

from pyspark.sql import functions as F
import logging

S = "workspace.silver"

MAX_QUARANTINE_RATE = 0.05   # fail the run if more than 5% of a table is quarantined

logger = logging.getLogger("silver_pipeline")
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())

# events first: the type tables must match the clean events
TABLES = ["events", "create_events", "pull_request_events", "push_events",
          "issue_events", "fork_events", "watch_events"]


def write_quarantine(bad, table):
    q = f"{S}.{table}_quarantine"
    bad = (bad.withColumn("quarantined_at", F.current_timestamp())
              .withColumn("status", F.lit("pending")))
    if spark.catalog.tableExists(q):
        bad = bad.join(spark.table(q).select("event_id"), "event_id", "left_anti")  # no duplicates on rerun
        bad.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(q)
    else:
        bad.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(q)


def run_table(table):
    logger.info(f"{table}: start")
    df = spark.table(f"{S}.{table}_parsed")
    total = df.count()

    df = standardize(df, table)
    df = cleansing(df, table)
    df = enrich(df, table)
    good, bad = validation(df, table)

    # keep type tables consistent with the clean events table
    if table != "events":
        good = good.join(spark.table(f"{S}.events").select("event_id"), "event_id", "left_semi")

    good = deduplicate(good, table)
    good = filtering(good, table)

    bad_n = bad.count()
    if total and bad_n / total > MAX_QUARANTINE_RATE:
        raise ValueError(f"{table}: {bad_n}/{total} rows quarantined, above {MAX_QUARANTINE_RATE:.0%}")
    if bad_n:
        write_quarantine(bad, table)

    (good.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(f"{S}.{table}"))
    logger.info(f"{table}: {total} in, {good.count()} clean, {bad_n} quarantined")


logger.info("Silver transformations started")
for t in TABLES:
    run_table(t)
logger.info("Silver transformations finished")