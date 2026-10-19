# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import logging
from pyspark.sql import functions as F

logger = logging.getLogger("silver_pipeline")

def enrich(df, table):
    if table == "events":
        df = df.withColumn(
            "is_bot",
            F.coalesce(
                F.col("actor_login").rlike(r"\[bot\]$")
                | F.col("actor_login").rlike(r"(^bot[-_]|[-_]bot$)"),
                F.lit(False),
            ),
        )

    elif table == "pull_request_events":
        df = (
            df
            .withColumn("lines_changed", F.col("additions") + F.col("deletions"))
            .withColumn(
                "pr_size",
                F.when(F.col("lines_changed").isNull(), F.lit(None))
                 .when(F.col("lines_changed") > 5000, "xlarge")
                 .when(F.col("lines_changed") > 1000, "large")
                 .when(F.col("lines_changed") > 200, "medium")
                 .when(F.col("lines_changed") > 20, "small")
                 .otherwise("tiny"),
            )
            .withColumn(
                "merge_time_hours",
                F.when(
                    F.col("is_merged") & F.col("merged_at").isNotNull() & F.col("pr_created_at").isNotNull(),
                    F.round((F.unix_timestamp("merged_at") - F.unix_timestamp("pr_created_at")) / 3600, 2),
                ),
            )
            .withColumn("merged_hour", F.when(F.col("is_merged"), F.hour("merged_at")))
            .withColumn(
                "is_bot",
                F.coalesce(
                    (F.lower(F.col("pr_author_type")) == "bot")
                    | F.col("pr_author").rlike(r"\[bot\]$"),
                    F.lit(False),
                ),
            )
        )

    elif table == "watch_events":
        df = df.withColumn("is_star", F.col("action") == "started")

    logger.info(f"{table}: enrich done")
    return df