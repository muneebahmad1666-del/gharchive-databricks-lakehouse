# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
dbutils.widgets.text("env", "prod")
dbutils.widgets.text("start_date", "2023-01-01")
dbutils.widgets.text("end_date", "2023-01-14")

ENV = dbutils.widgets.get("env")
assert ENV in ("dev", "prod"), f"unknown env: {ENV}"

B = "workspace.bronze"
S = "workspace.silver" if ENV == "prod" else "workspace.silver_dev"
START = dbutils.widgets.get("start_date")
END = dbutils.widgets.get("end_date")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}")
print(f"env={ENV} | writing to {S} | range {START or 'beginning'} to {END or 'end'}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import *
j = lambda path: F.get_json_object("raw_content", path)

parsed = (
    spark.table(f"{B}.gharchive_events_raw")
    .select(
        j("$.id").alias("event_id"),
        j("$.type").alias("type"),
        j("$.actor.id").cast("bigint").alias("actor_id"),
        F.trim(j("$.actor.login")).alias("actor_login"),
        j("$.repo.id").cast("bigint").alias("repo_id"),
        F.trim(j("$.repo.name")).alias("repo_name"),
        j("$.org.login").alias("org_login"),
        j("$.public").cast("boolean").alias("is_public"),
        F.to_timestamp(j("$.created_at")).alias("created_at"),
        j("$.payload").alias("payload"),
        F.col("_ingested_at").alias("ingested_at"),
        F.col("_source_file"),
    )
    .withColumn("event_date", F.to_date("created_at"))
    .withColumn("event_hour", F.hour("created_at"))
)

in_range = F.lit(True)
if START:
    in_range = in_range & (F.col("event_date") >= F.lit(START).cast("date"))
if END:
    in_range = in_range & (F.col("event_date") <= F.lit(END).cast("date"))

parsed = parsed.filter(F.col("event_date").isNull() | in_range)

has_keys = F.col("event_id").isNotNull() & F.col("created_at").isNotNull()

# rows we can't key or date: keep them, don't lose them silently
(parsed.filter(~has_keys)
    .withColumn("reject_reason", F.lit("null event_id or created_at"))
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{S}.events_rejected_parsed"))

silver = parsed.filter(has_keys).dropDuplicates(["event_id"])

(silver.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{S}.events_parsed"))

# COMMAND ----------

PR_PAYLOAD = """
action STRING,
number BIGINT,
pull_request STRUCT<
  id: BIGINT,
  number: BIGINT,
  state: STRING,
  title: STRING,
  draft: BOOLEAN,
  merged: BOOLEAN,
  author_association: STRING,
  created_at: STRING,
  closed_at: STRING,
  merged_at: STRING,
  commits: BIGINT,
  additions: BIGINT,
  deletions: BIGINT,
  changed_files: BIGINT,
  comments: BIGINT,
  review_comments: BIGINT,
  user: STRUCT<login: STRING, id: BIGINT, type: STRING>,
  merged_by: STRUCT<login: STRING, id: BIGINT>,
  head: STRUCT<ref: STRING>,
  base: STRUCT<
    ref: STRING,
    repo: STRUCT<id: BIGINT, full_name: STRING, language: STRING, private: BOOLEAN, fork: BOOLEAN>
  >
>
"""

EVENT_TABLES = {
    "pull_request_events_parsed": (
        "PullRequestEvent",
        PR_PAYLOAD,
        [
            "p.action",
            "p.number AS pr_number",
            "p.pull_request.id AS pr_id",
            "p.pull_request.state AS pr_state",
            "p.pull_request.draft AS is_draft",
            "p.pull_request.merged AS is_merged",
            "p.pull_request.user.login AS pr_author",
            "p.pull_request.user.type AS pr_author_type",
            "p.pull_request.author_association AS author_association",
            "to_timestamp(p.pull_request.created_at) AS pr_created_at",
            "to_timestamp(p.pull_request.merged_at) AS merged_at",
            "to_timestamp(p.pull_request.closed_at) AS closed_at",
            "p.pull_request.commits AS commit_count",
            "p.pull_request.additions AS additions",
            "p.pull_request.deletions AS deletions",
            "p.pull_request.additions + p.pull_request.deletions AS lines_changed",
            "p.pull_request.changed_files AS changed_files",
            "p.pull_request.base.ref AS base_branch",
            "p.pull_request.base.repo.language AS repo_language",
        ],
    ),
    "push_events_parsed": (
        "PushEvent",
        "push_id BIGINT, ref STRING, head STRING, size BIGINT, distinct_size BIGINT",
        ["p.push_id", "p.ref", "p.head", "p.size AS commit_count",
         "p.distinct_size AS distinct_commit_count"],
    ),
    "issue_events_parsed": (
        "IssuesEvent",
        "action STRING, issue STRUCT<id: BIGINT, number: BIGINT, state: STRING, comments: BIGINT>",
        ["p.action", "p.issue.id AS issue_id", "p.issue.number AS issue_number",
         "p.issue.state AS issue_state", "p.issue.comments AS comment_count"],
    ),
    "watch_events_parsed": ("WatchEvent", "action STRING", ["p.action"]),
    "fork_events_parsed": (
        "ForkEvent",
        "forkee STRUCT<id: BIGINT, full_name: STRING>",
        ["p.forkee.id AS forkee_id", "p.forkee.full_name AS forkee_name"],
    ),
    "create_events_parsed": (
        "CreateEvent",
        "ref STRING, ref_type STRING, master_branch STRING, pusher_type STRING",
        ["p.ref", "p.ref_type", "p.master_branch", "p.pusher_type"],
    ),
}

# COMMAND ----------

MAX_PARSE_FAIL = 0.001   # fail if more than 0.1% of payloads can't be parsed

BASE = ["event_id", "actor_id", "repo_id", "created_at",
        "event_date", "event_hour", "ingested_at"]

events = spark.table(f"{S}.events_parsed")
summary = []

for table, (event_type, schema, cols) in EVENT_TABLES.items():
    df = (
        events.filter(F.col("type") == event_type)
        .withColumn("p", F.from_json("payload", schema))
    )

    stats = df.agg(
        F.count("*").alias("total"),
        F.sum(F.col("p").isNull().cast("int")).alias("unparsed"),
    ).first()
    total, unparsed = stats["total"], stats["unparsed"] or 0
    if total and unparsed / total > MAX_PARSE_FAIL:
        raise ValueError(f"{table}: {unparsed}/{total} payloads failed to parse")

    (df.selectExpr(*BASE, *cols)
        .write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{S}.{table}"))

    written = spark.table(f"{S}.{table}").count()
    summary.append((table, total, unparsed, written))
    if written != total:
        raise ValueError(f"{table}: wrote {written} rows but events has {total}")