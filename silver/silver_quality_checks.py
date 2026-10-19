# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Databricks notebook source
import os, sys
from pyspark.sql import functions as F

# silver_config.py lives in the silver_transformations folder
# (assumes this notebook sits directly in the silver folder next to it; adjust if yours differs)
_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
sys.path.append("/Workspace" + os.path.dirname(_nb) + "/silver_transformations")

%run ./silver_config

dbutils.widgets.text("env", "dev")
SFX = "" if dbutils.widgets.get("env") == "prod" else "_dev"
S = f"workspace.silver{SFX}"

ROW_RULES = {
    t: [(f"null_{c}", "cleansing", "error", f"{c} IS NULL") for c in REQUIRED[t]]
       + COMMON_RULES + TABLE_RULES.get(t, [])
    for t in REQUIRED
}

UNIQUE_KEYS = {t: "event_id" for t in REQUIRED}

TYPE_TABLES = {"push_events": "PushEvent", "pull_request_events": "PullRequestEvent",
               "issue_events": "IssuesEvent", "watch_events": "WatchEvent",
               "fork_events": "ForkEvent", "create_events": "CreateEvent"}

BASE = ["event_id", "actor_id", "repo_id", "created_at", "event_date", "event_hour", "ingested_at"]

EXPECTED_COLUMNS = {
    "events": ["event_id", "type", "actor_id", "actor_login", "repo_id", "repo_name", "org_login",
               "is_public", "created_at", "payload", "ingested_at", "_source_file",
               "event_date", "event_hour", "is_bot"],
    "create_events": BASE + ["ref", "ref_type", "master_branch", "pusher_type"],
    "pull_request_events": BASE + ["action", "pr_number", "pr_id", "pr_state", "is_draft", "is_merged",
                                   "pr_author", "pr_author_type", "author_association", "pr_created_at",
                                   "merged_at", "closed_at", "commit_count", "additions", "deletions",
                                   "lines_changed", "changed_files", "base_branch", "repo_language",
                                   "pr_size", "merge_time_hours", "merged_hour", "is_bot"],
    "push_events": BASE + ["push_id", "ref", "head", "commit_count", "distinct_commit_count"],
    "issue_events": BASE + ["action", "issue_id", "issue_number", "issue_state", "comment_count"],
    "fork_events": BASE + ["forkee_id", "forkee_name"],
    "watch_events": BASE + ["action", "is_star"],
}

results = []   # (table, rule, technique, severity, failing_rows, total_rows)
def log(table, rule, technique, severity, failing, total):
    results.append((table, rule, technique, severity, int(failing), int(total)))

def finish():
    out = (spark.createDataFrame(
            results, "table_name STRING, rule STRING, technique STRING, severity STRING, failing_rows BIGINT, total_rows BIGINT")
           .withColumn("fail_rate", F.round(F.col("failing_rows") / F.greatest(F.col("total_rows"), F.lit(1)), 6))
           .withColumn("run_at", F.current_timestamp()))
    out.write.mode("append").saveAsTable(f"{S}.dq_results")
    display(out.filter("failing_rows > 0"))
    errors = out.filter("severity = 'error' AND failing_rows > 0").collect()
    if errors:
        raise ValueError(f"{len(errors)} error-level check(s) failed: "
                         + ", ".join(f"{r.table_name}.{r.rule}={r.failing_rows}" for r in errors))
    print(f"{len(results)} checks run, no error-level failures")

# COMMAND ----------
# 1. schema first: the SQL below crashes if a column is missing
for table, cols in EXPECTED_COLUMNS.items():
    missing = set(cols) - set(spark.table(f"{S}.{table}").columns)
    log(table, "expected_columns_present", "schema_evolution", "error", len(missing), len(cols))
    if missing:
        print(f"{table} is missing: {sorted(missing)}")

if any(r[4] > 0 for r in results):
    finish()   # records the failure and raises

# COMMAND ----------
# 2. row-level rules: one scan per table, all rules at once
for table, rules in ROW_RULES.items():
    exprs = ", ".join(f"count_if({pred}) AS r{i}" for i, (_, _, _, pred) in enumerate(rules))
    row = spark.sql(f"SELECT count(*) AS total, {exprs} FROM {S}.{table}").first()
    for i, (name, tech, sev, _) in enumerate(rules):
        log(table, name, tech, sev, row[f"r{i}"], row["total"])

# 3. deduplication: key uniqueness
for table, key in UNIQUE_KEYS.items():
    r = spark.sql(f"SELECT count(*) AS n, count(DISTINCT {key}) AS d FROM {S}.{table}").first()
    log(table, f"unique_{key}", "deduplication", "error", r["n"] - r["d"], r["n"])

# 4. joins and filtering: no orphans, counts add up
type_counts = {r["type"]: r["count"] for r in spark.table(f"{S}.events").groupBy("type").count().collect()}

def quarantined_in_clean_events(table):
    q = f"{S}.{table}_quarantine"
    if not spark.catalog.tableExists(q):
        return 0
    return spark.sql(f"SELECT count(*) FROM {q} q LEFT SEMI JOIN {S}.events e "
                     f"ON q.event_id = e.event_id").first()[0]

for table, etype in TYPE_TABLES.items():
    orphans = spark.sql(f"SELECT count(*) FROM {S}.{table} t LEFT ANTI JOIN {S}.events e "
                        f"ON t.event_id = e.event_id").first()[0]
    n = spark.table(f"{S}.{table}").count()
    expected = type_counts.get(etype, 0)
    q = quarantined_in_clean_events(table)
    log(table, "no_orphan_rows", "joins", "error", orphans, n)
    log(table, "row_count_matches_events", "filtering", "error", abs(expected - n - q), expected)

# COMMAND ----------
finish()