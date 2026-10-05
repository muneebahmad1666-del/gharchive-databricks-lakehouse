import os
from datetime import datetime, timedelta, timezone
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import Asset, dag, task

# Signal that connects the two DAGs
files_landed = Asset("gharchive_files_landed")


# ---------- DAG 1: download GH Archive files into the Databricks Volume ----------
@dag(
    dag_id="gharchive_ingestion",
    start_date=datetime(2026, 10, 3, tzinfo=timezone.utc),
    schedule=None,  # set a cron here later if you want it to run on a schedule
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    params={"start_date": "2023-01-01", "days": 1, "hours_per_chunk": 6},
)
def gharchive_ingestion():
    @task
    def plan_file_chunks(**context):
        from gharchive_lakehouse.plan_gharchive_chunks import plan_gharchive_chunks

        p = context["params"]
        return plan_gharchive_chunks(p["start_date"], p["days"], p["hours_per_chunk"])

    @task(retries=3, retry_delay=timedelta(minutes=2), max_active_tis_per_dag=4)
    def upload_chunk(urls: list[str]):
        from gharchive_lakehouse.upload_chunk import upload_chunk_files

        return upload_chunk_files(urls)

    @task(outlets=[files_landed])
    def mark_landed():
        """Runs only after every upload succeeds; updating the asset triggers DAG 2."""

    uploads = upload_chunk.expand(urls=plan_file_chunks())
    uploads >> mark_landed()


# ---------- DAG 2: run the Databricks job (notebook) when new files have landed ----------
@dag(
    dag_id="databricks_trigger",
    start_date=datetime(2026, 10, 3, tzinfo=timezone.utc),
    schedule=("40 14 4 10 *"),  # starts when DAG 1 updates the asset
    catchup=False,
    max_active_runs=1,  # never two runs on the same Auto Loader checkpoint
)
def databricks_trigger():
    DatabricksRunNowOperator(
        task_id="databricks_connect",
        databricks_conn_id=("databricks_default"),
        job_id=int(os.getenv("DATABRICKS_JOB_ID")),
        retries=1,
        retry_delay=timedelta(minutes=2),
    )


gharchive_ingestion()
databricks_trigger()
