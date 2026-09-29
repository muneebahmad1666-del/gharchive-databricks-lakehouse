from datetime import datetime, timedelta

from airflow.sdk import dag, get_current_context, task

from gharchive_lakehouse.plan_gharchive_chunks import plan_gharchive_chunks
from gharchive_lakehouse.upload_chunk import upload_chunk_files


@dag(
    dag_id="gharchive_ingestion",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"start_date": "2023-01-01", "days": 1, "days_per_chunk": 1},  # smoke test: 24 files
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["gharchive", "bronze"],
)
def gharchive_ingestion():
    @task(task_id="plan_chunks")
    def plan_chunks() -> list[list[str]]:
        p = get_current_context()["params"]
        return plan_gharchive_chunks(p["start_date"], int(p["days"]), int(p["days_per_chunk"]))

    @task(task_id="upload_chunk", max_active_tis_per_dag=4)
    def upload_chunk(chunk: list[str]) -> dict[str, int]:
        return upload_chunk_files(chunk)

    upload_chunk.expand(chunk=plan_chunks())  # dynamic task mapping: one task per chunk


gharchive_ingestion()
