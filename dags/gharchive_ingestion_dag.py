from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="gharchive_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    params={"start_date": "2023-01-01", "days": 1, "hours_per_chunk": 6},
)

@dag(
    dag_id="databricks_trigger",
    start_date=datetime(2026, 10, 3),
    schedule="30 22 * * *",
    catchup=True,
    max_active_tasks=4
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

    upload_chunk.expand(urls=plan_file_chunks())




def databricks_trigger():
    @task(retries=3, retry_delay=timedelta(minutes=2), max_active_tis_per_dag=4)
    from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

    return DatabricksRunNowOperator(
        task_id="databricks_connect",
        databricks_conn_id="databricks_default",
        job_id=834425134513479,  # Replace with your actual Databricks job ID
    )

gharchive_ingestion() >> databricks_trigger()


