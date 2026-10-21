from datetime import datetime, timezone
import os
from airflow.sdk import Param, dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


@dag(
    dag_id="gold_layer_pipeline",
    start_date=datetime(2026, 10, 3, tzinfo=timezone.utc),
    schedule=None,                 # manual for now
    catchup=False,
    max_active_runs=1,
)
def gold_layer_pipeline():
    # starts the Databricks job and waits for it to finish. If a dbt model or test fails,
    # the job fails and this task turns red.
    # The name renders to: {{ 'gold_dbt_prod' if params.env == 'prod' else 'gold_dbt' }}
    DatabricksRunNowOperator(
        task_id="dbt_build_gold",
        databricks_conn_id="databricks_default",
        job_id=os.environ["GOLD_JOB_ID"],
        retries=0,                 # a failed model or test is not fixed by retrying
    )


gold_layer_pipeline()