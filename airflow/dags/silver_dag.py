import os
from datetime import datetime, timedelta, timezone
from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

# e.g. /Workspace/Users/<your-email>/silver  (raises when the DAG loads if it isn't set)
NB = os.environ["DATABRICKS_NOTEBOOK_BASE"]

def notebook_run(task_id, notebook, params, retries=1):
    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        retries=retries,
        json={
            "run_name": task_id,
            "tasks": [{
                "task_key": task_id,
                "notebook_task": {
                    "notebook_path": f"{NB}/{notebook}",
                    "source": "WORKSPACE",
                    "base_parameters": params,
                },
            }],
        },
    )

@dag(
    dag_id="silver_layer_pipeline",
    start_date=datetime(2026, 10, 3, tzinfo=timezone.utc),
    schedule=None,                      # manual while you test; add a cron once dev works
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={"env": "prod", "start_date": "2023-01-01", "end_date": "2023-01-14"},
)
def silver_layer_pipeline():
    env = {"env": "{{ params.env }}"}

    silver_events = notebook_run(
        "silver_events", "silver_events",
        {**env, "start_date": "{{ params.start_date }}", "end_date": "{{ params.end_date }}"},
    )
    silver_transformations = notebook_run(
        "silver_transformations", "silver_transformations/main", env,
    )
    silver_quality_checks = notebook_run(
        "silver_quality_checks", "silver_quality_checks", env, retries=0,   # the gate never retries
    )

    silver_events >> silver_transformations >> silver_quality_checks

silver_layer_pipeline()