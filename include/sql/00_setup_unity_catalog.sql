-- Run ONCE in the Databricks SQL editor before triggering the DAG.
-- `workspace` is the pre-provisioned default catalog, so no CREATE CATALOG.

CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;

-- Landing area: Airflow uploads raw hourly .json.gz files here.
CREATE VOLUME IF NOT EXISTS workspace.bronze.raw_landing;

-- The table workspace.bronze.gharchive_events_raw is created by the
-- gharchive_bronze_ingest job, not here.
