# gharchive-databricks-lakehouse

Airflow lands GH Archive hourly files in a Unity Catalog Volume; a Databricks job loads them into `workspace.bronze.gharchive_events_raw`.

## Run the ingestion pipeline
1. Databricks SQL editor: run `include/sql/00_setup_unity_catalog.sql` once.
2. `cp .env.example .env` and fill `FERNET_KEY`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`.
3. `docker compose up airflow-init` then `docker compose up -d --build`
4. Open http://localhost:8080, unpause and trigger `gharchive_ingestion`.
   Defaults load 1 day (24 files). Raise `days` / `days_per_chunk` in the trigger form for more.

## Branch flow
`feature/*` -> PR to `dev` (ci-checks) -> PR `dev` to `main` (ci-checks, then deploy-prod).
