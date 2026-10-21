# gharchive_gold (dbt-databricks)

Gold layer for the GH Archive lakehouse. Reads the clean silver tables and builds:

- **core**: 5 conformed dimensions + 7 fact tables (galaxy schema / fact constellation)
- **marts**: 5 business-ready tables built only from the dims and facts
- **obt**: optional one-big-table models, off by default (`--vars '{build_obt: true}'`)

## Run
```bash
export DATABRICKS_HOST=dbc-xxxx.cloud.databricks.com   # no https://
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxx
export DATABRICKS_TOKEN=...
dbt debug --profiles-dir .
dbt build --target dev --profiles-dir .                 # models + tests, in dependency order
dbt build --target prod --profiles-dir .                # silver -> gold, gold schema without the _dev suffix
```
`dbt build` runs each model and its tests, and skips everything downstream of a failed test.
