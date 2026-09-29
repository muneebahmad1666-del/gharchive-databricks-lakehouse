import pytest

pytest.importorskip("airflow")  # skipped in CI, runs inside the Airflow container


def test_dag_imports():
    from airflow.models import DagBag

    bag = DagBag(dag_folder="dags", include_examples=False)
    assert not bag.import_errors
    assert "gharchive_ingestion" in bag.dags
