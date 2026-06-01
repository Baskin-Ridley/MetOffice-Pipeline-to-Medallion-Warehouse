"""
Tests for the BranchPythonOperator callables in each DAG.
These functions determine which task(s) to run based on `run_mode`.
"""
import pytest
from unittest.mock import MagicMock


def _ctx(run_mode=None):
    """Build a minimal Airflow task context with the given run_mode."""
    dag_run = MagicMock()
    dag_run.conf = {"run_mode": run_mode} if run_mode else {}
    return {"dag_run": dag_run}


# ---------------------------------------------------------------------------
# Ingestion DAG
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def ingestion_branch():
    from met_office_api_ingestion import determine_ingestion_branch
    return determine_ingestion_branch


def test_ingestion_branch_observations(ingestion_branch):
    assert ingestion_branch(**_ctx("observations")) == "ingest_met_office_observations"


def test_ingestion_branch_metadata_only(ingestion_branch):
    assert ingestion_branch(**_ctx("metadata_only")) == "ingest_met_office_metadata"


def test_ingestion_branch_default_returns_both(ingestion_branch):
    result = ingestion_branch(**_ctx())
    assert isinstance(result, list)
    assert set(result) == {"ingest_met_office_metadata", "ingest_met_office_observations"}


# ---------------------------------------------------------------------------
# Bronze DAG
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def bronze_branch():
    from met_office_bronze import determine_bronze_branch
    return determine_bronze_branch


def test_bronze_branch_observations(bronze_branch):
    assert bronze_branch(**_ctx("observations")) == "load_met_office_land_observations_to_bronze"


def test_bronze_branch_metadata_only(bronze_branch):
    assert bronze_branch(**_ctx("metadata_only")) == "load_met_office_metadata_to_bronze"


def test_bronze_branch_default_returns_both(bronze_branch):
    result = bronze_branch(**_ctx())
    assert isinstance(result, list)
    assert set(result) == {
        "load_met_office_metadata_to_bronze",
        "load_met_office_land_observations_to_bronze",
    }


# ---------------------------------------------------------------------------
# Gold DAG
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def gold_branch():
    from met_office_gold import determine_gold_branch
    return determine_gold_branch


def test_gold_branch_dim_date(gold_branch):
    assert gold_branch(**_ctx("dim_date")) == "load_dim_date"


def test_gold_branch_dim_stations(gold_branch):
    assert gold_branch(**_ctx("dim_stations")) == "load_dim_weather_stations"


def test_gold_branch_facts(gold_branch):
    assert gold_branch(**_ctx("facts")) == "load_fact_weather_metrics"


def test_gold_branch_default_returns_all(gold_branch):
    result = gold_branch(**_ctx())
    assert isinstance(result, list)
    assert set(result) == {"load_dim_date", "load_dim_weather_stations", "load_fact_weather_metrics"}
