"""
Smoke tests for DAG structure.
If any of these fail the DAG is broken before it even reaches Airflow.
"""
import pytest


@pytest.fixture(scope="module")
def full_pipeline_dag():
    import met_office_full_pipeline
    return met_office_full_pipeline.dag


@pytest.fixture(scope="module")
def bronze_dag():
    import met_office_bronze
    return met_office_bronze.dag


@pytest.fixture(scope="module")
def silver_dag():
    import met_office_silver
    return met_office_silver.dag


@pytest.fixture(scope="module")
def gold_dag():
    import met_office_gold
    return met_office_gold.dag


# ---------------------------------------------------------------------------
# Full pipeline DAG
# ---------------------------------------------------------------------------

def test_full_pipeline_has_nine_tasks(full_pipeline_dag):
    assert len(full_pipeline_dag.tasks) == 9


def test_bronze_runs_after_ingestion(full_pipeline_dag):
    bronze_task = full_pipeline_dag.get_task("trigger_met_office_bronze")
    assert "trigger_met_office_api_ingestion" in bronze_task.upstream_task_ids


def test_silver_runs_after_bronze(full_pipeline_dag):
    silver_task = full_pipeline_dag.get_task("trigger_met_office_silver")
    assert "trigger_met_office_bronze" in silver_task.upstream_task_ids


def test_gold_facts_runs_after_observations_silver(full_pipeline_dag):
    facts_task = full_pipeline_dag.get_task("trigger_met_office_gold_facts")
    assert "trigger_met_office_silver_observations" in facts_task.upstream_task_ids


def test_observations_ingestion_runs_after_metadata_silver(full_pipeline_dag):
    obs_task = full_pipeline_dag.get_task("trigger_met_office_observations_ingestion")
    assert "trigger_met_office_silver" in obs_task.upstream_task_ids


# ---------------------------------------------------------------------------
# Sub-DAG structure
# ---------------------------------------------------------------------------

def test_bronze_dag_has_three_tasks(bronze_dag):
    # check_run_mode + metadata_bronze + observations_bronze
    assert len(bronze_dag.tasks) == 3


def test_silver_dag_has_three_tasks(silver_dag):
    assert len(silver_dag.tasks) == 3


def test_gold_dag_has_four_tasks(gold_dag):
    # check_run_mode + dim_date + dim_stations + fact_weather_metrics
    assert len(gold_dag.tasks) == 4
