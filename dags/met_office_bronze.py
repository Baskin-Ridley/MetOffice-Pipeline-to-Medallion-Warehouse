import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.utils.trigger_rule import TriggerRule

GCS_BUCKET = os.environ.get("GCS_BUCKET")
DAGS_GCS_PATH = f"gs://{GCS_BUCKET}/dags"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "project_id": "noaa-medallion-warehouse",
    "region": "europe-west2",
}

def determine_bronze_branch(**context):
    """Checks if a specific run mode was requested by the caller pipeline."""
    run_mode = context["dag_run"].conf.get("run_mode", "all")
    
    if run_mode == "observations":
        return "load_met_office_land_observations_to_bronze"
    return "load_met_office_metadata_to_bronze"

with DAG(
    dag_id="met_office_bronze",
    default_args=DEFAULT_ARGS,
    description="Daily Bronze layer processing for Met Office landed data",
    schedule_interval="@daily",
    start_date=datetime(2026, 5, 23),
    catchup=False,
    tags=["met-office", "bronze"],
) as dag:

    check_run_mode = BranchPythonOperator(
        task_id="check_run_mode",
        python_callable=determine_bronze_branch,
    )

    metadata_bronze = DataprocCreateBatchOperator(
        task_id="load_met_office_metadata_to_bronze",
        batch_id="met-office-metadata-{{ ts_nodash | lower }}-{{ task_instance.try_number }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"{DAGS_GCS_PATH}/scripts/bronze/load_met_office_metadata_to_bronze.py",
                "python_file_uris": [f"{DAGS_GCS_PATH}/scripts/common/file_utils.py"],
            }
        },
    )

    observations_bronze = DataprocCreateBatchOperator(
        task_id="load_met_office_land_observations_to_bronze",
        batch_id="met-office-obs-{{ ts_nodash | lower }}-{{ task_instance.try_number }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"{DAGS_GCS_PATH}/scripts/bronze/load_met_office_land_observations_to_bronze.py",
                "python_file_uris": [f"{DAGS_GCS_PATH}/common/file_utils.py"],
            }
        },
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    check_run_mode >> metadata_bronze >> observations_bronze
    check_run_mode >> observations_bronze