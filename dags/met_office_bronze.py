from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

DAGS_GCS_PATH = conf.get("core", "dags_folder").rstrip("/")

print(f"--- DEBUG: DAGS_GCS_PATH is {DAGS_GCS_PATH} ---")

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

with DAG(
    dag_id="met_office_bronze",
    default_args=DEFAULT_ARGS,
    description="Daily Bronze layer processing for Met Office landed data",
    schedule_interval="@daily",
    start_date=datetime(2026, 5, 23),
    catchup=False,
    tags=["met-office", "bronze"],
) as dag:

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
                "python_file_uris": [f"{DAGS_GCS_PATH}/scripts/common/file_utils.py"],
            }
        },
    )

    metadata_bronze >> observations_bronze