import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

GCS_BUCKET = os.environ.get("GCS_BUCKET")
DAGS_GCS_PATH = f"gs://{GCS_BUCKET}/dags"
DATALAKE_BUCKET = Variable.get("datalake_bucket")
SPARK_JARS_PACKAGES = Variable.get("spark_jars_packages", "io.delta:delta-spark_2.13:3.1.0")
GCP_REGION = os.environ.get("GCP_REGION", "europe-west2")

DAG_START_DATE = datetime(2026, 5, 23)

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "project_id": "noaa-medallion-warehouse",
    "region": GCP_REGION,
}

def determine_bronze_branch(**context):
    """Checks if a specific run mode was requested by the caller pipeline."""
    run_mode = context["dag_run"].conf.get("run_mode", "all")

    if run_mode == "observations":
        return "load_met_office_land_observations_to_bronze"
    elif run_mode == "metadata_only":
        return "load_met_office_metadata_to_bronze"
    return ["load_met_office_metadata_to_bronze", "load_met_office_land_observations_to_bronze"]

with DAG(
    dag_id="met_office_bronze",
    default_args=DEFAULT_ARGS,
    description="Daily Bronze layer processing for Met Office landed data",
    schedule=None,
    start_date=DAG_START_DATE,
    catchup=False,
    tags=["met-office", "bronze"],
    params={
        "gcs_dags_path": DAGS_GCS_PATH,
        "datalake_bucket": DATALAKE_BUCKET,
        "spark_jars_packages": SPARK_JARS_PACKAGES,
    },
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
                "main_python_file_uri": "{{ params.gcs_dags_path }}/scripts/bronze/load_met_office_metadata_to_bronze.py",
                "python_file_uris": ["{{ params.gcs_dags_path }}/common/file_utils.py"],
                "args": ["{{ params.datalake_bucket }}"],
            },
            "runtime_config": {
                "properties": {
                    "spark.jars.packages": "{{ params.spark_jars_packages }}"
                }
            }
        },
    )

    observations_bronze = DataprocCreateBatchOperator(
        task_id="load_met_office_land_observations_to_bronze",
        batch_id="met-office-obs-{{ ts_nodash | lower }}-{{ task_instance.try_number }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "{{ params.gcs_dags_path }}/scripts/bronze/load_met_office_land_observations_to_bronze.py",
                "python_file_uris": ["{{ params.gcs_dags_path }}/common/file_utils.py"],
                "args": ["{{ params.datalake_bucket }}"],
            },
            "runtime_config": {
                "properties": {
                    "spark.jars.packages": "{{ params.spark_jars_packages }}"
                }
            }
        },
    )

    check_run_mode >> [metadata_bronze, observations_bronze]