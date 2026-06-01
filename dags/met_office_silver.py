import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

GCS_BUCKET = os.environ.get("GCS_BUCKET")
DAGS_GCS_PATH = f"gs://{GCS_BUCKET}/dags"
DATALAKE_BUCKET = Variable.get("datalake_bucket")

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


def determine_silver_branch(**context):
    """Checks if a specific run mode was requested by the caller pipeline."""
    run_mode = context["dag_run"].conf.get("run_mode", "all")

    if run_mode == "observations":
        return "load_met_office_land_observations_to_silver"
    elif run_mode == "metadata_only":
        return "load_met_office_metadata_to_silver"
    return ["load_met_office_metadata_to_silver", "load_met_office_land_observations_to_silver"]


with DAG(
    dag_id="met_office_silver",
    default_args=DEFAULT_ARGS,
    description="Daily Silver layer processing for Met Office bronze data",
    schedule_interval="@daily",
    start_date=datetime(2026, 5, 23),
    catchup=False,
    tags=["met-office", "silver"],
    params={
        "gcs_dags_path": DAGS_GCS_PATH,
        "datalake_bucket": DATALAKE_BUCKET,
        "spark_jars_packages": "io.delta:delta-spark_2.13:3.1.0",
    },
) as dag:

    check_run_mode = BranchPythonOperator(
        task_id="check_run_mode",
        python_callable=determine_silver_branch,
    )

    metadata_silver = DataprocCreateBatchOperator(
        task_id="load_met_office_metadata_to_silver",
        batch_id="met-office-metadata-silver-{{ ts_nodash | lower }}-{{ task_instance.try_number }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "{{ params.gcs_dags_path }}/scripts/silver/load_met_office_metadata_to_silver.py",
                "args": ["{{ params.datalake_bucket }}"],
            },
            "runtime_config": {
                "properties": {
                    "spark.jars.packages": "{{ params.spark_jars_packages }}"
                }
            }
        },
    )

    observations_silver = DataprocCreateBatchOperator(
        task_id="load_met_office_land_observations_to_silver",
        batch_id="met-office-obs-silver-{{ ts_nodash | lower }}-{{ task_instance.try_number }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "{{ params.gcs_dags_path }}/scripts/silver/load_met_office_land_observations_to_silver.py",
                "args": ["{{ params.datalake_bucket }}"],
            },
            "runtime_config": {
                "properties": {
                    "spark.jars.packages": "{{ params.spark_jars_packages }}"
                }
            }
        },
    )

    check_run_mode >> [metadata_silver, observations_silver]
