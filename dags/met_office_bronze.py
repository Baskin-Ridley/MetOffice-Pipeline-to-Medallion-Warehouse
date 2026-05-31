from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.models import Variable

PROJECT_ID = "noaa-medallion-warehouse"
REGION = "europe-west2"
BUCKET_NAME = Variable.get("datalake_bucket", "your-gcp-datalake-bucket")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
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
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="met-office-metadata-{{ ds_nodash }}-{{ task_instance.try_number }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/bronze/load_met_office_metadata_to_bronze.py",
                "python_file_uris": [f"gs://{BUCKET_NAME}/scripts/common/file_utils.py"]
            }
        }
    )

    observations_bronze = DataprocCreateBatchOperator(
        task_id="load_met_office_land_observations_to_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="met-office-obs-{{ ds_nodash }}-{{ task_instance.try_number }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/bronze/load_met_office_land_observations_to_bronze.py",
                "python_file_uris": [f"gs://{BUCKET_NAME}/scripts/common/file_utils.py"]
            }
        }
    )

    metadata_bronze >> observations_bronze