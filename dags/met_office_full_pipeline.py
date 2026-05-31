import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

DAGS_GCS_PATH = conf.get("core", "dags_folder").rstrip("/")
if DAGS_GCS_PATH not in sys.path:
    sys.path.append(DAGS_GCS_PATH)

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

with DAG(
    dag_id="met_office_full_pipeline",
    default_args=DEFAULT_ARGS,
    description="Master controller",
    schedule_interval="@daily",
    start_date=datetime(2026, 5, 23),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["met-office", "full-pipeline"],
) as dag:

    trigger_ingestion_layer = TriggerDagRunOperator(
        task_id="trigger_met_office_api_ingestion",
        trigger_dag_id="met_office_api_ingestion",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    metadata_bronze = DataprocCreateBatchOperator(
        task_id="load_met_office_metadata_to_bronze",
        batch_id="met-office-metadata-{{ ds_nodash }}-{{ task_instance.try_number }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"{DAGS_GCS_PATH}/scripts/bronze/load_met_office_metadata_to_bronze.py",
                "python_file_uris": [f"{DAGS_GCS_PATH}/scripts/common/file_utils.py"],
                "args": [DATALAKE_BUCKET],
            }
        },
    )

    trigger_ingestion_layer >> metadata_bronze