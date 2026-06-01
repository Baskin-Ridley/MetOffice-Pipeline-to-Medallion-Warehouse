import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

GCS_BUCKET = os.environ.get("GCS_BUCKET")
GCS_DAGS_PATH = f"gs://{GCS_BUCKET}/dags"
DATALAKE_BUCKET = Variable.get("datalake_bucket")

def log_debug_info():
    from airflow.configuration import conf
    dags_path = conf.get("core", "dags_folder").rstrip("/")
    print(f"--- DEBUG: conf core dags_folder is {dags_path} ---")
    print(f"--- DEBUG: GCS_BUCKET env var is {os.environ.get('GCS_BUCKET')} ---")
    print(f"--- DEBUG: Resolved GCS_DAGS_PATH is gs://{os.environ.get('GCS_BUCKET')}/dags ---")

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

    debug_check = PythonOperator(
        task_id="debug_environment_paths",
        python_callable=log_debug_info,
    )

    trigger_ingestion_layer = TriggerDagRunOperator(
        task_id="trigger_met_office_api_ingestion",
        trigger_dag_id="met_office_api_ingestion",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_bronze_layer = TriggerDagRunOperator(
        task_id="trigger_met_office_bronze",
        trigger_dag_id="met_office_bronze",
        conf={
            "run_mode": "metadata_only",
            "gcs_dags_path": GCS_DAGS_PATH,
            "datalake_bucket": DATALAKE_BUCKET,
            "spark_jars_packages": "io.delta:delta-spark_2.13:3.1.0",
        },
        wait_for_completion=True,
        reset_dag_run=True,
    )

    debug_check >> trigger_ingestion_layer >> trigger_bronze_layer