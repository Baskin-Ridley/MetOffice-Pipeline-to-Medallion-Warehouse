import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.configuration import conf
from airflow.operators.python import PythonOperator

DAGS_GCS_PATH = conf.get("core", "dags_folder").rstrip("/")
if DAGS_GCS_PATH not in sys.path:
    sys.path.append(DAGS_GCS_PATH)

from scripts.ingestion.ingest_met_office_metadata_to_landed import main as ingest_metadata_main
from scripts.ingestion.ingest_met_office_land_observations_to_landed import main as ingest_observations_main

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="met_office_api_ingestion",
    default_args=DEFAULT_ARGS,
    description="Daily Met Office API ingestion to the landed data layer",
    schedule_interval=None,
    start_date=datetime(2026, 5, 23),
    catchup=False,
    tags=["met-office", "ingestion"],
) as dag:

    ingest_metadata = PythonOperator(
        task_id="ingest_met_office_metadata",
        python_callable=ingest_metadata_main,
    )

    ingest_observations = PythonOperator(
        task_id="ingest_met_office_observations",
        python_callable=ingest_observations_main,
    )

    ingest_metadata >> ingest_observations