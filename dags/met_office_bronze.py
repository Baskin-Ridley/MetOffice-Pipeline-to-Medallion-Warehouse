from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.bronze.load_met_office_metadata_to_bronze import main as load_metadata_bronze_main
from scripts.bronze.load_met_office_land_observations_to_bronze import main as load_observations_bronze_main

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
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["met-office", "bronze"],
) as dag:

    metadata_bronze = PythonOperator(
        task_id="load_met_office_metadata_to_bronze",
        python_callable=load_metadata_bronze_main,
    )

    observations_bronze = PythonOperator(
        task_id="load_met_office_land_observations_to_bronze",
        python_callable=load_observations_bronze_main,
    )

    metadata_bronze >> observations_bronze
