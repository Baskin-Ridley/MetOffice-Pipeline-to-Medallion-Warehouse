from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.silver.load_met_office_metadata_to_silver import main as load_metadata_silver_main
from scripts.silver.load_met_office_land_observations_to_silver import main as load_observations_silver_main

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="met_office_silver",
    default_args=DEFAULT_ARGS,
    description="Daily Silver layer processing for Met Office bronze data",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["met-office", "silver"],
) as dag:

    metadata_silver = PythonOperator(
        task_id="load_met_office_metadata_to_silver",
        python_callable=load_metadata_silver_main,
    )

    observations_silver = PythonOperator(
        task_id="load_met_office_land_observations_to_silver",
        python_callable=load_observations_silver_main,
    )

    metadata_silver >> observations_silver
