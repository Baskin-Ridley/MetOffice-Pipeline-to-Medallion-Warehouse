from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.ingestion.ingest_met_office_metadata_to_landed import main as ingest_metadata_main
from scripts.ingestion.ingest_met_office_land_observations_to_landed import main as ingest_observations_main
from scripts.bronze.load_met_office_metadata_to_bronze import main as load_metadata_bronze_main
from scripts.bronze.load_met_office_land_observations_to_bronze import main as load_observations_bronze_main
from scripts.silver.load_met_office_metadata_to_silver import main as load_metadata_silver_main
from scripts.silver.load_met_office_land_observations_to_silver import main as load_observations_silver_main
from scripts.gold.load_dim_date import main as load_dim_date_main
from scripts.gold.load_dim_weather_stations import main as load_dim_weather_stations_main
from scripts.gold.load_fact_weather_metrics import main as load_fact_weather_metrics_main

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="met_office_full_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end Met Office medallion pipeline including ingestion and all transformation layers",
    schedule_interval="@daily",
    start_date=datetime(2026, 5, 23),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["met-office", "full-pipeline"],
) as dag:

    ingest_metadata = PythonOperator(
        task_id="ingest_met_office_metadata",
        python_callable=ingest_metadata_main,
    )

    metadata_bronze = PythonOperator(
        task_id="load_met_office_metadata_to_bronze",
        python_callable=load_metadata_bronze_main,
    )

    metadata_silver = PythonOperator(
        task_id="load_met_office_metadata_to_silver",
        python_callable=load_metadata_silver_main,
    )

    ingest_observations = PythonOperator(
        task_id="ingest_met_office_observations",
        python_callable=ingest_observations_main,
    )

    observations_bronze = PythonOperator(
        task_id="load_met_office_land_observations_to_bronze",
        python_callable=load_observations_bronze_main,
    )

    observations_silver = PythonOperator(
        task_id="load_met_office_land_observations_to_silver",
        python_callable=load_observations_silver_main,
    )

    dim_date = PythonOperator(
        task_id="load_dim_date",
        python_callable=load_dim_date_main,
    )

    dim_weather_stations = PythonOperator(
        task_id="load_dim_weather_stations",
        python_callable=load_dim_weather_stations_main,
    )

    fact_weather_metrics = PythonOperator(
        task_id="load_fact_weather_metrics",
        python_callable=load_fact_weather_metrics_main,
    )

    ingest_metadata >> metadata_bronze >> metadata_silver >> ingest_observations >> observations_bronze >> observations_silver
    metadata_silver >> dim_weather_stations
    observations_silver >> fact_weather_metrics
    metadata_silver >> dim_date
    dim_date >> dim_weather_stations
