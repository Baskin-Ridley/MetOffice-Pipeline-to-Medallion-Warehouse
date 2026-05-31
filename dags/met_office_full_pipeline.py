from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from scripts.ingestion.ingest_met_office_land_observations_to_landed import main as ingest_observations_main
from scripts.ingestion.ingest_met_office_metadata_to_landed import main as ingest_metadata_main
from scripts.silver.load_met_office_land_observations_to_silver import main as load_observations_silver_main
from scripts.silver.load_met_office_metadata_to_silver import main as load_metadata_silver_main
from scripts.gold.load_dim_date import main as load_dim_date_main
from scripts.gold.load_dim_weather_stations import main as load_dim_weather_stations_main
from scripts.gold.load_fact_weather_metrics import main as load_fact_weather_metrics_main

DAGS_GCS_PATH = conf.get("core", "dags_folder").rstrip("/")

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
    description="End-to-end Met Office medallion pipeline including ingestion and all transformation layers",
    schedule_interval="@daily",
    start_date=datetime(2026, 5, 23),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["met-office", "full-pipeline"],
) as dag:

    # --- METADATA PIPELINE TRACK ---
    ingest_metadata = PythonOperator(
        task_id="ingest_met_office_metadata",
        python_callable=ingest_metadata_main,
    )

    trigger_metadata_bronze = TriggerDagRunOperator(
        task_id="trigger_metadata_bronze",
        trigger_dag_id="met_office_bronze",
        conf={"run_mode": "metadata"},
        wait_for_completion=True,
        reset_dag_run=True,
    )

    metadata_silver = PythonOperator(
        task_id="load_met_office_metadata_to_silver",
        python_callable=load_metadata_silver_main,
    )

    # --- OBSERVATIONS PIPELINE TRACK ---
    ingest_observations = PythonOperator(
        task_id="ingest_met_office_observations",
        python_callable=ingest_observations_main,
    )

    trigger_observations_bronze = TriggerDagRunOperator(
        task_id="trigger_observations_bronze",
        trigger_dag_id="met_office_bronze",
        conf={"run_mode": "observations"},
        wait_for_completion=True,
        reset_dag_run=True,
    )

    observations_silver = PythonOperator(
        task_id="load_met_office_land_observations_to_silver",
        python_callable=load_observations_silver_main,
    )

    # --- GOLD LAYER ---
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

    # --- LINEAR DEPENDENCY PIPELINE MAP ---
    
    # 1. Force the entire Metadata lifecycle to finish all the way through Silver first
    ingest_metadata >> trigger_metadata_bronze >> metadata_silver

    # 2. Start observations only after Metadata hits Silver
    metadata_silver >> ingest_observations >> trigger_observations_bronze >> observations_silver

    # 3. Gold operations branch out independently as soon as their respective source layers finish
    metadata_silver >> dim_date >> dim_weather_stations
    observations_silver >> fact_weather_metrics