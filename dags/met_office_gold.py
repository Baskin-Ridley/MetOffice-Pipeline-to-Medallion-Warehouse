from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

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
    dag_id="met_office_gold",
    default_args=DEFAULT_ARGS,
    description="Daily Gold layer processing for Met Office Silver data",
    schedule_interval="@daily",
    start_date=datetime(2026, 5, 23),
    catchup=False,
    tags=["met-office", "gold"],
) as dag:

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

    dim_date >> dim_weather_stations >> fact_weather_metrics
