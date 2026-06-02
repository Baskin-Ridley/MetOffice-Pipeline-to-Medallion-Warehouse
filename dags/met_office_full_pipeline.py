import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

GCS_BUCKET = os.environ.get("GCS_BUCKET")
GCS_DAGS_PATH = f"gs://{GCS_BUCKET}/dags"
DATALAKE_BUCKET = Variable.get("datalake_bucket")
PROJECT_ID = Variable.get("project_id")
SPARK_JARS_PACKAGES = Variable.get("spark_jars_packages", "io.delta:delta-spark_2.13:3.1.0")
GCP_REGION = os.environ.get("GCP_REGION", "europe-west2")

DAG_START_DATE = datetime(2026, 5, 23)

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "project_id": PROJECT_ID,
    "region": GCP_REGION,
}

with DAG(
    dag_id="met_office_full_pipeline",
    default_args=DEFAULT_ARGS,
    description="Master controller",
    schedule="@daily",
    start_date=DAG_START_DATE,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["met-office", "full-pipeline"],
) as dag:

    trigger_ingestion_layer = TriggerDagRunOperator(
        task_id="trigger_met_office_api_ingestion",
        trigger_dag_id="met_office_api_ingestion",
        conf={"run_mode": "metadata_only"},
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=15,
    )

    trigger_bronze_layer = TriggerDagRunOperator(
        task_id="trigger_met_office_bronze",
        trigger_dag_id="met_office_bronze",
        conf={
            "run_mode": "metadata_only",
            "gcs_dags_path": GCS_DAGS_PATH,
            "datalake_bucket": DATALAKE_BUCKET,
            "spark_jars_packages": SPARK_JARS_PACKAGES,
        },
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_silver_layer = TriggerDagRunOperator(
        task_id="trigger_met_office_silver",
        trigger_dag_id="met_office_silver",
        conf={
            "run_mode": "metadata_only",
            "gcs_dags_path": GCS_DAGS_PATH,
            "datalake_bucket": DATALAKE_BUCKET,
            "spark_jars_packages": SPARK_JARS_PACKAGES,
        },
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_observations_ingestion = TriggerDagRunOperator(
        task_id="trigger_met_office_observations_ingestion",
        trigger_dag_id="met_office_api_ingestion",
        conf={"run_mode": "observations"},
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=15,
    )

    trigger_bronze_observations = TriggerDagRunOperator(
        task_id="trigger_met_office_bronze_observations",
        trigger_dag_id="met_office_bronze",
        conf={
            "run_mode": "observations",
            "gcs_dags_path": GCS_DAGS_PATH,
            "datalake_bucket": DATALAKE_BUCKET,
            "spark_jars_packages": SPARK_JARS_PACKAGES,
        },
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_silver_observations = TriggerDagRunOperator(
        task_id="trigger_met_office_silver_observations",
        trigger_dag_id="met_office_silver",
        conf={
            "run_mode": "observations",
            "gcs_dags_path": GCS_DAGS_PATH,
            "datalake_bucket": DATALAKE_BUCKET,
            "spark_jars_packages": SPARK_JARS_PACKAGES,
        },
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_gold_dim_date = TriggerDagRunOperator(
        task_id="trigger_met_office_gold_dim_date",
        trigger_dag_id="met_office_gold",
        conf={
            "run_mode": "dim_date",
            "gcs_dags_path": GCS_DAGS_PATH,
            "datalake_bucket": DATALAKE_BUCKET,
            "spark_jars_packages": SPARK_JARS_PACKAGES,
        },
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_gold_dim_stations = TriggerDagRunOperator(
        task_id="trigger_met_office_gold_dim_stations",
        trigger_dag_id="met_office_gold",
        conf={
            "run_mode": "dim_stations",
            "gcs_dags_path": GCS_DAGS_PATH,
            "datalake_bucket": DATALAKE_BUCKET,
            "spark_jars_packages": SPARK_JARS_PACKAGES,
        },
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_gold_facts = TriggerDagRunOperator(
        task_id="trigger_met_office_gold_facts",
        trigger_dag_id="met_office_gold",
        conf={
            "run_mode": "facts",
            "gcs_dags_path": GCS_DAGS_PATH,
            "datalake_bucket": DATALAKE_BUCKET,
            "spark_jars_packages": SPARK_JARS_PACKAGES,
        },
        wait_for_completion=True,
        reset_dag_run=True,
    )

    trigger_ingestion_layer >> trigger_bronze_layer >> trigger_silver_layer

    trigger_silver_layer >> trigger_observations_ingestion >> trigger_bronze_observations >> trigger_silver_observations

    trigger_silver_layer >> trigger_gold_dim_date >> trigger_gold_dim_stations
    trigger_silver_observations >> trigger_gold_facts