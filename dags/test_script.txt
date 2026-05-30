import os
from datetime import datetime, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

from scripts.ingestion.ingest_met_office_metadata_to_landed import main as ingest_metadata_main
from scripts.ingestion.ingest_met_office_land_observations_to_landed import main as ingest_observations_main

PROJECT_ID = Variable.get("project_id", default_var="your-gcp-project-id")
REGION = Variable.get("region", default_var="europe-west2")
DATA_LAKE = Variable.get("data_lake_root_path", default_var="gs://your-global-datalake-bucket")
CLUSTER_NAME = f"met-office-dataproc-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

CLUSTER_CONFIG = {
    "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-4"},
    "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-4"},
    "software_config": {
        "image_version": "2.1-debian11",
        "properties": {
            "spark:spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark:spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
    },
}


def run_ingest_metadata():
    os.environ["DATALAKE_ROOT"] = DATA_LAKE
    ingest_metadata_main()


def run_ingest_observations():
    os.environ["DATALAKE_ROOT"] = DATA_LAKE
    ingest_observations_main()


default_args = {
    "start_date": datetime(2026, 1, 1),
    "depends_on_past": False,
}

with DAG(
    dag_id="met_office_medallion_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["met-office", "gcp"],
) as dag:

    ingest_metadata = PythonOperator(
        task_id="ingest_metadata_to_landed",
        python_callable=run_ingest_metadata,
    )

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id="create_spark_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    load_metadata_bronze = DataprocSubmitJobOperator(
        task_id="metadata_to_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{DATA_LAKE}/scripts/bronze/load_met_office_metadata_to_bronze.py",
            },
        },
    )

    load_metadata_silver = DataprocSubmitJobOperator(
        task_id="metadata_to_silver",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{DATA_LAKE}/scripts/silver/load_met_office_metadata_to_silver.py",
            },
        },
    )

    ingest_observations = PythonOperator(
        task_id="ingest_observations_to_landed",
        python_callable=run_ingest_observations,
    )

    load_observations_bronze = DataprocSubmitJobOperator(
        task_id="observations_to_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{DATA_LAKE}/scripts/bronze/load_met_office_land_observations_to_bronze.py",
            },
        },
    )

    load_observations_silver = DataprocSubmitJobOperator(
        task_id="observations_to_silver",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{DATA_LAKE}/scripts/silver/load_met_office_land_observations_to_silver.py",
            },
        },
    )

    load_dim_date = DataprocSubmitJobOperator(
        task_id="load_dim_date",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{DATA_LAKE}/scripts/gold/load_dim_date.py",
            },
        },
    )

    load_dim_stations = DataprocSubmitJobOperator(
        task_id="load_dim_stations",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{DATA_LAKE}/scripts/gold/load_dim_weather_stations.py",
            },
        },
    )

    load_fact_metrics = DataprocSubmitJobOperator(
        task_id="load_fact_metrics",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{DATA_LAKE}/scripts/gold/load_fact_weather_metrics.py",
            },
        },
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id="delete_spark_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done",
    )

    ingest_metadata >> create_spark_cluster >> load_metadata_bronze >> load_metadata_silver >> ingest_observations
    ingest_observations >> load_observations_bronze >> load_observations_silver
    load_metadata_silver >> load_dim_date >> load_dim_stations
    load_observations_silver >> load_fact_metrics
    [load_dim_stations, load_fact_metrics] >> delete_spark_cluster
