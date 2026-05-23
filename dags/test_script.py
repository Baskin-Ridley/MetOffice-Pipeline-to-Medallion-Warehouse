from datetime import datetime
from airflow import DAG
from airflow.models import Variable  # Added to fetch Terraform variables
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Fetch variables provisioned via Terraform
PROJECT_ID = Variable.get("project_id", default_var="your-gcp-project-id")
REGION = Variable.get("region", default_var="europe-west2")
BUCKET = Variable.get("data_lake_root_path", default_var="gs://your-global-datalake-bucket")

# Constructing matching environment URLs dynamically
ARTIFACT_REGISTRY_URL = f"{REGION}-docker.pkg.dev/{PROJECT_ID}/met-office-repo"
CLUSTER_NAME = "ephemeral-spark-cluster-met-office"

CLUSTER_CONFIG = {
    "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-4"},
    "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-4"},
    "software_config": {
        "image_version": "2.1-debian11", 
        "properties": {
            "spark:spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark:spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    },
}

default_args = {
    "start_date": datetime(2026, 1, 1),
    "depends_on_past": False,
}

with DAG(
    dag_id="met_office_medallion_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    # 1. Ingest Metadata
    ingest_metadata = KubernetesPodOperator(
        task_id="ingest_metadata_to_landed",
        image=f"{ARTIFACT_REGISTRY_URL}/polars-ingest:latest",
        cmds=["python", "scripts/ingestion/ingest_met_office_metadata_to_landed.py"],
        name="ingest-metadata"
    )

    # 2. Ingest Observations
    ingest_observations = KubernetesPodOperator(
        task_id="ingest_observations_to_landed",
        image=f"{ARTIFACT_REGISTRY_URL}/polars-ingest:latest",
        cmds=["python", "scripts/ingestion/ingest_met_office_land_observations_to_landed.py"],
        name="ingest-observations"
    )

    # 3. Spin up the heavy Spark infrastructure
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id="create_spark_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    # 4. Submit your Spark scripts step-by-step to the cluster
    load_metadata_bronze = DataprocSubmitJobOperator(
        task_id="metadata_to_bronze",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": f"{BUCKET}/scripts/bronze/load_met_office_metadata_to_bronze.py"},
        },
        region=REGION,
    )

    load_metadata_silver = DataprocSubmitJobOperator(
        task_id="metadata_to_silver",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": f"{BUCKET}/scripts/silver/load_met_office_metadata_to_silver.py"},
        },
        region=REGION,
    )

    load_observations_bronze = DataprocSubmitJobOperator(
        task_id="observations_to_bronze",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": f"{BUCKET}/scripts/bronze/load_met_office_land_observations_to_bronze.py"},
        },
        region=REGION,
    )

    load_observations_silver = DataprocSubmitJobOperator(
        task_id="observations_to_silver",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": f"{BUCKET}/scripts/silver/load_met_office_land_observations_to_silver.py"},
        },
        region=REGION,
    )

    # 5. Gold Layer Transformations
    load_fact_metrics = DataprocSubmitJobOperator(
        task_id="load_fact_metrics",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": f"{BUCKET}/scripts/gold/load_fact_weather_metrics.py"},
        },
        region=REGION,
    )

    load_dim_stations = DataprocSubmitJobOperator(
        task_id="load_dim_stations",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": f"{BUCKET}/scripts/gold/load_dim_weather_stations.py"},
        },
        region=REGION,
    )

    load_dim_date = DataprocSubmitJobOperator(
        task_id="load_dim_date",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": f"{BUCKET}/scripts/gold/load_dim_date.py"},
        },
        region=REGION,
    )

    # 6. Shut down the Dataproc cluster
    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id="delete_spark_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done" 
    )

    # --- SET RUNTIME DEPENDENCIES ---
    ingest_metadata >> create_spark_cluster >> load_metadata_bronze >> load_metadata_silver
    ingest_observations >> create_spark_cluster >> load_observations_bronze >> load_observations_silver
    
    [load_metadata_silver, load_observations_silver] >> load_fact_metrics >> load_dim_stations >> load_dim_date >> delete_spark_cluster