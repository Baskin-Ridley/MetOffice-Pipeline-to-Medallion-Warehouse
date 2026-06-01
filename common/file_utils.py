import logging
import posixpath
from typing import Tuple
from urllib.parse import urlparse
from google.cloud import storage
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

def get_latest_version_paths(base_dir: str, target_dir: str) -> Tuple[str, str, str]:
    """
    Identifies the most recent subfolder and prepares paths for Spark.
    """
    parsed_base = urlparse(base_dir)
    bucket_name = parsed_base.netloc
    base_prefix = parsed_base.path.lstrip("/")
    
    if base_prefix and not base_prefix.endswith("/"):
        base_prefix += "/"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    if not bucket.exists():
        raise FileNotFoundError(f"The GCS bucket '{bucket_name}' does not exist.")

    try:
        blobs = bucket.list_blobs(prefix=base_prefix, delimiter='/')
        list(blobs)
        folders = sorted(list(blobs.prefixes))
    except Exception as e:
        raise FileNotFoundError(
            f"Unable to list directories under '{base_dir}'. "
            f"Ensure the path is accessible and contains valid subfolders."
        ) from e
    
    if not folders:
        raise FileNotFoundError(
            f"The directory '{base_dir}' does not exist or contains no subfolders. "
            f"Ensure your ingestion script has run successfully to create this path."
        )

    latest_folder_prefix = folders[-1]
    clean_folder_prefix = latest_folder_prefix.rstrip("/")
    version_id = clean_folder_prefix.split("/")[-1]
    
    latest_folder_uri = f"gs://{bucket_name}/{clean_folder_prefix}"
    landed_pattern = f"{latest_folder_uri}/*.json"
    
    parsed_target = urlparse(target_dir)
    target_bucket = parsed_target.netloc
    target_prefix = parsed_target.path.strip("/")
    output_path = f"gs://{target_bucket}/{posixpath.join(target_prefix, version_id)}"

    return landed_pattern, version_id, output_path

def start_spark_session(app_name: str) -> SparkSession:
    logger.info("Connecting to Spark...")
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    logger.info("Connected to Spark")
    return spark