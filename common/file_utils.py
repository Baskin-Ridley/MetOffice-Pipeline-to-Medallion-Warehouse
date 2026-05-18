from upath import UPath as Path
from typing import Tuple

from pyspark.sql import SparkSession

def get_latest_version_paths(base_dir: str, target_dir: str) -> Tuple[str, str, str]:
    """
    Identifies the most recent subfolder and prepares paths for Spark.
    """
    base_path = Path(base_dir)
    target_path = Path(target_dir)

    if not base_path.exists():
        raise FileNotFoundError(
            f"The directory '{base_dir}' does not exist. "
            f"Ensure your ingestion script has run successfully to create this path."
        )

    folders = sorted([f for f in base_path.iterdir() if f.is_dir()])
    
    if not folders:
        raise FileNotFoundError(f"No subfolders found inside '{base_dir}'")

    latest_folder = folders[-1]
    
    landed_pattern = str(latest_folder / "*.json")
    version_id = latest_folder.name
    output_path = str(target_path / version_id)

    return landed_pattern, version_id, output_path

def start_spark_session(app_name: str) -> SparkSession:
    print("Connecting to Spark...")
    spark = SparkSession.builder \
        .remote("sc://spark:15002") \
        .appName(app_name) \
        .config("spark.sql.extensions.delta", "org.apache.spark.sql.delta.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    print("Connected to Spark")
    return spark

    