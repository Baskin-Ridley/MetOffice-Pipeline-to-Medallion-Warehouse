import logging
import sys
import uuid
from urllib.parse import urlparse
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, date_format, from_utc_timestamp,
    input_file_name, regexp_replace, lit, sha2, concat_ws, explode, col
)
from file_utils import get_latest_version_paths

logger = logging.getLogger(__name__)


def gcs_prefix_exists(gcs_uri: str) -> bool:
    parsed = urlparse(gcs_uri)
    bucket_name = parsed.netloc
    prefix = parsed.path.lstrip("/")

    if prefix and not prefix.endswith("/"):
        prefix += "/"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix, max_results=1))
    return len(blobs) > 0


def main():
    if len(sys.argv) < 2:
        raise ValueError("Missing required datalake bucket argument.")

    BUCKET_NAME = sys.argv[1]
    DATALAKE_ROOT = f"gs://{BUCKET_NAME}"

    BRONZE_DIR = f"{DATALAKE_ROOT}/bronze/met_office/station_observation_land"
    LANDED_BASE_DIR = f"{DATALAKE_ROOT}/landed/met_office/station_observation_land"

    logger.info("Connecting to Spark...")
    spark = SparkSession.builder \
        .appName("MetOffice Land Observations landed to bronze") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    landed_pattern, version_id, output_path = get_latest_version_paths(
        LANDED_BASE_DIR,
        BRONZE_DIR
    )

    df = (
        spark.read
        .option("multiline", "true")
        .json(landed_pattern)
    )

    df_exploded = df.withColumn("observation", explode(col("data"))) \
        .select(
            col("station_geohash"),
            col("extracted_at"),
            col("observation.*")
        )

    incremental_keys = ["station_geohash", "datetime"]
    extraction_id = str(uuid.uuid4())

    df_bronze = df_exploded \
        .withColumn("_source_file", regexp_replace(input_file_name(), "%20", " ")) \
        .withColumn("_processed_at",
            date_format(
                from_utc_timestamp(current_timestamp(), "Europe/London"),
                "yyyy-MM-dd'T'HH:mm:ssXXX"
            )
        ) \
        .withColumn("_extraction_id", lit(extraction_id)) \
        .withColumn("_row_hash", sha2(concat_ws("||", *df_exploded.columns), 256))

    if gcs_prefix_exists(BRONZE_DIR):
        try:
            df_existing = spark.read.format("delta").load(BRONZE_DIR)
            df_bronze = df_bronze.join(df_existing, on=incremental_keys, how="left_anti")
        except Exception as e:
            logger.warning("Could not read existing bronze table for deduplication: %s", e)

    new_records_count = df_bronze.count()
    if new_records_count > 0:
        logger.info("Writing %d new records to: %s", new_records_count, output_path)
        df_bronze.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(BRONZE_DIR)
        logger.info("Moved to bronze. Version %s created.", version_id)
    else:
        logger.info("No new observations found. Skipping write.")

    spark.stop()


if __name__ == "__main__":
    main()
