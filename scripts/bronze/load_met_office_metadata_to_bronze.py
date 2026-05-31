import sys
import uuid
from urllib.parse import urlparse
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_utc_timestamp, date_format, current_timestamp, 
    input_file_name, regexp_replace, regexp_extract, lit, sha2, concat_ws
)
from file_utils import get_latest_version_paths

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

    BRONZE_DIR = f"{DATALAKE_ROOT}/bronze/met_office/station_metadata"
    LANDED_BASE_DIR = f"{DATALAKE_ROOT}/landed/met_office/station_metadata"

    print("connecting to spark...")
    spark = SparkSession.builder \
        .appName("MetOffice Metadata landed to bronze") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    landed_pattern, version_id, output_path = get_latest_version_paths(
        LANDED_BASE_DIR, 
        BRONZE_DIR
    )

    df = ( 
        spark.read
        .option("multiline", "true") \
        .json(str(landed_pattern))
    )

    extraction_id = str(uuid.uuid4())
    business_keys = ["station_name", "area", "country", "geohash", "olson_time_zone", "region"]

    df_bronze = df \
        .withColumn("_source_file", regexp_replace(input_file_name(), "%20", " ")) \
        .withColumn("station_name", regexp_extract("_source_file", r"([^/]+)(?=\.json$)", 1)) \
        .withColumn("_processed_at", 
            date_format(
                from_utc_timestamp(current_timestamp(), "Europe/London"), 
                "yyyy-MM-dd'T'HH:mm:ssXXX"
            )
        ) \
        .withColumn("_extraction_id", lit(extraction_id)) \
        .withColumn("_row_hash", sha2(concat_ws("||", *df.columns), 256))

    if gcs_prefix_exists(BRONZE_DIR):
        try:
            df_existing = spark.read.format("delta").load(BRONZE_DIR)
            df_bronze = df_bronze.join(df_existing, on=business_keys, how="left_anti")
        except Exception:
            pass

    new_column_order = ["station_name"] + [col for col in df_bronze.columns if col != "station_name"]
    df_bronze = df_bronze.select(*new_column_order)

    if df_bronze.count() > 0:
        print(f"Writing {df_bronze.count()} new records to: {output_path}")
        df_bronze.write.format("delta") \
         .mode("append") \
         .option("mergeSchema", "true") \
         .save(BRONZE_DIR)
        print(f"Moved to bronze. Version {version_id} created.")
    else:
        print("No new data combinations found. Skipping write.")
    
    df_bronze.printSchema()
    df_bronze.show(10, truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()