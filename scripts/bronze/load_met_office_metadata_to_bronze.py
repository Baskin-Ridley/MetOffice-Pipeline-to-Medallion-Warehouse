from pyspark.sql import SparkSession
from pyspark.sql.functions import from_utc_timestamp, date_format, current_timestamp, input_file_name, regexp_replace, regexp_extract, lit, sha2, concat_ws
import uuid
from upath import UPath as Path
from common.file_utils import get_latest_version_paths
import os

# Base directory
BRONZE_DIR = Path("/opt/airflow/bronze/met_office/station_metadata")
LANDED_BASE_DIR = Path("/opt/airflow/landed/met_office/station_metadata")

def main():
    print("connecting to spark...")
    spark = SparkSession.builder \
        .remote("sc://spark:15002") \
        .appName("MetOffice Metadata landed to bronze") \
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

    if os.path.exists(BRONZE_DIR) and len(os.listdir(BRONZE_DIR)) > 0:
        print("Checking for existing records in Bronze...")
        df_existing = spark.read.format("delta").load(str(BRONZE_DIR))
        df_bronze = df_bronze.join(df_existing, on=business_keys, how="left_anti")

    new_column_order = ["station_name"] + [col for col in df_bronze.columns if col != "station_name"]
    df_bronze = df_bronze.select(*new_column_order)

    if df_bronze.count() > 0:
        print(f"Writing {df_bronze.count()} new records to: {output_path}")
        df_bronze.write.format("delta") \
         .mode("append") \
         .option("mergeSchema", "true") \
         .save(str(BRONZE_DIR))
        print(f"Moved to bronze. Version {version_id} created.")
    else:
        print("No new data combinations found. Skipping write.")
    
    df_bronze.printSchema()
    df_bronze.show(10, truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()