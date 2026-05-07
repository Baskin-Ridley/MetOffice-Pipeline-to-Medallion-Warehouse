from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, regexp_replace, regexp_extract, lit, sha2, concat_ws
import uuid
import glob
import os

# Base directory
BRONZE_DIR = "/opt/airflow/bronze/met_office/station_metadata"
LANDED_BASE_DIR = "/opt/airflow/landed/met_office/station_metadata"

def main():
    print("connecting to spark...")
    spark = SparkSession.builder \
        .remote("sc://spark:15002") \
        .appName("MetOffice Metadata landed to bronze") \
        .getOrCreate()

    landed_folders = sorted(glob.glob(f"{LANDED_BASE_DIR}/*"))
    latest_folder = landed_folders[-1] 
    
    LANDED_DIR = f"{latest_folder}/*.json"

    version_id = os.path.basename(latest_folder)
    versioned_output_path = f"{BRONZE_DIR}/{version_id}"

    df = ( 
        spark.read
        .option("multiline", "true") \
        .json(LANDED_DIR)
    )

    extraction_id = str(uuid.uuid4())
    df_bronze = df \
        .withColumn("_source_file", regexp_replace(input_file_name(), "%20", " ")) \
        .withColumn("station_name", regexp_extract("_source_file", r"([^/]+)(?=\.json$)", 1)) \
        .withColumn("_processed_at", current_timestamp()) \
        .withColumn("_extraction_id", lit(extraction_id)) \
        .withColumn("_row_hash", sha2(concat_ws("||", *df.columns), 256))

    new_column_order = ["station_name"] + [col for col in df_bronze.columns if col != "station_name"]
    df_bronze = df_bronze.select(*new_column_order)

    print(f"Writing data to: {versioned_output_path}")
    #df_bronze.coalesce(1).write.mode("overwrite").parquet(versioned_output_path)
    df_bronze.write.format("delta") \
     .mode("append") \
     .option("mergeSchema", "true") \
     .save(BRONZE_DIR)
    print(f"Ingestion complete. Version {version_id} created.")
    df_bronze.printSchema()
    df_bronze.show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()