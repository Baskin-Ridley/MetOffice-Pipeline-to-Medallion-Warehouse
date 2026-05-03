from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, regexp_extract, lit, sha2, concat_ws
from datetime import datetime
import uuid

# Base directory
BRONZE_DIR = "/opt/airflow/bronze/met_office/station_metadata"
LANDED_DIR = "/opt/airflow/landed/met_office/station_metadata/*/*.json"

def main():
    print("connecting to spark...")
    spark = SparkSession.builder \
        .remote("sc://spark:15002") \
        .appName("MetOffice Metadata landed to bronze") \
        .getOrCreate()

    version_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    versioned_output_path = f"{BRONZE_DIR}/{version_id}"

    df = spark.read.json(LANDED_DIR)

    extraction_id = str(uuid.uuid4())
    df_bronze = df \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("station_name", regexp_extract("_source_file", r"([^/]+)(?=\.json$)", 1)) \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_extraction_id", lit(extraction_id)) \
        .withColumn("_row_hash", sha2(concat_ws("||", *df.columns), 256)) # won't matter on this small dataset, but good practice to unlock efficiences later layers

    print(f"Writing data to: {versioned_output_path}")
    
    df_bronze.write.mode("overwrite").parquet(versioned_output_path)
    
    print(f"Ingestion complete. Version {version_id} created.")

if __name__ == "__main__":
    main()