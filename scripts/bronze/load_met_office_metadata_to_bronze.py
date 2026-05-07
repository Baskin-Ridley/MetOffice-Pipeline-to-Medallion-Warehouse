from pyspark.sql import SparkSession
from pyspark.sql.functions import from_utc_timestamp, date_format, current_timestamp, input_file_name, regexp_replace, regexp_extract, lit, sha2, concat_ws
import uuid
from pathlib import Path
from common.file_utils import get_latest_version_paths

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

    new_column_order = ["station_name"] + [col for col in df_bronze.columns if col != "station_name"]
    df_bronze = df_bronze.select(*new_column_order)

    print(f"Writing data to: {output_path}")
    df_bronze.write.format("delta") \
     .mode("append") \
     .option("mergeSchema", "true") \
     .save(str(BRONZE_DIR))
     
    print(f"Ingestion complete. Version {version_id} created.")
    df_bronze.printSchema()
    df_bronze.show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()