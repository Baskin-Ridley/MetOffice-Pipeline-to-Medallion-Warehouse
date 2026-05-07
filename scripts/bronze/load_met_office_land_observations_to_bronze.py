from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, from_utc_timestamp, input_file_name, regexp_replace, regexp_extract, lit, sha2, concat_ws, explode, col
import uuid
from pathlib import Path
from common.file_utils import get_latest_version_paths

# Base directory
BRONZE_DIR = Path("/opt/airflow/bronze/met_office/station_observation_land")
LANDED_BASE_DIR = Path("/opt/airflow/landed/met_office/station_observation_land")

def main():
#schema
# {
#   "type": "array",
#   "items": {
#     "type": "object",
#     "properties": {
#       "station_geohash": { "type": "string" },
#       "extracted_at": { "type": "string" },
#       "data": {
#         "type": "array",
#         "items": {
#           "type": "object",
#           "properties": {
#             "datetime": { "type": "string", "format": "date-time" },
#             "visibility": { "type": ["integer", "null"] },
#             "temperature": { "type": ["number", "null"] },
#             "mslp": { "type": ["integer", "null"] },
#             "wind_gust": { "type": ["number", "null"] },
#             "wind_direction": { "type": ["string", "null"] },
#             "wind_speed": { "type": ["number", "null"] },
#             "humidity": { "type": ["integer", "null"] },
#             "weather_code": { "type": ["integer", "null"] },
#             "pressure_tendency": { "type": ["string", "null"] }
#           }
#         }
#       }
#     }
#   }
# }

    print("connecting to spark...")
    spark = SparkSession.builder \
        .remote("sc://spark:15002") \
        .appName("MetOffice Land Observations landed to bronze") \
        .getOrCreate()
    
    landed_pattern, version_id, output_path = get_latest_version_paths(
            LANDED_BASE_DIR, 
            BRONZE_DIR
        )
    
    df = (
        spark.read
        .option("multiline", "true")
        .json(str(landed_pattern))
    )


    df_exploded = df.withColumn("observation", explode(col("data"))) \
        .select(
            col("station_geohash"),
            col("extracted_at"),
            col("observation.*")
        )

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