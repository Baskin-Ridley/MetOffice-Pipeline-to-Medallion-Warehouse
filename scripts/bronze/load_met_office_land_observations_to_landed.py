from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, regexp_replace, regexp_extract, lit, sha2, concat_ws
import uuid
from pathlib import Path

# Base directory
BRONZE_DIR = "/opt/airflow/bronze/met_office/station_observation_land"
LANDED_BASE_DIR = "/opt/airflow/landed/met_office/station_observation_land"



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
    

    
    print("scafolding")

if __name__ == "__main__":
    main()