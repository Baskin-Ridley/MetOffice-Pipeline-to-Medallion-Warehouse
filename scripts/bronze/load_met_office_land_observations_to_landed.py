from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, regexp_replace, regexp_extract, lit, sha2, concat_ws
import uuid
import glob
import os

# Base directory
BRONZE_DIR = "/opt/airflow/bronze/met_office/station_observation_land"
LANDED_BASE_DIR = "/opt/airflow/landed/met_office/station_observation_land"



def main():
    #schema
    # array
# Multiline description
# [{
# datetime: string

# Date of the observation.
# humidity: integer┃null

# Probability as a percentage of 100.
# mslp: integer┃null

# Mean surface level pressure in hPA.
# pressure_tendency: string┃null

# Pressure tendency representing Rising, Falling or Steady.
# temperature: number┃null

# Air temperature in °C.
# visibility: integer┃null

# Visibility in metres.
# weather_code: integer┃null

# Numerical code for the weather symbol.
# wind_direction: string┃null

# Direction the wind is travelling from in 16 point compass notation.
# wind_gust: number┃null

# Wind gust speed in m/s.
# wind_speed: number┃null

# Wind speed in m/s.
# }] 
    print("scafolding")

if __name__ == "__main__":
    main()