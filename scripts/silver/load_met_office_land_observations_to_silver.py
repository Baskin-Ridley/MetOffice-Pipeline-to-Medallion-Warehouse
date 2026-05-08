from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, upper, current_timestamp, lit, sha2, concat_ws
from pathlib import Path
from common.file_utils import start_spark_session

BRONZE_DIR = Path("/opt/airflow/bronze/met_office/station_observation_land")
SILVER_DIR = Path("/opt/airflow/silver/met_office/station_observation_land")

def transform_to_silver(df):
    print("scaffolding")
    return df

def main():
    spark = start_spark_session("MetOffice Land Observations bronze to silver")
    spark.stop()


    

if __name__ == "__main__":
    main()