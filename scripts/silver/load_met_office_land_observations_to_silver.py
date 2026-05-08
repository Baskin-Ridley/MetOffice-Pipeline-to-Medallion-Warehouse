from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, upper, current_timestamp, lit, sha2, concat_ws
from pathlib import Path

BRONZE_DIR = Path("/opt/airflow/bronze/met_office/station_observation_land")
SILVER_DIR = Path("/opt/airflow/silver/met_office/station_observation_land")

def transform_to_silver(df):
    print("scaffolding")
    return df

def main():
    spark = SparkSession.builder \
        .remote("sc://spark:15002") \
        .appName("MetOffice Land Observations bronze to silver") \
        .config("spark.sql.extensions.delta", "org.apache.spark.sql.delta.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.stop()

if __name__ == "__main__":
    main()