from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, upper, current_timestamp, lit, sha2, concat_ws
from pathlib import Path
from common.file_utils import start_spark_session

BRONZE_DIR = Path("/opt/airflow/bronze/met_office/station_observation_land")
SILVER_DIR = Path("/opt/airflow/silver/met_office/station_observation_land")

def transform_to_silver(df):
    df_silver = df.select(
        trim(col("station_geohash")).alias("station_geohash"),
        col("observation.datetime").alias("observation_datetime"),
        col("observation.visibility").alias("visibility_m"),
        col("observation.temperature").alias("temperature_c"),
        col("observation.mslp").alias("mean_sea_level_pressure_hpa"),
        col("observation.wind_gust").alias("wind_gust_ms"),
        col("observation.wind_direction").alias("wind_direction"),
        col("observation.wind_speed").alias("wind_speed_ms"),
        col("observation.humidity").alias("humidity"),
        col("observation.weather_code").alias("weather_code"),
        col("observation.pressure_tendency").alias("pressure_tendency"),
        # audit columns
        current_timestamp().alias("_processed_at"),
        sha2(concat_ws("||", col("station_geohash"), col("observation.datetime"), col("observation.visibility"), col("observation.temperature"), col("observation.mslp"), col("observation.wind_gust"), col("observation.wind_direction"), col("observation.wind_speed"), col("observation.humidity"), col("observation.weather_code"), col("observation.pressure_tendency")), 256).alias("_row_hash"),
        lit("met_office").alias("_source_system")
    )
    print("Transformation to Silver layer complete. Schema:")
    df_silver.printSchema()
    return df_silver

def main():
    spark = start_spark_session("MetOffice Land Observations bronze to silver")
    df = spark.readStream.format("delta").load(str(BRONZE_DIR))
    df_silver = transform_to_silver(df)
    query = df_silver.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", str(SILVER_DIR / "_checkpoints")) \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .start(str(SILVER_DIR))
    print("New data successfully written to Silver layer!")
    spark.stop()


    

if __name__ == "__main__":
    main()