import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, current_timestamp, lit, sha2, concat_ws


def transform_to_silver(df):
    df_silver = df.select(
        trim(col("station_geohash")).alias("station_geohash"),
        col("datetime").alias("observation_datetime"),
        col("visibility").alias("visibility_m"),
        col("temperature").alias("temperature_c"),
        col("mslp").alias("mean_sea_level_pressure_hpa"),
        col("wind_gust").alias("wind_gust_ms"),
        col("wind_direction").alias("wind_direction"),
        col("wind_speed").alias("wind_speed_ms"),
        col("humidity").alias("humidity_percentage"),
        col("weather_code").alias("weather_code"),
        col("pressure_tendency").alias("pressure_tendency"),
        current_timestamp().alias("_processed_at"),
        sha2(concat_ws("||", col("station_geohash"), col("datetime"), col("visibility"), col("temperature"), col("mslp"), col("wind_gust"), col("wind_direction"), col("wind_speed"), col("humidity"), col("weather_code"), col("pressure_tendency")), 256).alias("_row_hash"),
        lit("met_office").alias("_source_system")
    )
    print("Transformation to Silver layer complete. Schema:")
    df_silver.printSchema()
    return df_silver


def main():
    if len(sys.argv) < 2:
        raise ValueError("Missing required datalake bucket argument.")

    BUCKET_NAME = sys.argv[1]
    DATALAKE_ROOT = f"gs://{BUCKET_NAME}"
    BRONZE_DIR = f"{DATALAKE_ROOT}/bronze/met_office/station_observation_land"
    SILVER_DIR = f"{DATALAKE_ROOT}/silver/met_office/station_observation_land"

    print("Connecting to Spark...")
    spark = SparkSession.builder \
        .appName("MetOffice Land Observations bronze to silver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    print("Connected to Spark")

    df = spark.readStream.format("delta").load(BRONZE_DIR)

    df_silver = transform_to_silver(df)
    query = df_silver.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{SILVER_DIR}/_checkpoints") \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .start(SILVER_DIR)
    query.awaitTermination()
    print("New data successfully written to Silver layer!")
    spark.stop()


if __name__ == "__main__":
    main()
