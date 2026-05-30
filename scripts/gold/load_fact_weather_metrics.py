import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, trim, col, upper, current_timestamp, lit, sha2, concat_ws, when, date_format 
from upath import UPath
from common.file_utils import start_spark_session

# Base directories
BUCKET_NAME = os.getenv("DATALAKE_BUCKET", "your-gcp-datalake-bucket")
DATALAKE_ROOT = UPath(f"gs://{BUCKET_NAME}")
STATION_OBSERVATIONS_LAND_SILVER_DIR = DATALAKE_ROOT / "silver/met_office/station_observation_land"
GOLD_DIR = DATALAKE_ROOT / "gold/weather/weather_metrics"

def transform_to_gold(df):
    descriptions = {
        "StationKey": "Geohash or unique identifier for the weather station.",
        "DateKey": "The date of observation in YYYYMMDD format.",
        "ObservationTime": "The time of the weather reading.",
        "MetricName": "The specific weather variable being measured.",
        "ValueNumeric": "The decimal value of the metric (if applicable).",
        "ValueString": "The string/categorical value of the metric (e.g., Wind Direction).",
        "Unit": "Standardized unit of measurement.",
        "ProcessedAt": "Audit timestamp showing when data reached the Gold layer.",
        "RowHash": "Deterministic hash for row-level idempotency and uniqueness.",
        "SourceSystem": "The originating system (e.g., met_office)."
    }
    
    unpivot_expr = """
        stack(8, 
            'Visibility', cast(visibility_m as string), 'm',
            'Temperature', cast(temperature_c as string), 'C',
            'Pressure', cast(mean_sea_level_pressure_hpa as string), 'hPa',
            'Wind Gust', cast(wind_gust_ms as string), 'm/s',
            'Wind Direction', cast(wind_direction as string), 'text',
            'Wind Speed', cast(wind_speed_ms as string), 'm/s',
            'Humidity', cast(humidity_percentage as string), '%',
            'Weather Code', cast(weather_code as string), 'code'
        ) as (MetricName, RawValue, Unit)
    """
    
    df_gold = df.select(
        trim(col("station_geohash").cast("string")).alias("StationKey"),
        date_format(col("observation_datetime"), "yyyyMMdd").cast("int").alias("DateKey"),
        col("observation_datetime").cast("timestamp").cast("string").substr(12, 8).alias("ObservationTime"),
        expr(unpivot_expr),
        expr("try_cast(RawValue as double)").alias("ValueNumeric"),
        when(expr("try_cast(RawValue as double)").isNull(), col("RawValue")).alias("ValueString"),
        # Audit columns
        current_timestamp().alias("ProcessedAt"),
        sha2(concat_ws("||", col("station_geohash"), col("observation_datetime"), col("MetricName")), 256).alias("RowHash"),
        lit("met_office").alias("SourceSystem")
    ).drop("RawValue")
    
    print("Transformation to Gold layer complete. Schema:")
    df_gold.printSchema()
    return df_gold

def main():
    spark = start_spark_session("MetOffice Weather Metrics silver to gold")
    df = spark.readStream.format("delta").load(str(STATION_OBSERVATIONS_LAND_SILVER_DIR))
    df_gold = transform_to_gold(df)
    query = df_gold.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", str(GOLD_DIR / "_checkpoints")) \
        .option("path", str(GOLD_DIR)) \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .toTable("FactWeatherMetrics") 
    
    query.awaitTermination()
    print("New data successfully written to Gold layer!")
    spark.stop()

if __name__ == "__main__":
    main()