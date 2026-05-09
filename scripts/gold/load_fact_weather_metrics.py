from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, trim, col, upper, current_timestamp, lit, sha2, concat_ws, when
from pathlib import Path
from common.file_utils import start_spark_session

# Base directories
STATION_OBSERVATIONS_LAND_SILVER_DIR = Path("/opt/airflow/silver/met_office/station_observation_land") #in the gold layer you often take data from multiple sources so using a different name to be extensible.
GOLD_DIR = Path("/opt/airflow/gold/met_office/weather_metrics")

def transform_to_gold(df):
    descriptions = {
        "StationKey": "Geohash or unique identifier for the weather station.",
        "ObservationDateTime": "The UTC timestamp of the weather reading.",
        "MetricName": "The specific weather variable being measured.",
        "ValueNumeric": "The decimal value of the metric (if applicable).",
        "ValueString": "The string/categorical value of the metric (e.g., Wind Direction).",
        "Unit": "Standardized unit of measurement.",
        "ProcessedAt": "Audit timestamp showing when data reached the Gold layer.",
        "RowHash": "Deterministic hash for row-level idempotency and uniqueness.",
        "SourceSystem": "The originating system (e.g., met_office)."
    } #implementing a dictionary only on gold, in previous roles I would also add this on silver, but as this is a hobbiest project I want to limit scope.
    unpivot_expr = """
        stack(8, 
            'Visibility', cast(visibility_m as string), 'm',
            'Temperature', cast(temperature_c as string), 'C',
            'Pressure', cast(mean_sea_level_pressure_hpa as string), 'hPa',
            'WindGust', cast(wind_gust_ms as string), 'm/s',
            'WindDirection', cast(wind_direction as string), 'text',
            'WindSpeed', cast(wind_speed_ms as string), 'm/s',
            'Humidity', cast(humidity_percentage as string), '%',
            'WeatherCode', cast(weather_code as string), 'code'
        ) as (MetricName, RawValue, Unit)
    """
    df_gold = df.select(
        trim(col("station_geohash").cast("string")).alias("StationKey"),
        col("observation_datetime").cast("timestamp").alias("ObservationDateTime"),
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
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .start(str(GOLD_DIR))
    print("Gold layer schema:")
    df_gold.printSchema()
    query.awaitTermination()
    print("New data successfully written to Gold layer!")
    spark.stop()

if __name__ == "__main__":
    main()
