import logging
import sys
from pyspark.sql.functions import expr, trim, col, current_timestamp, lit, sha2, concat_ws, when, date_format
from file_utils import start_spark_session

logger = logging.getLogger(__name__)


def transform_to_gold(df):
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

    return df.select(
        trim(col("station_geohash").cast("string")).alias("StationKey"),
        date_format(col("observation_datetime"), "yyyyMMdd").cast("int").alias("DateKey"),
        col("observation_datetime").cast("timestamp").cast("string").substr(12, 8).alias("ObservationTime"),
        expr(unpivot_expr),
        expr("try_cast(RawValue as double)").alias("ValueNumeric"),
        when(expr("try_cast(RawValue as double)").isNull(), col("RawValue")).alias("ValueString"),
        current_timestamp().alias("ProcessedAt"),
        sha2(concat_ws("||", col("station_geohash"), col("observation_datetime"), col("MetricName")), 256).alias("RowHash"),
        lit("met_office").alias("SourceSystem")
    ).drop("RawValue")


def main():
    if len(sys.argv) < 2:
        raise ValueError("Missing required datalake bucket argument.")

    BUCKET_NAME = sys.argv[1]
    DATALAKE_ROOT = f"gs://{BUCKET_NAME}"
    STATION_OBSERVATIONS_LAND_SILVER_DIR = f"{DATALAKE_ROOT}/silver/met_office/station_observation_land"
    GOLD_DIR = f"{DATALAKE_ROOT}/gold/weather/weather_metrics"

    spark = start_spark_session("MetOffice Weather Metrics silver to gold")

    df = spark.readStream.format("delta").load(STATION_OBSERVATIONS_LAND_SILVER_DIR)
    df_gold = transform_to_gold(df)

    query = df_gold.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{GOLD_DIR}/_checkpoints") \
        .option("path", GOLD_DIR) \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .toTable("FactWeatherMetrics")

    query.awaitTermination()
    logger.info("New data successfully written to Gold layer.")
    spark.stop()


if __name__ == "__main__":
    main()
