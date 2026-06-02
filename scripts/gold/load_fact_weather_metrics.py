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

    df_unpivoted = df.select(
        trim(col("station_geohash").cast("string")).alias("StationKey"),
        date_format(col("observation_datetime"), "yyyyMMdd").cast("int").alias("DateKey"),
        col("observation_datetime").cast("timestamp").cast("string").substr(12, 8).alias("ObservationTime"),
        expr(unpivot_expr),
    )

    return df_unpivoted.select(
        col("StationKey"),
        col("DateKey"),
        col("ObservationTime"),
        col("MetricName"),
        col("Unit"),
        expr("try_cast(RawValue as double)").alias("ValueNumeric"),
        when(expr("try_cast(RawValue as double)").isNull(), col("RawValue")).alias("ValueString"),
        current_timestamp().alias("ProcessedAt"),
        sha2(concat_ws("||", col("StationKey"), col("DateKey"), col("MetricName")), 256).alias("RowHash"),
        lit("met_office").alias("SourceSystem")
    )


def main():
    if len(sys.argv) < 4:
        raise ValueError("Usage: load_fact_weather_metrics.py <bucket> <project_id> <dataset_id>")

    BUCKET_NAME = sys.argv[1]
    PROJECT_ID = sys.argv[2]
    DATASET_ID = sys.argv[3]
    DATALAKE_ROOT = f"gs://{BUCKET_NAME}"
    STATION_OBSERVATIONS_LAND_SILVER_DIR = f"{DATALAKE_ROOT}/silver/met_office/station_observation_land"
    GOLD_DIR = f"{DATALAKE_ROOT}/gold/weather/weather_metrics"
    BQ_TABLE = f"{PROJECT_ID}:{DATASET_ID}.FactWeatherMetrics"

    spark = start_spark_session("MetOffice Weather Metrics silver to gold")

    df = spark.readStream.format("delta").load(STATION_OBSERVATIONS_LAND_SILVER_DIR)
    df_gold = transform_to_gold(df)

    def write_batch(batch_df, _epoch_id):
        batch_df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(GOLD_DIR)
        batch_df.write \
            .format("bigquery") \
            .option("table", BQ_TABLE) \
            .option("temporaryGcsBucket", BUCKET_NAME) \
            .mode("append") \
            .save()

    query = df_gold.writeStream \
        .foreachBatch(write_batch) \
        .option("checkpointLocation", f"{GOLD_DIR}/_checkpoints") \
        .trigger(availableNow=True) \
        .start()

    query.awaitTermination()
    logger.info("New data successfully written to Delta and BigQuery.")
    spark.stop()


if __name__ == "__main__":
    main()
