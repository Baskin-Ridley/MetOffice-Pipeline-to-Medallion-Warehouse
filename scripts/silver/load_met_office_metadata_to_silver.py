import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, upper, current_timestamp, lit, sha2, concat_ws

logger = logging.getLogger(__name__)


def transform_to_silver(df):
    return df.select(
        trim(col("station_name")).alias("station_name"),
        trim(col("geohash")).alias("station_geohash"),
        col("latitude").cast("double"),
        col("longitude").cast("double"),
        trim(col("area")).alias("county_name"),
        trim(col("country")).alias("country_name"),
        trim(col("country_code")).alias("country_code"),
        trim(col("station_type")).alias("station_type"),
        trim(col("olson_time_zone")).alias("olson_time_zone"),
        upper(trim(col("region"))).alias("region_code"),
        current_timestamp().alias("_processed_at"),
        sha2(concat_ws("||", col("station_name"), col("geohash"), col("area"), col("country"), col("olson_time_zone"), col("region")), 256).alias("_row_hash"),
        lit("met_office").alias("_source_system")
    )


def main():
    if len(sys.argv) < 2:
        raise ValueError("Missing required datalake bucket argument.")

    BUCKET_NAME = sys.argv[1]
    DATALAKE_ROOT = f"gs://{BUCKET_NAME}"
    BRONZE_DIR = f"{DATALAKE_ROOT}/bronze/met_office/station_metadata"
    SILVER_DIR = f"{DATALAKE_ROOT}/silver/met_office/station_metadata"

    logger.info("Connecting to Spark...")
    spark = SparkSession.builder \
        .appName("MetOffice Metadata bronze to silver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    logger.info("Connected to Spark")

    df = spark.readStream.format("delta").load(BRONZE_DIR)

    df_silver = transform_to_silver(df)
    query = df_silver.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{SILVER_DIR}/_checkpoints") \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .start(SILVER_DIR)
    query.awaitTermination()
    logger.info("New data successfully written to Silver layer.")
    spark.stop()


if __name__ == "__main__":
    main()
