from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, upper, current_timestamp, lit, sha2, concat_ws

BRONZE_DIR = "/opt/airflow/bronze/met_office/station_metadata"
SILVER_DIR = "/opt/airflow/silver/met_office/station_metadata"

# station_name       |area          |country|geohash|olson_time_zone|region|_source_file                                                                                   |_processed_at          |_extraction_id                      |_row_hash                                                       |
# -------------------+--------------+-------+-------+---------------+------+-----------------------------------------------------------------------------------------------+-----------------------+------------------------------------+----------------------------------------------------------------+
# Great Dun Fell No 2|Cumbria       |England|gcwr04 |Europe/London  |nw    |file:///opt/airflow/landed/met_office/station_metadata/20260504_135700/Great Dun Fell No 2.json|2026-05-04 13:57:09.257|a9f89f5c-ee4c-45b4-9f62-4e32fd1894bd|c15b37d359b5b256ba50fe7744a8cc22e8c65dbb4a9c20c5f20b41cc4701dd25|
# Cambridge, Niab    |Bedford       |England|gcr9j7 |Europe/London  |ee    |file:///opt/airflow/landed/met_office/station_metadata/20260504_135700/Cambridge, Niab.json    |2026-05-04 13:57:09.257|a9f89f5c-ee4c-45b4-9f62-4e32fd1894bd|0159c758f67ca3248af6792bf06cbbe3599beb328ee4503980f4f774f6d9578c|
# Heathrow           |Greater London|England|gcpsvg |Europe/London  |se    |file:///opt/airflow/landed/met_office/station_metadata/20260504_135700/Heathrow.json           |2026-05-04 13:57:09.257|a9f89f5c-ee4c-45b4-9f62-4e32fd1894bd|48ab8df3704a0de72c79b311883dca62210dfbe159cf2262e0818a60a1aecd1a|

def transform_to_silver(df):
    df_silver = df.select(
        trim(col("station_name")).alias("station_name"),
        trim(col("geohash")).alias("station_geohash"),
        trim(col("area")).alias("county_name"),
        trim(col("country")).alias("country_name"),
        trim(col("olson_time_zone")).alias("olson_time_zone"),
        upper(trim(col("region"))).alias("region_code"),
        # audit columns
        current_timestamp().alias("_processed_at"),
        sha2(concat_ws("||", col("station_name"), col("geohash"), col("area"), col("country"), col("olson_time_zone"), col("region")), 256).alias("_row_hash"),
        lit("met_office").alias("_source_system")
    )
    print("Transformation to Silver layer complete. Schema:")
    df_silver.printSchema()
    return df_silver





def main():
    print("Connecting to Spark...")
    spark = SparkSession.builder \
        .remote("sc://spark:15002") \
        .appName("MetOffice Metadata bronze to silver") \
        .config("spark.sql.extensions.delta", "org.apache.spark.sql.delta.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    print("Connected to Spark")

    df = spark.readStream.format("delta").load(BRONZE_DIR)
    
    df_silver = transform_to_silver(df)
    query = df_silver.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/opt/airflow/silver/met_office/station_metadata/_checkpoints") \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .start(SILVER_DIR)

    print("New data successfully written to Silver layer!")
    spark.stop()

if __name__ == "__main__":
    main()