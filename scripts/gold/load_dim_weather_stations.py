from pyspark.sql.functions import col, lit, current_timestamp
from upath import UPath as Path
from common.file_utils import start_spark_session

STATION_METADATA_SILVER_DIR = Path("/opt/airflow/silver/met_office/station_metadata")
GOLD_DIR = Path("/opt/airflow/gold/weather/dim_weather_stations")

def main():
    spark = start_spark_session("MetOffice Weather Stations Gold Dimension")
    
    df_updates = spark.read.format("delta").load(str(STATION_METADATA_SILVER_DIR))
    
    df_gold = df_updates.select(
        col("station_geohash").alias("StationKey"),
        col("station_name").alias("StationName"),
        col("latitude").alias("Latitude"),
        col("longitude").alias("Longitude"),
        col("county_name").alias("County"),
        col("country_name").alias("Country"),
        col("country_code").alias("CountryCode"),
        col("station_type").alias("StationType"),
        col("region_code").alias("RegionCode"),
        col("olson_time_zone").alias("TimeZone"),
        current_timestamp().alias("EffStartDate"),
        lit(None).cast("timestamp").alias("EffEndDate"),
        lit(True).alias("IsCurrent"),
        col("_row_hash").alias("RowHash"),
        lit("met_office").alias("SourceSystem")
    )

    try:
        spark.read.format("delta").load(str(GOLD_DIR))
        
        df_gold.createOrReplaceTempView("updates")
        
        spark.sql(f"""
            MERGE INTO delta.`{GOLD_DIR}` AS target
            USING updates
            ON target.StationKey = updates.StationKey
            WHEN MATCHED AND target.IsCurrent = true AND target.RowHash <> updates.RowHash THEN
              UPDATE SET IsCurrent = false, EffEndDate = current_timestamp()
        """)
        
        df_gold.write.format("delta").mode("append").save(str(GOLD_DIR))
        
    except Exception:
        # Initial Load if table doesn't exist
        print("Table not found. Performing initial load...")
        df_gold.write.format("delta").mode("overwrite").save(str(GOLD_DIR))

    print(f"DimWeatherStations written to {GOLD_DIR}. schema:")
    df_gold.printSchema()
    spark.stop()

if __name__ == "__main__":
    main()