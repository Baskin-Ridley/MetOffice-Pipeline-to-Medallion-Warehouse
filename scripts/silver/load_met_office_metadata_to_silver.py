from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, regexp_extract, lit, sha2, concat_ws
from delta.tables import DeltaTable

BRONZE_DIR = "/opt/airflow/bronze/met_office/station_metadata"
SILVER_DIR = "/opt/airflow/silver/met_office/station_metadata"

def main():
    spark = SparkSession.builder \
        .remote("sc://spark:15002") \
        .appName("MetOffice Metadata bronze to silver") \
        .config("spark.sql.extensions.delta", "org.apache.spark.sql.delta.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # 1. Change to readStream to pick up only new data
    df = spark.readStream.format("delta").load(BRONZE_DIR)
    
    # 2. Use writeStream with a checkpoint and trigger
    query = df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{SILVER_DIR}/_checkpoints/metadata_job") \
        .trigger(availableNow=True) \
        .start(SILVER_DIR)
    
    query.awaitTermination()
    
    print("New data successfully incrementally written to Silver layer!")
    spark.stop()

if __name__ == "__main__":
    main()