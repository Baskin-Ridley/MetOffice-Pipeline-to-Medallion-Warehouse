from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def main():
    print("🚀 Connecting to Remote PySpark Engine...")
    
    # 1. Connect to the separate Spark container over the Docker network!
    # "sc://" means Spark Connect. "spark" is the container name in docker-compose.
    spark = SparkSession.builder \
        .remote("sc://spark:15002") \
        .getOrCreate()
    
    print("✅ Successfully connected to Spark Container!")
    print(f"🔹 Spark Version: {spark.version}\n")

    # 2. Create some dummy weather data
    data = [
        ("gcpsvg", "Greater London", "Heathrow", 51.47, -0.45),
        ("gcpv8m", "Greater London", "Gatwick", 51.15, -0.18)
    ]
    columns = ["geohash", "area", "station_name", "latitude", "longitude"]

    print("📊 Creating DataFrame...")
    df = spark.createDataFrame(data, schema=columns)
    
    # 3. Simulate a Bronze layer transformation (adding a load timestamp)
    df_bronze = df.withColumn("_bronze_loaded_at", current_timestamp())

    # 4. Prove it works by printing the Schema and the Data
    print("🔍 Schema:")
    df_bronze.printSchema()

    print("📈 Data:")
    df_bronze.show(truncate=False)

    print("🎉 Spark Connect is fully operational!")
    
    spark.stop()

if __name__ == "__main__":
    main()