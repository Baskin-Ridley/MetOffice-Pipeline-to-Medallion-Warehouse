from pyspark.sql import SparkSession
import uuid

def test_remote_delta():
    print("--- Starting Spark Connect (Remote) Verification ---")
    
    # 1. Connect to the remote server
    # Ensure your Spark Server is started with the Delta configs we discussed
    spark = SparkSession.builder \
        .remote("sc://spark:15002") \
        .getOrCreate()

    try:
        # 2. Use a local path mapped in your docker-compose volumes
        # The Spark container sees the volume at /opt/airflow/bronze
        test_id = str(uuid.uuid4())[:8]
        path = f"/opt/airflow/bronze/test_delta_{test_id}"
        
        print(f"Target Path (Remote): {path}")
        print("Writing data via Spark Connect...")
        
        # Create sample data
        df = spark.createDataFrame([("Alice", 1), ("Bob", 2)], ["name", "id"])
        
        # Write as Delta
        df.write.format("delta").mode("overwrite").save(path)
        
        print("Reading data back from Delta...")
        # Read back to verify
        result_df = spark.read.format("delta").load(path)
        result_df.show()
        
        print("✅ SUCCESS: Remote Spark Connect & Delta are working locally.")

    except Exception as e:
        print(f"❌ ERROR: {e}")
        print("\nTIP: If you still see 'Data source not found: delta', ensure")
        print("your Spark container was started with the --packages flag.")
    finally:
        spark.stop()

if __name__ == "__main__":
    test_remote_delta()