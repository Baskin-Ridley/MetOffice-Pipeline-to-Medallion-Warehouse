from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, sequence, to_date, col, date_format, 
    year, month, dayofmonth, quarter, when, lit
)
from pathlib import Path
from common.file_utils import start_spark_session

GOLD_DIM_DIR = Path("/opt/airflow/gold/master/dim_date")

def generate_dim_date(spark, start_date="2020-01-01", end_date="2031-12-31"):
    """
    Manufactures a comprehensive Date Dimension.
    """
    # Generate a range of dates
    df = spark.createDataFrame([(start_date, end_date)], ["start", "end"])
    
    dim_date = df.select(
        explode(sequence(to_date(col("start")), to_date(col("end")))).alias("FullDate")
    )

    df_transformed = dim_date.select(
        date_format(col("FullDate"), "yyyyMMdd").cast("int").alias("DateKey"),
        col("FullDate"),
        year(col("FullDate")).alias("Year"),
        month(col("FullDate")).alias("Month"),
        dayofmonth(col("FullDate")).alias("Day"),
        date_format(col("FullDate"), "MMMM").alias("MonthName"),
        date_format(col("FullDate"), "EEEE").alias("DayName"),
        quarter(col("FullDate")).alias("Quarter"),
        when(date_format(col("FullDate"), "E").isin("Sat", "Sun"), True)
            .otherwise(False).alias("IsWeekend"),
        when(month(col("FullDate")).isin(12, 1, 2), "Winter")
            .when(month(col("FullDate")).isin(3, 4, 5), "Spring")
            .when(month(col("FullDate")).isin(6, 7, 8), "Summer")
            .otherwise("Autumn").alias("Season"),
        lit("met_office_calendar").alias("SourceSystem")
    )

    return df_transformed

def main():
    spark = start_spark_session("Generate Gold DimDate")

    print(f"Generating Date Dimension...")
    df_dim_date = generate_dim_date(spark)


    print(f"Writing DimDate to: {GOLD_DIM_DIR}")
    df_dim_date.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(str(GOLD_DIM_DIR))

    spark.sql(f"CREATE TABLE IF NOT EXISTS DimDate USING DELTA LOCATION '{GOLD_DIM_DIR}'")

    print("DimDate successfully initialized. Sample output:")
    df_dim_date.show(5)
    
    spark.stop()

if __name__ == "__main__":
    main()