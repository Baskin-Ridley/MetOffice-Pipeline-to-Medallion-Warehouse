import logging
import sys
from pyspark.sql.functions import (
    explode, sequence, to_date, col, date_format,
    year, month, dayofmonth, quarter, when, lit
)
from file_utils import start_spark_session

logger = logging.getLogger(__name__)


def generate_dim_date(spark, start_date: str, end_date: str):
    df = spark.createDataFrame([(start_date, end_date)], ["start", "end"])

    dim_date = df.select(
        explode(sequence(to_date(col("start")), to_date(col("end")))).alias("FullDate")
    )

    return dim_date.select(
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


def main():
    if len(sys.argv) < 2:
        raise ValueError("Missing required datalake bucket argument.")

    BUCKET_NAME = sys.argv[1]
    start_date = sys.argv[2] if len(sys.argv) > 2 else "2020-01-01"
    end_date = sys.argv[3] if len(sys.argv) > 3 else "2031-12-31"
    GOLD_DIM_DIR = f"gs://{BUCKET_NAME}/gold/master/dim_date"

    spark = start_spark_session("Generate Gold DimDate")

    logger.info("Generating Date Dimension from %s to %s...", start_date, end_date)
    df_dim_date = generate_dim_date(spark, start_date, end_date)

    logger.info("Writing DimDate to: %s", GOLD_DIM_DIR)
    df_dim_date.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(GOLD_DIM_DIR)

    spark.sql(f"CREATE TABLE IF NOT EXISTS DimDate USING DELTA LOCATION '{GOLD_DIM_DIR}'")

    logger.info("DimDate successfully written to %s.", GOLD_DIM_DIR)
    spark.stop()


if __name__ == "__main__":
    main()
