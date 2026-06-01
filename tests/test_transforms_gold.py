"""
Tests for the gold-layer transform functions: DimDate and FactWeatherMetrics.
"""
import pytest
from pyspark.sql import Row


# ---------------------------------------------------------------------------
# DimDate
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def generate_dim_date():
    from load_dim_date import generate_dim_date
    return generate_dim_date


def test_dim_date_row_count(spark, generate_dim_date):
    df = generate_dim_date(spark, "2024-01-01", "2024-01-07")
    assert df.count() == 7


def test_dim_date_key_format(spark, generate_dim_date):
    df = generate_dim_date(spark, "2024-06-15", "2024-06-15")
    row = df.collect()[0]
    assert row["DateKey"] == 20240615


def test_dim_date_expected_columns(spark, generate_dim_date):
    df = generate_dim_date(spark, "2024-01-01", "2024-01-01")
    expected = {"DateKey", "FullDate", "Year", "Month", "Day", "MonthName", "DayName", "Quarter", "IsWeekend", "Season", "SourceSystem"}
    assert set(df.columns) == expected


def test_dim_date_is_weekend_saturday(spark, generate_dim_date):
    # 2024-01-06 is a Saturday
    df = generate_dim_date(spark, "2024-01-06", "2024-01-06")
    assert df.collect()[0]["IsWeekend"] is True


def test_dim_date_is_weekend_monday(spark, generate_dim_date):
    # 2024-01-08 is a Monday
    df = generate_dim_date(spark, "2024-01-08", "2024-01-08")
    assert df.collect()[0]["IsWeekend"] is False


def test_dim_date_season_winter(spark, generate_dim_date):
    df = generate_dim_date(spark, "2024-12-21", "2024-12-21")
    assert df.collect()[0]["Season"] == "Winter"


def test_dim_date_season_summer(spark, generate_dim_date):
    df = generate_dim_date(spark, "2024-07-15", "2024-07-15")
    assert df.collect()[0]["Season"] == "Summer"


# ---------------------------------------------------------------------------
# FactWeatherMetrics
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def transform_gold_facts():
    from load_fact_weather_metrics import transform_to_gold
    return transform_to_gold


@pytest.fixture(scope="module")
def gold_facts_df(spark, transform_gold_facts):
    rows = [
        Row(
            station_geohash="gcpu",
            observation_datetime="2024-06-15 12:00:00",
            visibility_m=10000,
            temperature_c=18.5,
            mean_sea_level_pressure_hpa=1013.0,
            wind_gust_ms=5.0,
            wind_direction="SW",
            wind_speed_ms=3.0,
            humidity_percentage=60.0,
            weather_code=1,
        )
    ]
    return transform_gold_facts(spark.createDataFrame(rows))


def test_gold_facts_unpivots_to_eight_rows(gold_facts_df):
    assert gold_facts_df.count() == 8


def test_gold_facts_expected_columns(gold_facts_df):
    expected = {"StationKey", "DateKey", "ObservationTime", "MetricName", "Unit", "ValueNumeric", "ValueString", "ProcessedAt", "RowHash", "SourceSystem"}
    assert set(gold_facts_df.columns) == expected


def test_gold_facts_date_key_is_int(gold_facts_df):
    rows = gold_facts_df.collect()
    assert all(isinstance(r["DateKey"], int) for r in rows)
    assert rows[0]["DateKey"] == 20240615


def test_gold_facts_numeric_metric_has_value_numeric(gold_facts_df):
    temp_row = gold_facts_df.filter("MetricName = 'Temperature'").collect()[0]
    assert temp_row["ValueNumeric"] == pytest.approx(18.5)
    assert temp_row["ValueString"] is None


def test_gold_facts_text_metric_has_value_string(gold_facts_df):
    wind_dir_row = gold_facts_df.filter("MetricName = 'Wind Direction'").collect()[0]
    assert wind_dir_row["ValueNumeric"] is None
    assert wind_dir_row["ValueString"] == "SW"


def test_gold_facts_source_system(gold_facts_df):
    rows = gold_facts_df.collect()
    assert all(r["SourceSystem"] == "met_office" for r in rows)


def test_gold_facts_row_hash_not_null(gold_facts_df):
    rows = gold_facts_df.collect()
    assert all(r["RowHash"] is not None for r in rows)
