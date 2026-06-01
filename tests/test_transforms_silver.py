"""
Tests for the silver-layer transform functions.
These are pure DataFrame → DataFrame functions; no GCS or Delta needed.
"""
import pytest
from pyspark.sql import Row


# ---------------------------------------------------------------------------
# Station metadata silver transform
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def transform_metadata_silver():
    from load_met_office_metadata_to_silver import transform_to_silver
    return transform_to_silver


@pytest.fixture(scope="module")
def metadata_silver_df(spark, transform_metadata_silver):
    rows = [
        Row(
            station_name="  Heathrow  ",
            geohash="gcpu",
            latitude=51.477,
            longitude=-0.461,
            area="  Greater London  ",
            country="  England  ",
            country_code="GB-ENG",
            station_type="  land  ",
            olson_time_zone="Europe/London",
            region="  se  ",
        )
    ]
    return transform_metadata_silver(spark.createDataFrame(rows))


def test_metadata_silver_expected_columns(metadata_silver_df):
    expected = {
        "station_name", "station_geohash", "latitude", "longitude",
        "county_name", "country_name", "country_code", "station_type",
        "olson_time_zone", "region_code", "_processed_at", "_row_hash", "_source_system",
    }
    assert set(metadata_silver_df.columns) == expected


def test_metadata_silver_row_count_preserved(metadata_silver_df):
    assert metadata_silver_df.count() == 1


def test_metadata_silver_strings_are_trimmed(metadata_silver_df):
    row = metadata_silver_df.collect()[0]
    assert row["station_name"] == "Heathrow"
    assert row["county_name"] == "Greater London"
    assert row["country_name"] == "England"
    assert row["station_type"] == "land"


def test_metadata_silver_region_is_uppercased(metadata_silver_df):
    row = metadata_silver_df.collect()[0]
    assert row["region_code"] == "SE"


def test_metadata_silver_source_system(metadata_silver_df):
    row = metadata_silver_df.collect()[0]
    assert row["_source_system"] == "met_office"


def test_metadata_silver_row_hash_not_null(metadata_silver_df):
    row = metadata_silver_df.collect()[0]
    assert row["_row_hash"] is not None
    assert len(row["_row_hash"]) == 64  # SHA-256 hex string


# ---------------------------------------------------------------------------
# Land observations silver transform
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def transform_observations_silver():
    from load_met_office_land_observations_to_silver import transform_to_silver
    return transform_to_silver


@pytest.fixture(scope="module")
def observations_silver_df(spark, transform_observations_silver):
    rows = [
        Row(
            station_geohash="gcpu",
            datetime="2024-01-15T12:00:00Z",
            visibility=10000,
            temperature=8.5,
            mslp=1013.0,
            wind_gust=15.0,
            wind_direction="SW",
            wind_speed=10.0,
            humidity=75.0,
            weather_code=3,
            pressure_tendency="falling",
        )
    ]
    return transform_observations_silver(spark.createDataFrame(rows))


def test_observations_silver_expected_columns(observations_silver_df):
    expected = {
        "station_geohash", "observation_datetime", "visibility_m", "temperature_c",
        "mean_sea_level_pressure_hpa", "wind_gust_ms", "wind_direction", "wind_speed_ms",
        "humidity_percentage", "weather_code", "pressure_tendency",
        "_processed_at", "_row_hash", "_source_system",
    }
    assert set(observations_silver_df.columns) == expected


def test_observations_silver_row_count_preserved(observations_silver_df):
    assert observations_silver_df.count() == 1


def test_observations_silver_source_system(observations_silver_df):
    row = observations_silver_df.collect()[0]
    assert row["_source_system"] == "met_office"


def test_observations_silver_row_hash_not_null(observations_silver_df):
    row = observations_silver_df.collect()[0]
    assert row["_row_hash"] is not None
    assert len(row["_row_hash"]) == 64
