import logging
import polars as pl
import requests
import time
from typing import Dict, Optional
import json
import re
from datetime import datetime
from upath import UPath
from dotenv import load_dotenv
from airflow.models import Variable


load_dotenv()

logger = logging.getLogger(__name__)

BUCKET_NAME = Variable.get("datalake_bucket", "your-gcp-datalake-bucket")
DATALAKE_ROOT = UPath(f"gs://{BUCKET_NAME}")
SEEDS_FILE = DATALAKE_ROOT / "seeds/met_office_weather_stations_seed.csv"
LANDED_DIR = DATALAKE_ROOT / "landed/met_office/station_metadata"

BASE_URL = "https://data.hub.api.metoffice.gov.uk/observation-land/1/nearest"


def fetch_met_office_metadata(lat: float, lon: float) -> Optional[Dict]:
    API_KEY = Variable.get("MET_OFFICE_API_KEY")
    HEADERS = {"apikey": API_KEY}

    params = {
        "lat": round(lat, 2),
        "lon": round(lon, 2)
    }

    try:
        response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
        else:
            logger.warning("No metadata found for coordinates: (%s, %s)", lat, lon)
            return None

    except requests.exceptions.RequestException as e:
        logger.error("Error fetching metadata for coordinates: (%s, %s) - %s", lat, lon, e)
        return None


def get_run_timestamp() -> str:
    return datetime.now().astimezone().strftime("%Y%m%d_%H%M%S%z")


def save_metadata_to_landed(station_name: str, latitude: float, longitude: float, region: str, country_code: str, station_type: str, metadata: Dict, run_timestamp: str):
    output_data = {
        "station_name": station_name,
        "latitude": latitude,
        "longitude": longitude,
        "region": region,
        "country_code": country_code,
        "station_type": station_type,
        **metadata
    }

    file_path = LANDED_DIR / run_timestamp / f"{re.sub(r'[^A-Za-z0-9._-]', '_', station_name)}.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with file_path.open("w") as f:
        json.dump(output_data, f, indent=4)


def main():
    seeds_df = (
        pl.read_csv(str(SEEDS_FILE))
        .filter(pl.col("is_monitored") == 1)
        .with_columns(
            pl.col("location")
            .str.split(",")
            .list.get(0)
            .str.strip_chars()
            .cast(pl.Float64, strict=False)
            .alias("latitude"),

            pl.col("location")
            .str.split(",")
            .list.get(1)
            .str.strip_chars()
            .cast(pl.Float64, strict=False)
            .alias("longitude")
        )
    )

    run_timestamp = get_run_timestamp()
    for row in seeds_df.iter_rows(named=True):
        station_name = row["station_name"]
        latitude = row["latitude"]
        longitude = row["longitude"]
        region = row["region"]
        country_code = row["country_code"]
        station_type = row["station_type"]

        logger.info("Fetching metadata for station: %s at coordinates: (%s, %s)", station_name, latitude, longitude)
        metadata = fetch_met_office_metadata(latitude, longitude)

        if metadata:
            save_metadata_to_landed(station_name, latitude, longitude, region, country_code, station_type, metadata, run_timestamp)
            logger.info("Metadata for station %s saved to landed zone.", station_name)
        else:
            logger.warning("Failed to fetch metadata for station: %s", station_name)

        time.sleep(0.5)


if __name__ == "__main__":
    main()