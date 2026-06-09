import logging
import requests
import json
import time
from datetime import datetime
from dotenv import load_dotenv
import polars as pl
from upath import UPath
from airflow.models import Variable
import re
load_dotenv()

logger = logging.getLogger(__name__)

BUCKET_NAME = Variable.get("datalake_bucket", "your-gcp-datalake-bucket")
DATALAKE_ROOT = UPath(f"gs://{BUCKET_NAME}")
LANDED_DIR = DATALAKE_ROOT / "landed/met_office/station_observation_land"
METADATA_DIR = DATALAKE_ROOT / "silver/met_office/station_metadata"

BASE_URL = "https://data.hub.api.metoffice.gov.uk/observation-land/1/"


def fetch_met_office_data(geohash: str) -> dict:
    API_KEY = Variable.get("MET_OFFICE_API_KEY")
    HEADERS = {"apikey": API_KEY}

    url = f"{BASE_URL}{geohash}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error("Error for geohash %s: %s", geohash, e)
        return None


def main():
    try:
        geohashes = pl.read_delta(str(METADATA_DIR)).get_column("station_geohash").unique().to_list()
    except Exception as e:
        logger.error("Failed to load silver metadata: %s", e)
        return

    run_timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S%z")
    target_dir = LANDED_DIR / run_timestamp
    target_dir.mkdir(parents=True, exist_ok=True)

    all_data = []

    for geohash in geohashes:
        raw_response = fetch_met_office_data(geohash)
        if raw_response:
            record = {
                'station_geohash': geohash,
                'extracted_at': run_timestamp,
                'data': raw_response
            }
            all_data.append(record)

            time.sleep(0.5)

    if all_data:
        file_path = target_dir / "observations_batch.json"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("w") as f:
            json.dump(all_data, f, indent=4)
        logger.info("Saved %d records to %s", len(all_data), file_path)


if __name__ == "__main__":
    main()