import os
import requests
import json
import time # Fixed import from 'from datetime import datetime, time' to avoid conflict
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv
from deltalake import DeltaTable
import polars as pl
from upath import UPath as Path


# configure
load_dotenv()
DATALAKE_ROOT = Path(os.getenv("DATALAKE_ROOT", "/opt/airflow"))
LANDED_DIR = DATALAKE_ROOT / "landed/met_office/station_observation_land"
METADATA_DIR = DATALAKE_ROOT / "silver/met_office/station_metadata"
#API_KEY = open(os.getenv("MET_OFFICE_API_KEY_PATH")).read().strip()
API_KEY = os.getenv("MET_OFFICE_API_KEY")
HEADERS = {"apikey": API_KEY} # met office expects key to be in header
BASE_URL = "https://data.hub.api.metoffice.gov.uk/observation-land/1/"

def fetch_met_office_data(geohash: str) -> dict:
    url = f"{BASE_URL}{geohash}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error for geohash {geohash}: {e}")
        return None

def main():
    # Load geohashes from Silver metadata table
    try:
        geohashes = pl.read_delta(METADATA_DIR).get_column("station_geohash").unique().to_list()
    except Exception as e:
        print(f"Failed to load silver metadata: {e}")
        return

    run_timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S%z")
    target_dir = os.path.join(LANDED_DIR, run_timestamp)
    os.makedirs(target_dir, exist_ok=True)

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
        file_path = os.path.join(target_dir, "observations_batch.json")
        pl.DataFrame(all_data).write_json(file_path,)
        print(f"Saved {len(all_data)} records to {file_path}")
if __name__ == "__main__":
    main()