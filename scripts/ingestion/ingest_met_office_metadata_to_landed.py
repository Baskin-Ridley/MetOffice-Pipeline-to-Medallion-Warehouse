import polars as pl
import requests
import time
import os
from typing import Dict, Optional
import json
from datetime import datetime

# configure
SEEDS_FILE = "seeds/met_office_weather_stations_seed.csv"
LANDED_DIR = "landed/met_office/station_metadata"  
API_KEY = open(os.getenv("MET_OFFICE_API_KEY")).read().strip()
HEADERS = {"apikey": API_KEY} # met office expects key to be in header
BASE_URL = " https://data.hub.api.metoffice.gov.uk/observation-land/1/nearest"

# load seeds
seeds_df = (
    pl.read_csv(SEEDS_FILE)
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

def fetch_met_office_metadata (lat: float, lon: float) -> Optional[Dict]:
    params = {
        "lat": round(lat, 2),
        "lon": round(lon, 2)
    }
    
    try:
        response = requests.get(BASE_URL, headers=HEADERS, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]  # Return the first metadata entry
        else:
            print(f"No metadata found for coordinates: ({lat}, {lon})")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching metadata for coordinates: ({lat}, {lon}) - {e}")
        return None
    
def get_run_timestamp() -> str: 
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def save_metadata_to_landed(station_name: str, metadata: Dict, run_timestamp: str):
    # <-- EDIT: Removed geohash, path is just LANDED_DIR / timestamp
    target_dir = os.path.join(LANDED_DIR, run_timestamp)
    
    os.makedirs(target_dir, exist_ok=True)
    
    # <-- EDIT: File name is just the station name
    file_path = os.path.join(target_dir, f"{station_name}.json")
    with open(file_path, "w") as f:
        json.dump(metadata, f, indent=4)

def main():
    run_timestamp = get_run_timestamp()
    for row in seeds_df.head(3).iter_rows(named=True):
    #for row in seeds_df.iter_rows(named=True):
        station_name = row["station_name"]
        lat = row["latitude"]
        lon = row["longitude"]
        
        print(f"Fetching metadata for station: {station_name} at coordinates: ({lat}, {lon})")
        metadata = fetch_met_office_metadata(lat, lon)
        
        if metadata:
            save_metadata_to_landed(station_name, metadata, run_timestamp)
            print(f"Metadata for station {station_name} saved to landed zone.")
        else:
            print(f"Failed to fetch metadata for station: {station_name}")
        
        time.sleep(1)  # Sleep to respect API rate limits

if __name__ == "__main__":
    main()