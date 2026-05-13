import polars as pl
import requests
import time
import os
from typing import Dict, Optional
import json
from datetime import datetime
from upath import UPath as Path

# configure
SEEDS_FILE = Path("/opt/airflow/seeds/met_office_weather_stations_seed.csv")
LANDED_DIR = Path("/opt/airflow/landed/met_office/station_metadata")  
API_KEY = open(os.getenv("MET_OFFICE_API_KEY")).read().strip()
HEADERS = {"apikey": API_KEY} 
BASE_URL = "https://data.hub.api.metoffice.gov.uk/observation-land/1/nearest"

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
            return data[0]  
        else:
            print(f"No metadata found for coordinates: ({lat}, {lon})")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching metadata for coordinates: ({lat}, {lon}) - {e}")
        return None
    
def get_run_timestamp() -> str: 
    return datetime.now().astimezone().strftime("%Y%m%d_%H%M%S%z")

def save_metadata_to_landed(station_name: str, latitude: float, longitude: float, region: str, country_code: str, station_type: str, metadata: Dict, run_timestamp: str):
    target_dir = os.path.join(LANDED_DIR, run_timestamp)
    os.makedirs(target_dir, exist_ok=True)
    
    output_data = {
        "station_name": station_name,
        "latitude": latitude,
        "longitude": longitude,
        "region": region,
        "country_code": country_code,
        "station_type": station_type,
        **metadata  
    }
    
    file_path = os.path.join(target_dir, f"{station_name}.json")
    with open(file_path, "w") as f:
        json.dump(output_data, f, indent=4)

def main():
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

    run_timestamp = get_run_timestamp()
    for row in seeds_df.head(3).iter_rows(named=True):
        station_name = row["station_name"]
        latitude = row["latitude"]
        longitude = row["longitude"]
        region = row["region"]
        country_code = row["country_code"]
        station_type = row["station_type"]

        print(f"Fetching metadata for station: {station_name} at coordinates: ({latitude}, {longitude})")
        metadata = fetch_met_office_metadata(latitude, longitude)
        
        if metadata:
            save_metadata_to_landed(station_name, latitude, longitude, region, country_code, station_type, metadata, run_timestamp)
            print(f"Metadata for station {station_name} saved to landed zone.")
        else:
            print(f"Failed to fetch metadata for station: {station_name}")
        
        time.sleep(0.5)  

if __name__ == "__main__":
    main()