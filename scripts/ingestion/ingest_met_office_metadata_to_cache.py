import polars as pl
import requests
import time
import os
from typing import Dict, Optional

# configure
SEEDS_FILE = "seeds/met_office_weather_stations_seed.csv"
CACHE_DIR = "cache/met_office_metadata"
API_KEY = os.getenv("MET_OFFICE_API_KEY")
HEADERS = {"apikey": API_KEY} # met office expects key to be in header
BASE_URL = " https://data.hub.api.metoffice.gov.uk/observation-land/1/nearest"





