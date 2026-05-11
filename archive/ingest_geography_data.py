import gzip
import json
import httpx
from datetime import datetime
from pathlib import Path
import polars as pl

# Configuration
BASE_LANDED_DIR = Path("landed/geography")
BACKUP_FILE = Path("seeds/geography_backup_seed.json")
URL = "https://github.com/dr5hn/countries-states-cities-database/releases/download/v3.2-export.2/geojson-cities.geojson.gz"

def main():
    # Create a unique folder based on the current fetch time
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fetch_dir = BASE_LANDED_DIR / timestamp
    target_file = fetch_dir / "geography.json"
    
    fetch_dir.mkdir(parents=True, exist_ok=True)

    try:
        print(f"Downloading and extracting data from {URL}...")
        
        with httpx.Client(follow_redirects=True, timeout=30.0) as client:
            response = client.get(URL)
            response.raise_for_status()
            
            decompressed_data = gzip.decompress(response.content)
            data = json.loads(decompressed_data)
            
            with open(target_file, 'w', encoding='utf-8') as out_f:
                json.dump(data, out_f, indent=4)
        
        print(f"✅ Data successfully saved to {target_file}")

    except Exception as e:
        print(f"❌ Failed to download or process data: {e}")
        print(f"🔄 Falling back to backup file: {BACKUP_FILE}")
        
        if BACKUP_FILE.exists():
            pl.read_json(BACKUP_FILE).write_json(target_file)
            print(f"✅ Backup data successfully saved to {target_file}")
        else:
            print("🚨 Critical Error: Backup file not found. No data ingested.")

if __name__ == "__main__":
    main()