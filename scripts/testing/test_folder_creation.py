import os
import uuid

# Configure the directory path
LANDED_DIR = "landed/met_office/test_station_metadata"

def generate_random_blob():
    # 1. Create the nested directories (exist_ok=True prevents errors if it already exists)
    os.makedirs(LANDED_DIR, exist_ok=True)
    print(f"Ensured directory exists: {LANDED_DIR}")

    # 2. Create a random filename (e.g., blob_a1b2c3d4.dat)
    random_id = uuid.uuid4().hex[:8]
    filename = f"blob_{random_id}.dat"
    file_path = os.path.join(LANDED_DIR, filename)

    # 3. Generate random "blob" data (1024 bytes = 1 KB of random binary data)
    blob_data = os.urandom(1024)

    # 4. Write the binary data to the file
    with open(file_path, "wb") as f:
        f.write(blob_data)
        
    print(f"Successfully created random blob file: {file_path}")

if __name__ == "__main__":
    generate_random_blob()