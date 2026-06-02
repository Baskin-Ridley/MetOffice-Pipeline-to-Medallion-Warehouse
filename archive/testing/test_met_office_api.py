import os
import requests
import json
import sys

# 1. Get the FILE PATH from the environment variable
KEY_FILE_PATH = os.getenv("MET_OFFICE_API_KEY")

if not KEY_FILE_PATH:
    print("❌ ERROR: MET_OFFICE_API_KEY is missing from the environment.")
    sys.exit(1)

# Clean up the path just in case there are hidden spaces
KEY_FILE_PATH = KEY_FILE_PATH.strip(' "\'')

print(f"📂 Looking for API key file at: {KEY_FILE_PATH}")

# 2. Read the actual token from that file
try:
    with open(KEY_FILE_PATH, "r") as f:
        # .read() grabs the text, .strip() removes whitespace, newlines, and rogue quotes
        API_KEY = f.read().strip(' \n\r"\'')
except FileNotFoundError:
    print(f"❌ ERROR: Could not find the file at {KEY_FILE_PATH}.")
    print("⚠️ DOCKER TIP: Make sure this path is where the file lives INSIDE the container, not on your host machine!")
    sys.exit(1)
except IsADirectoryError:
    print(f"❌ ERROR: {KEY_FILE_PATH} is a folder, but it needs to be a text file containing the key.")
    sys.exit(1)

if not API_KEY:
    print(f"❌ ERROR: The key file at {KEY_FILE_PATH} is empty.")
    sys.exit(1)

# 3. Your exact hardcoded URL
URL = "https://data.hub.api.metoffice.gov.uk/observation-land/1/nearest?lat=51.47&lon=-0.45"

# 4. Header variations
header_variations = [
    {"name": "Standard apikey", "headers": {"apikey": API_KEY, "Accept": "application/json"}},
    {"name": "Bearer Token", "headers": {"Authorization": f"Bearer {API_KEY}", "Accept": "application/json"}},
    {"name": "X-API-Key", "headers": {"X-API-Key": API_KEY, "Accept": "application/json"}},
]

print("\n🕵️‍♂️ --- STARTING API DEBUGGER --- 🕵️‍♂️")
print(f"Target URL: {URL}")
print(f"Key loaded: {API_KEY[:10]}...[HIDDEN] (Length: {len(API_KEY)})\n")

success = False

# 5. Loop through the header variations
for attempt in header_variations:
    print(f"🔄 Trying auth method: [{attempt['name']}] ...")
    
    try:
        response = requests.get(URL, headers=attempt["headers"])
        
        if response.status_code == 200:
            print(f"✅ SUCCESS using [{attempt['name']}]!\n")
            print("--- DATA SNIPPET ---")
            print(json.dumps(response.json(), indent=2)[:500] + "\n...[truncated]")
            success = True
            break
            
        elif response.status_code == 401:
            print(f"❌ 401 Unauthorized with [{attempt['name']}].")
            # Print the Met Office's hidden error message
            if response.text:
                print(f"   API says: {response.text}")
            
        else:
            print(f"⚠️ Unexpected Status: {response.status_code}")
            print(f"   Response: {response.text}\n")
            
    except Exception as e:
        print(f"🚨 Connection Error: {e}")

if not success:
    print("\n💀 --- FINAL DIAGNOSIS --- 💀")
    print("All auth methods failed.")
    print("Check the 'API says' output above to see if it complains about an invalid token vs. missing subscription.")