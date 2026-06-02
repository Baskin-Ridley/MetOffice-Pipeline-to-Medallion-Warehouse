import os
from google.cloud import storage

def test_connection():
    # Retrieve project ID from the environment
    project_id = os.getenv('GCP_PROJECT_ID')
    
    try:
        if not project_id:
            raise ValueError("GCP_PROJECT_ID environment variable is missing")

        # The library automatically locates the credentials file using the 
        # GOOGLE_APPLICATION_CREDENTIALS environment variable
        client = storage.Client(project=project_id)
        
        # Verify connection by listing buckets
        buckets = list(client.list_buckets())
        
        if not buckets:
            print(f"Connected to {project_id}. No buckets found.")
        else:
            bucket_names = [b.name for b in buckets]
            print(f"Connected to {project_id}.")
            print(f"Buckets found: {bucket_names}")

    except Exception as e:
        print(f"Failed to connect to Google Cloud Storage: {e}")

if __name__ == "__main__":
    test_connection()