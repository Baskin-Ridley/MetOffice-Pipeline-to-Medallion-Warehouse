#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "🐳 Starting Docker containers..."
# Starts containers in the background (-d)
docker compose up -d

echo "⏳ Waiting 60 seconds for Airflow services to fully initialize..."
# Count down the minute visually so you know it hasn't frozen
for i in {60..1}; do
    echo -ne "Time remaining: ${i}s \r"
    sleep 1
done
echo -e "\n✅ Wait time complete!"

echo "🚀 Executing metadata pipeline test script..."
# Runs your target python command inside the active container
docker compose exec airflow-webserver python /opt/airflow/scripts/testing/test_call_metadata_pipeline.py

echo "🎉 Script execution finished successfully."