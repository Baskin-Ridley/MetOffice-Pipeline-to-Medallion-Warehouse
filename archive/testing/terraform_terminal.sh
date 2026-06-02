terraform init
terraform apply -auto-approve \
  -var="project_id=YOUR_PROJECT_ID" \
  -var="region=europe-west2" \
  -var="environment=gcp" \
  -var="gcp_credentials_json=$(cat /path/to/your-key.json)"