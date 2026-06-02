terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket = "met-office-medallion-warehouse-datalake"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  layers = ["seeds", "landed", "bronze", "silver", "gold"]
}

#=========================================
# 0. APIS
#=========================================
resource "google_project_service" "apis" {
  for_each = var.environment == "gcp" ? toset([
    "cloudbuild.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "composer.googleapis.com",
    "secretmanager.googleapis.com",
    "bigqueryconnection.googleapis.com",
  ]) : toset([])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

#=========================================
# 1. STORAGE (DATA LAKE)
#=========================================
resource "google_storage_bucket" "datalake" {
  count         = var.environment == "gcp" ? 1 : 0
  name          = "${var.project_id}-datalake"
  location      = var.region
  force_destroy = true 

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket_object" "lake_folders" {
  count   = var.environment == "gcp" ? length(local.layers) : 0
  name    = "${local.layers[count.index]}/"
  content = "Placeholder"
  bucket  = google_storage_bucket.datalake[0].name
}

#=========================================
# 2. BIGQUERY (DATA WAREHOUSE / TOKENS)
#=========================================
resource "google_bigquery_dataset" "warehouse" {
  count                      = var.environment == "gcp" ? 1 : 0
  dataset_id                 = "met_office_medallion_warehouse"
  friendly_name              = "Met Office Medallion Data Warehouse"
  description                = "Houses the Bronze, Silver, and Gold analytical data layers"
  location                   = "EU" 
  delete_contents_on_destroy = true

  depends_on = [google_project_service.apis]
}

#=========================================
# 3. CLOUD COMPOSER
#=========================================
resource "google_composer_environment" "composer" {
  count  = var.environment == "gcp" ? 1 : 0
  name   = "${var.project_id}-composer"
  region = var.region

  config {
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      service_account = "${var.project_number}-compute@developer.gserviceaccount.com"
    }

    software_config {
      pypi_packages = {
        polars            = "==1.40.1"
        requests          = "==2.33.1"
        pyspark           = "==3.5.3"
        delta-spark       = "==3.2.0"
        deltalake         = "==1.5.1"
      }

      env_variables = {
        AIRFLOW_VAR_DATALAKE_BUCKET = google_storage_bucket.datalake[0].name
      }

      airflow_config_overrides = {
        "secrets-backend" = "airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend"
      }
    }
  }

  depends_on = [google_project_service.apis]
}

#=========================================
# 4. SECRET MANAGER
#=========================================
resource "google_secret_manager_secret" "met_office_api_key" {
  count     = var.environment == "gcp" ? 1 : 0
  project   = var.project_id
  secret_id = "airflow-variables-MET_OFFICE_API_KEY"

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_iam_member" "composer_secret_accessor" {
  count     = var.environment == "gcp" ? 1 : 0
  project   = var.project_id
  secret_id = "airflow-variables-MET_OFFICE_API_KEY"
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

#=========================================
# 5. BIGLAKE CONNECTION + EXTERNAL TABLES
#=========================================
resource "google_bigquery_connection" "biglake" {
  count         = var.environment == "gcp" ? 1 : 0
  connection_id = "${var.project_id}-biglake"
  location      = "EU"
  cloud_resource {}

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket_iam_member" "biglake_storage_viewer" {
  count  = var.environment == "gcp" ? 1 : 0
  bucket = google_storage_bucket.datalake[0].name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_bigquery_connection.biglake[0].cloud_resource[0].service_account_id}"
}

resource "google_bigquery_table" "dim_date" {
  count               = var.environment == "gcp" ? 1 : 0
  dataset_id          = google_bigquery_dataset.warehouse[0].dataset_id
  table_id            = "DimDate"
  deletion_protection = false

  external_data_configuration {
    source_uris   = ["gs://${google_storage_bucket.datalake[0].name}/gold/master/dim_date"]
    source_format = "DELTA_LAKE"
    connection_id = google_bigquery_connection.biglake[0].name
    autodetect    = true
  }
}

resource "google_bigquery_table" "dim_weather_stations" {
  count               = var.environment == "gcp" ? 1 : 0
  dataset_id          = google_bigquery_dataset.warehouse[0].dataset_id
  table_id            = "DimWeatherStations"
  deletion_protection = false

  external_data_configuration {
    source_uris   = ["gs://${google_storage_bucket.datalake[0].name}/gold/weather/dim_weather_stations"]
    source_format = "DELTA_LAKE"
    connection_id = google_bigquery_connection.biglake[0].name
    autodetect    = true
  }
}

resource "google_bigquery_table" "fact_weather_metrics" {
  count               = var.environment == "gcp" ? 1 : 0
  dataset_id          = google_bigquery_dataset.warehouse[0].dataset_id
  table_id            = "FactWeatherMetrics"
  deletion_protection = false

  external_data_configuration {
    source_uris   = ["gs://${google_storage_bucket.datalake[0].name}/gold/weather/weather_metrics"]
    source_format = "DELTA_LAKE"
    connection_id = google_bigquery_connection.biglake[0].name
    autodetect    = true
  }
}

#=========================================
# 6. CLOUD BUILD PERMISSIONS
#=========================================
resource "google_project_iam_member" "cloudbuild_roles" {
  for_each = var.environment == "gcp" ? toset([
    "roles/editor",
    "roles/composer.admin",
    "roles/secretmanager.admin",
    "roles/bigquery.admin",
  ]) : toset([])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${var.project_number}@cloudbuild.gserviceaccount.com"

  depends_on = [google_project_service.apis]
}

#=========================================
# OUTPUTS
#=========================================
output "data_lake_bucket_name" {
  value       = var.environment == "gcp" ? google_storage_bucket.datalake[0].name : "local_fallback"
  description = "The target bucket name for your ingestion scripts"
}

output "bigquery_dataset_id" {
  value       = var.environment == "gcp" ? google_bigquery_dataset.warehouse[0].dataset_id : "local_fallback"
  description = "The target dataset where your analytical layers reside"
}

output "composer_environment_name" {
  value       = var.environment == "gcp" ? google_composer_environment.composer[0].name : "local_fallback"
  description = "The Cloud Composer environment name for DAG deployment"
}