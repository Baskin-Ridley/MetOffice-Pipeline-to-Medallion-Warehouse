terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket = "noaa-medallion-warehouse-met-office-datalake"
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
# 1. STORAGE (DATA LAKE)
#=========================================
resource "google_storage_bucket" "datalake" {
  count         = var.environment == "gcp" ? 1 : 0
  name          = "${var.project_id}-met-office-datalake"
  location      = var.region
  force_destroy = true 

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }
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
  dataset_id                 = "noaa_medallion_warehouse"
  friendly_name              = "Met Office Medallion Data Warehouse"
  description                = "Houses the Bronze, Silver, and Gold analytical data layers"
  location                   = "EU" 
  delete_contents_on_destroy = true 
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
}

#=========================================
# 4. IAM PERMISSIONS FOR SECRET MANAGER
#=========================================
resource "google_secret_manager_secret_iam_member" "composer_secret_accessor" {
  count     = var.environment == "gcp" ? 1 : 0
  project   = var.project_id
  secret_id = "airflow-variables-MET_OFFICE_API_KEY"
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
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