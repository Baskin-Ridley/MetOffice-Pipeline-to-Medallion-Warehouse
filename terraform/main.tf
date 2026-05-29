terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  # Credentials are intentionally omitted so Terraform can use
  # Application Default Credentials in Cloud Build or local ADC.
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
  force_destroy = true # Allows easy tear-down for portfolio resetting

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }
}

# Generates your Medallion layer directory structure inside the bucket
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
  friendly_name              = "NOAA Medallion Data Warehouse"
  description                = "Houses the Bronze, Silver, and Gold analytical data layers"
  location                   = "EU" # Keeps data residency consistent
  delete_contents_on_destroy = true # Helpful for clean local portfolio teardowns
}

#=========================================
# 3. CLOUD COMPOSER
#=========================================
resource "google_composer_environment" "composer" {
  count  = var.environment == "gcp" ? 1 : 0
  name   = "${var.project_id}-composer"
  region = var.region

  config {
    # MINIMAL CHANGES MADE HERE:
    # Swapped 'node_count = 3' for modern autoscaling sizing profiles
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    software_config {
      pypi_packages = {
        "apache-airflow-providers-google" = ">=21.3.0"
      }
    }
  }
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