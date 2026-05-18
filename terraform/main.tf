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
}

# Plural 'locals' block is used only for declaring variables
locals {
  layers = ["landed", "bronze", "silver", "gold"]
}

# Dynamic GCS Bucket: Only spins up if environment is set to 'gcp'
resource "google_storage_bucket" "datalake" {
  count         = var.environment == "gcp" ? 1 : 0
  name          = "${var.project_id}-met-office-datalake"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket_object" "lake_folders" {
  # FIX 1: Changed 'locals.layers' to 'local.layers' (singular) for references
  count   = var.environment == "gcp" ? length(local.layers) : 0
  name    = "${local.layers[count.index]}/"
  content = "Placeholder"
  bucket  = google_storage_bucket.datalake[0].name
}

output "data_lake_root_path" {
  # FIX 2: Safeguarded against index [0] errors when count is 0 in local environment
  value       = var.environment == "gcp" ? "gs://${one(google_storage_bucket.datalake[*].name)}" : "/opt/airflow"
  description = "Expose this value to your Python scripts as DATALAKE_ROOT"
}