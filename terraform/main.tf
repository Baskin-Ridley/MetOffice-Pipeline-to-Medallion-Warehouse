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

locals {
  layers = ["seeds", "landed", "bronze", "silver", "gold"] 

  # --- PARSE REQUIREMENTS.TXT FROM ONE DIRECTORY ABOVE ---
  # Reads the file using "../" to step up one folder level
  req_lines = split("\n", file("${path.module}/../requirements.txt"))

  # Filter out empty lines or comment lines (starting with #)
  clean_lines = [
    for line in local.req_lines : 
    trimspace(line) if trimspace(line) != "" && !startswith(trimspace(line), "#")
  ]

  # Transform the lines into a key-value map for Cloud Composer
  pypi_map = {
    for line in local.clean_lines :
    element(split("==", replace(replace(line, ">=", "=="), "<=", "==")), 0) => 
    length(regexall("(==|>=|<=)", line)) > 0 ? substr(line, length(element(split("==", replace(replace(line, ">=", "=="), "<=", "==")), 0)), length(line)) : ""
  }
}

#=========================================
# 1. STORAGE (DATA LAKE)
#=========================================
resource "google_storage_bucket" "datalake" {
  count         = var.environment == "gcp" ? 1 : 0 [cite: 1, 2]
  name          = "${var.project_id}-met-office-datalake" 
  location      = var.region 
  force_destroy = true 

  uniform_bucket_level_access = true 

  versioning {
    enabled = true 
  }
}

resource "google_storage_bucket_object" "lake_folders" {
  count   = var.environment == "gcp" ? length(local.layers) : 0 [cite: 1, 3]
  name    = "${local.layers[count.index]}/" [cite: 1, 3]
  content = "Placeholder" [cite: 3]
  bucket  = google_storage_bucket.datalake[0].name [cite: 3]
}

#=========================================
# 2. BIGQUERY (DATA WAREHOUSE / TOKENS)
#=========================================
resource "google_bigquery_dataset" "warehouse" {
  count                      = var.environment == "gcp" ? 1 : 0 [cite: 1, 4]
  dataset_id                 = "noaa_medallion_warehouse" [cite: 4]
  friendly_name              = "NOAA Medallion Data Warehouse" [cite: 4]
  description                = "Houses the Bronze, Silver, and Gold analytical data layers" [cite: 4]
  location                   = "EU" [cite: 4]
  delete_contents_on_destroy = true [cite: 4]
}

#=========================================
# 3. CLOUD COMPOSER
#=========================================
resource "google_composer_environment" "composer" {
  count  = var.environment == "gcp" ? 1 : 0 [cite: 1, 6]
  name   = "${var.project_id}-composer" [cite: 1, 6]
  region = var.region [cite: 1, 6]

  config {
    environment_size = "ENVIRONMENT_SIZE_SMALL" [cite: 6]

    node_config {
      service_account = "${var.project_number}-compute@developer.gserviceaccount.com" [cite: 1, 6]
    }

    # --- INJECT PARSED REQUIREMENTS HERE ---
    software_config {
      pypi_packages = local.pypi_map
    }
  }
}

#=========================================
# OUTPUTS
#=========================================
output "data_lake_bucket_name" {
  value       = var.environment == "gcp" ? google_storage_bucket.datalake[0].name : "local_fallback" [cite: 1, 7]
  description = "The target bucket name for your ingestion scripts" [cite: 7]
}

output "bigquery_dataset_id" {
  value       = var.environment == "gcp" ? google_bigquery_dataset.warehouse[0].dataset_id : "local_fallback" [cite: 1, 8]
  description = "The target dataset where your analytical layers reside" [cite: 8]
}

output "composer_environment_name" {
  value       = var.environment == "gcp" ? google_composer_environment.composer[0].name : "local_fallback" [cite: 1, 9]
  description = "The Cloud Composer environment name for DAG deployment" [cite: 9]
}