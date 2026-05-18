variable "project_id" {
  type        = string
  description = "The Google Cloud Project ID"
}

variable "region" {
  type        = string
  default     = "europe-west2"
}

variable "environment" {
  type        = string
  description = "Toggle between 'local' and 'gcp'"
  default     = "local"
}