variable "project" {
  description = "Your GCP Project ID"
  type        = string
}

variable "region" {
  description = "Region for GCP resources"
  default     = "us-central1"
  type        = string
}

variable "location" {
  description = "Project location"
  default     = "US"
  type        = string
}

variable "bq_dataset_raw" {
  description = "BigQuery dataset for raw/external tables"
  default     = "wine_quality_raw"
  type        = string
}

variable "bq_dataset_analytics" {
  description = "BigQuery dataset for dbt-transformed analytics tables"
  default     = "wine_quality_analytics"
  type        = string
}

variable "gcs_bucket_name" {
  description = "GCS bucket for the data lake (raw files)"
  default     = "wine-quality-data-lake"
  type        = string
}

variable "gcs_storage_class" {
  description = "Storage class for the GCS bucket"
  default     = "STANDARD"
  type        = string
}
