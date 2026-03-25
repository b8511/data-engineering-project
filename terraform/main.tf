terraform {
  required_version = ">= 1.0"

  backend "local" {}

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

# Data Lake Bucket
resource "google_storage_bucket" "data_lake" {
  name     = "${var.gcs_bucket_name}-${var.project}"
  location = var.location

  storage_class               = var.gcs_storage_class
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # days
    }
  }

  force_destroy = true
}

# BigQuery dataset for raw/external tables
resource "google_bigquery_dataset" "raw" {
  dataset_id = var.bq_dataset_raw
  project    = var.project
  location   = var.location
}

# BigQuery dataset for dbt analytics tables
resource "google_bigquery_dataset" "analytics" {
  dataset_id = var.bq_dataset_analytics
  project    = var.project
  location   = var.location
}
