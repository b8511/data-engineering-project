output "gcs_bucket_name" {
  description = "The name of the GCS data lake bucket"
  value       = google_storage_bucket.data_lake.name
}

output "bq_dataset_raw" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "bq_dataset_analytics" {
  description = "BigQuery analytics dataset ID"
  value       = google_bigquery_dataset.analytics.dataset_id
}
