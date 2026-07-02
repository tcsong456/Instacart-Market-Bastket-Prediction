output "data_bucket_uri" {
  value = "gs://${google_storage_bucket.data_bucket.name}"
}

output "dataproc_staging_bucket" {
  value = "gs://${google_storage_bucket.dataproc_staging.name}"
}

output "dataproc_etl_service_account" {
  value = google_service_account.dataproc_etl.email
}