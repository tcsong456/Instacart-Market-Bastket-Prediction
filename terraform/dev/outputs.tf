output "raw_bucket_uri" {
  value = "gs://${google_storage_bucket.raw_bucket.name}"
}

output "bronze_bucket_uri" {
  value = "gs://${google_storage_bucket.bronze_bucket.name}"
}

output "silver_bucket_uri" {
  value = "gs://${google_storage_bucket.silver_bucket.name}"
}

output "gold_bucket_uri" {
  value = "gs://${google_storage_bucket.gold_bucket.name}"
}

output "dataproc_staging_bucket" {
  value = "gs://${google_storage_bucket.dataproc_staging.name}"
}

output "dataproc_etl_service_account" {
  value = google_service_account.dataproc_etl.email
}