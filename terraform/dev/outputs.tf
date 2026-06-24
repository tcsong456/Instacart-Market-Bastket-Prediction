output "raw_bucket" {
  value = google_storage_bucket.raw_bucket.name
}

output "bronze_bucket" {
  value = google_storage_bucket.bronze_bucket.name
}

output "dataproc_etl_service_account" {
  value = google_service_account.dataproc_etl.email
}