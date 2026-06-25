output "raw_bucket" {
  value = google_storage_bucket.raw_bucket.name
}

output "curated_bucket" {
  value = google_storage_bucket.curated_bucket.name
}

output "dataproc_etl_service_account" {
  value = google_service_account.dataproc_etl.email
}