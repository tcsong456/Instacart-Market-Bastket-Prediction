resource "google_service_account" "dataproc_etl" {
  account_id   = "dataproc-etl-sa"
  display_name = "Dataproc ETL Service Account"
  depends_on   = [google_project_service.required_apis]
}

resource "google_storage_bucket_iam_member" "raw_bucket_reader" {
  bucket = google_storage_bucket.raw_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.dataproc_etl.email}"
}

resource "google_storage_bucket_iam_member" "curated_bucket_writer" {
  bucket = google_storage_bucket.curated_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc_etl.email}"
}

resource "google_project_iam_member" "dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc_etl.email}"
}

resource "google_project_iam_member" "logging_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.dataproc_etl.email}"
}

resource "google_project_iam_member" "monitoring_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.dataproc_etl.email}"
}