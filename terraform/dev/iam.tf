data "google_project" "current" {
  project_id = var.project_id
}

resource "google_service_account" "dataproc_etl" {
  account_id   = "dataproc-etl-sa"
  display_name = "Dataproc ETL Service Account"
  depends_on   = [google_project_service.required_apis]
}

resource "google_storage_bucket_iam_member" "raw_bucket_admin" {
  bucket = google_storage_bucket.raw_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc_etl.email}"
}

resource "google_storage_bucket_iam_member" "dataproc_staging_admin" {
  bucket = google_storage_bucket.dataproc_staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc_etl.email}"
}

resource "google_storage_bucket_iam_member" "bronze_bucket_admin" {
  bucket = google_storage_bucket.bronze_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc_etl.email}"
}

resource "google_storage_bucket_iam_member" "silver_bucket_admin" {
  bucket = google_storage_bucket.silver_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc_etl.email}"
}

resource "google_storage_bucket_iam_member" "gold_bucket_admin" {
  bucket = google_storage_bucket.gold_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc_etl.email}"
}

resource "google_project_iam_member" "dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc_etl.email}"
}

resource "google_project_iam_member" "user_dataproc_editor" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "user:congxisong@hotmail.com"
}

resource "google_service_account_iam_member" "user_act_as_dataproc_sa" {
  service_account_id = google_service_account.dataproc_etl.name
  role               = "roles/iam.serviceAccountUser"
  member             = "user:congxisong@hotmail.com"
}

resource "google_storage_bucket_iam_member" "dataproc_agent_staging_admin" {
  bucket = google_storage_bucket.dataproc_staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:service-${data.google_project.current.number}@dataproc-accounts.iam.gserviceaccount.com"
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