resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "google_storage_bucket" "data_bucket" {
  name          = "${var.data_bucket_name}-${random_id.bucket_suffix.hex}"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  labels = {
    layer = "normal_data"
    env   = "dev"
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_storage_bucket" "dataproc_staging" {
  name          = "${var.dataproc_staging_bucket_name}-${random_id.bucket_suffix.hex}"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  labels = {
    layer = "dataproc"
    env   = "dev"
  }

  depends_on = [google_project_service.required_apis]
}
