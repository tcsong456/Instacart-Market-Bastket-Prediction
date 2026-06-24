resource "google_storage_bucket" "raw_bucket" {
  name          = var.raw_bucket_name
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  labels = {
    layer = "raw"
    env   = "dev"
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_storage_bucket" "bronze_bucket" {
  name          = var.bronze_bucket_name
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  labels = {
    layer = "bronze"
    env   = "dev"
  }

  depends_on = [google_project_service.required_apis]
}
