resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "google_storage_bucket" "raw_bucket" {
  name          = "${var.raw_bucket_name}-${random_id.bucket_suffix.hex}"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  labels = {
    layer = "raw_data"
    env   = "dev"
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_storage_bucket" "bronze_bucket" {
  name          = "${var.bronze_bucket_name}-${random_id.bucket_suffix.hex}"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  labels = {
    layer = "bronze_data"
    env   = "dev"
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_storage_bucket" "silver_bucket" {
  name          = "${var.silver_bucket_name}-${random_id.bucket_suffix.hex}"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  labels = {
    layer = "silver_data"
    env   = "dev"
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_storage_bucket" "gold_bucket" {
  name          = "${var.gold_bucket_name}-${random_id.bucket_suffix.hex}"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  labels = {
    layer = "gold_data"
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
