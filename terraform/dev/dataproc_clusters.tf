resource "google_dataproc_cluster" "instacart_cluster" {
  name   = "${var.dataproc_cluster_name}-${random_id.bucket_suffix.hex}"
  region = var.region

  cluster_config {
    staging_bucket = google_storage_bucket.dataproc_staging.name


    gce_cluster_config {
      zone            = "${var.region}-b"
      service_account = google_service_account.dataproc_etl.email
    }

    master_config {
      num_instances = 1
      machine_type  = "n2-standard-4"

      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 100
      }
    }

    worker_config {
      num_instances = 2
      machine_type  = "n2-standard-4"

      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 250
      }
    }

    software_config {
      image_version = "2.2-debian12"

      override_properties = {
        "spark:spark.sql.shuffle.partitions" = "64"
        "spark:spark.sql.adaptive.enabled"   = "true"
      }
    }
  }
}