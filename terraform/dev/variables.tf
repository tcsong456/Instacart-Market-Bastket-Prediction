variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "raw_bucket_name" {
  type = string
}

variable "dataproc_staging_bucket_name" {
  type = string
}

variable "bronze_bucket_name" {
  type = string
}

variable "silver_bucket_name" {
  type = string
}

variable "gold_bucket_name" {
  type = string
}

variable "dataproc_cluster_name" {
  type = string
}