variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "data_bucket_name" {
  type = string
}

variable "dataproc_staging_bucket_name" {
  type = string
}

variable "curated_bucket_name" {
  type = string
}