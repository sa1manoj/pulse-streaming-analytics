# GCP resources for Pulse: a GCS bucket for checkpoints/exports, BigQuery
# datasets for raw and modelled data, and a least-privilege service account.
#
#   terraform init
#   terraform apply -var="project_id=<your-project>"
#   ...
#   terraform destroy -var="project_id=<your-project>"   # when finished, to stop charges
#
# No service-account key is created. Run the pipeline with your own ADC
# (`gcloud auth application-default login`) impersonating the service account.

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "bq_location" {
  type    = string
  default = "US"
}

variable "checkpoint_retention_days" {
  type    = number
  default = 30
}

resource "google_project_service" "apis" {
  for_each           = toset(["bigquery.googleapis.com", "storage.googleapis.com", "iam.googleapis.com"])
  service            = each.value
  disable_on_destroy = false
}

resource "google_storage_bucket" "pulse" {
  name                        = "${var.project_id}-pulse"
  location                    = var.bq_location
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true

  lifecycle_rule {
    condition {
      age = var.checkpoint_retention_days
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "pulse_raw"
  location                   = var.bq_location
  description                = "Events and minute metrics landed by the Spark stream job"
  delete_contents_on_destroy = true
  depends_on                 = [google_project_service.apis]
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id                 = "pulse_analytics"
  location                   = var.bq_location
  description                = "dbt models: sessions, daily funnel, A/B summary"
  delete_contents_on_destroy = true
  depends_on                 = [google_project_service.apis]
}

resource "google_service_account" "pipeline" {
  account_id   = "pulse-pipeline"
  display_name = "Pulse streaming + dbt pipeline"
  depends_on   = [google_project_service.apis]
}

# Least privilege: write access only to the two datasets and the bucket,
# plus permission to run query/load jobs in the project.
resource "google_bigquery_dataset_iam_member" "raw_editor" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "analytics_editor" {
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "bucket_rw" {
  bucket = google_storage_bucket.pulse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

output "bucket" {
  value = google_storage_bucket.pulse.url
}

output "service_account" {
  value = google_service_account.pipeline.email
}

output "datasets" {
  value = [google_bigquery_dataset.raw.dataset_id, google_bigquery_dataset.analytics.dataset_id]
}
