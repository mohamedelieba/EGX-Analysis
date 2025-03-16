terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = "alexy-de-bootcamp"
  region  = "us-central1"
  credentials = file("~/.gc/alexy-de-bootcamp.json")
}

resource "google_storage_bucket" "data-lake-bucket" {
  name     = "project-egx-bucket"
  location = "US"

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 15 // days
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "terra_demo_dataset" {
  dataset_id = "EGX_dataset"
  project    = "alexy-de-bootcamp"
  location   = "US"
}