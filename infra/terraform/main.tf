# APIs
resource "google_project_service" "storage" {
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigquery" {
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudfunctions" {
  service            = "cloudfunctions.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "pubsub" {
  service            = "pubsub.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "eventarc" {
  service            = "eventarc.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "logging" {
  service            = "logging.googleapis.com"
  disable_on_destroy = false
}

# Buckets
resource "google_storage_bucket" "raw_products" {
  name                        = "${var.project_id}-raw-products"
  location                    = var.region
  uniform_bucket_level_access = true

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    google_project_service.storage
  ]
}

resource "google_storage_bucket" "failed_products" {
  name                        = "${var.project_id}-failed-products"
  location                    = var.region
  uniform_bucket_level_access = true

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    google_project_service.storage
  ]
}

# Pub/Sub Topic
resource "google_pubsub_topic" "price_updates" {
  name = "price-updates"

  depends_on = [
    google_project_service.pubsub
  ]
}
