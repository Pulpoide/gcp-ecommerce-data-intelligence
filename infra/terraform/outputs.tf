output "bucket_raw_url" {
  description = "URL of the raw products bucket"
  value       = google_storage_bucket.raw_products.url
}

output "bucket_failed_url" {
  description = "URL of the failed products bucket"
  value       = google_storage_bucket.failed_products.url
}

output "dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.main.dataset_id
}

output "pubsub_topic_name" {
  description = "Pub/Sub topic name"
  value       = google_pubsub_topic.price_updates.name
}
