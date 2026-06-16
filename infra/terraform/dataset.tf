resource "google_bigquery_dataset" "main" {
  dataset_id = var.dataset_name
  location   = var.region
  project    = var.project_id

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    google_project_service.bigquery
  ]
}

resource "google_bigquery_table" "staging_products" {
  dataset_id  = google_bigquery_dataset.main.dataset_id
  table_id    = "staging_products"
  description = "Productos en staging — datos crudos post-validación"
  schema      = file("${path.module}/bigquery/schemas/products.json")
}

resource "google_bigquery_table" "final_products" {
  dataset_id  = google_bigquery_dataset.main.dataset_id
  table_id    = "final_products"
  description = "Productos finales — datos validados y transformados"
  schema      = file("${path.module}/bigquery/schemas/products.json")
}

resource "google_bigquery_table" "enriched_products" {
  dataset_id  = google_bigquery_dataset.main.dataset_id
  table_id    = "enriched_products"
  description = "Productos enriquecidos — categorización AI con Gemini"
  schema      = file("${path.module}/bigquery/schemas/enriched.json")
}

resource "google_bigquery_table" "price_history" {
  dataset_id           = google_bigquery_dataset.main.dataset_id
  table_id              = "price_history"
  description          = "Histórico de precios — monitoreo en tiempo real"
  schema                = file("${path.module}/bigquery/schemas/price_history.json")
}
