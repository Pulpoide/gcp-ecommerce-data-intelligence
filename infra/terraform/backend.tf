terraform {
  backend "gcs" {
    bucket = "project-b8b7f8b6-4ca7-4b6e-96b-terraform-state"
    prefix = "gcp-ecommerce-data-intelligence"
  }
}
