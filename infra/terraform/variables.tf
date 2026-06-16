variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "region" {
  type        = string
  description = "GCP Region"
  default     = "us-central1"
}

variable "dataset_name" {
  type        = string
  description = "BigQuery dataset name"
  default     = "dropshipping"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
  default     = "dev"
}
