variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region (e.g. asia-southeast1)"
  type        = string
  default     = "asia-southeast1"
}

variable "domain" {
  description = "Domain prefix used for resource names"
  type        = string
}

variable "airflow_env_vars" {
  description = "Environment variables to set for the Airflow environment"
  type        = map(string)
  default     = {}
}