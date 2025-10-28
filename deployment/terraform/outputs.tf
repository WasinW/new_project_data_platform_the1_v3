# ============================================
# OUTPUTS - ใช้สำหรับ GitLab CI ดึงค่าไป set variables
# ============================================

# Buckets
output "composer_bucket" {
  value       = module.insight_data_pipeline_composer_bucket.name
  description = "Composer bucket name"
}

output "dataflow_bucket" {
  value       = module.insight_data_pipeline_dataflow_bucket.name
  description = "Dataflow bucket name"
}

# Artifact Registry
output "artifact_registry_url" {
  value       = "${var.region}-docker.pkg.dev/${var.domain}-${terraform.workspace}/insight-dataflow-common"
  description = "Artifact Registry URL"
}

# DTS Config IDs - สำคัญ! ต้องเอาไป set ใน Composer
output "mapping_transfer_config_id" {
  value       = google_bigquery_data_transfer_config.mapping_transfer.name
  description = "Mapping transfer config ID"
}

output "member_transfer_config_id" {
  value       = google_bigquery_data_transfer_config.member_transfer.name
  description = "Member transfer config ID"
}

# Composer
output "composer_env_name" {
  value       = google_composer_environment.main.name
  description = "Composer environment name"
}

# BigQuery Tables
output "bigquery_tables" {
  value = {
    stg_mapping_reconcile = google_bigquery_table.stg_mapping_reconcile.id
    stg_ms_member        = google_bigquery_table.stg_ms_member.id
    stg_ms_member_temp   = google_bigquery_table.stg_ms_member_temp.id
  }
  description = "Created BigQuery tables"
}