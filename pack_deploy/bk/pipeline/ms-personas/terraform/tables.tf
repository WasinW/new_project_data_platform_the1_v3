# ============================================
# BIGQUERY TABLES
# ============================================

# Staging table for mapping reconcile
resource "google_bigquery_table" "stg_mapping_reconcile" {
  project    = "${var.domain}-${terraform.workspace}"
  dataset_id = var.bigquery_dataset_id
  table_id   = "stg_mapping_reconcile"
  
  # Load schema from file
  schema = file("${path.module}/schemas/stg_mapping_reconcile.json")
  
  time_partitioning {
    type  = "DAY"
    field = "UPDATED_DATE"
  }
  
  deletion_protection = terraform.workspace == "prod" ? true : false
}

# Staging table for MS member (127 columns)
resource "google_bigquery_table" "stg_ms_member" {
  project    = "${var.domain}-${terraform.workspace}"
  dataset_id = var.bigquery_dataset_id
  table_id   = "stg_ms_member"
  
  # Load schema from external file (127 columns)
  schema = file("${path.module}/schemas/stg_ms_member.json")
  
  time_partitioning {
    type  = "DAY"
    field = "updated_date"
  }
  
  clustering = ["member_number"]
  
  deletion_protection = terraform.workspace == "prod" ? true : false
}

# Temp table for processing
resource "google_bigquery_table" "stg_ms_member_temp" {
  project    = "${var.domain}-${terraform.workspace}"
  dataset_id = var.bigquery_dataset_id
  table_id   = "stg_ms_member_temp"
  
  # Use same schema as main table
  schema = file("${path.module}/schemas/stg_ms_member.json")
  
  # Auto-expire temp tables in non-prod
  expiration_time = terraform.workspace != "prod" ? 
    timeadd(timestamp(), "168h") : null  # 7 days
  
  deletion_protection = false
}