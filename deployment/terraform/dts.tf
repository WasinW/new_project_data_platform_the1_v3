# ============================================
# BIGQUERY DATA TRANSFER SERVICE
# Must run AFTER tables are created
# ============================================

resource "google_bigquery_data_transfer_config" "mapping_transfer" {
  project                = "${var.domain}-${terraform.workspace}"
  location               = var.region
  data_source_id         = "amazon_s3"
  display_name           = "mapping_reconcile_transfer"
  destination_dataset_id = var.bigquery_dataset_id
  
  params = {
    destination_table_name_template = google_bigquery_table.stg_mapping_reconcile.table_id
    data_path                      = var.s3_mapping_path
    access_key_id                  = var.aws_access_key_id
    secret_access_key              = var.aws_secret_access_key
    file_format                    = "PARQUET"
    max_bad_records               = "0"
    skip_leading_rows             = "1"
    write_disposition             = "WRITE_TRUNCATE"  # Overwrite data
  }
  
  schedule = "every day 06:00"
  disabled = false
  
  # IMPORTANT: Wait for table to be created first
  depends_on = [
    google_bigquery_table.stg_mapping_reconcile
  ]
}

resource "google_bigquery_data_transfer_config" "member_transfer" {
  project                = "${var.domain}-${terraform.workspace}"
  location               = var.region
  data_source_id         = "amazon_s3"
  display_name           = "ms_member_transfer"
  destination_dataset_id = var.bigquery_dataset_id
  
  params = {
    destination_table_name_template = google_bigquery_table.stg_ms_member.table_id
    data_path                      = var.s3_member_path
    access_key_id                  = var.aws_access_key_id
    secret_access_key              = var.aws_secret_access_key
    file_format                    = "PARQUET"
    max_bad_records               = "0"
    skip_leading_rows             = "1"
    write_disposition             = "WRITE_TRUNCATE"  # Overwrite data
  }
  
  schedule = "every day 06:00"
  disabled = false
  
  # IMPORTANT: Wait for table to be created first
  depends_on = [
    google_bigquery_table.stg_ms_member
  ]
}