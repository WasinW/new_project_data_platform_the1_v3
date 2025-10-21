resource "google_bigquery_data_transfer_config" "scheduled_query" {
  project       = "${var.domain}-${terraform.workspace}"
  location      = google_bigquery_dataset.insight.location
  display_name  = "insight_personas_sq"
  data_source_id = "scheduled_query"
  destination_dataset_id = google_bigquery_dataset.insight.dataset_id
  params = {
    query = file("${path.module}/sql/personas_scheduled.sql")
    destination_table_name_template = local.table_id
    write_disposition = "WRITE_APPEND"
  }
  schedule = "every 1 hours"
  depends_on = [
    google_bigquery_table.personas
  ]
}