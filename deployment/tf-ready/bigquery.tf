resource "google_bigquery_dataset" "insight" {
  dataset_id = local.dataset_id
  project    = "${var.domain}-${terraform.workspace}"
  location   = var.region
  delete_contents_on_destroy = true
  labels = {
    team = "insight"
  }
}

resource "google_bigquery_table" "personas" {
  project    = "${var.domain}-${terraform.workspace}"
  dataset_id = google_bigquery_dataset.insight.dataset_id
  table_id   = local.table_id
  schema     = file("${path.module}/schemas/personas.json")
  deletion_protection = false
}