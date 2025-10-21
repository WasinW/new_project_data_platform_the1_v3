# bigquery.tf
resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "insight"
  description                 = "Insight dataset for Personas"
  location                    = var.region

  labels = {
    team = "insight"
  }
}

resource "google_bigquery_dataset" "collector_dataset" {
  dataset_id                  = "collector"
  description                 = "Insight dataset for Collector"
  location                    = var.region

  labels = {
    team = "insight"
  }
}