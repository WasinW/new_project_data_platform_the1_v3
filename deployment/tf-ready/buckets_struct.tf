# Create zero-byte objects to simulate folder structure in GCS
resource "google_storage_bucket_object" "composer_prefixes" {
  for_each = toset([
    "dags/.keep",
    "config/.keep",
    "packages/dataflow_common/.keep"
  ])
  bucket  = local.bucket_composer
  name    = each.key
  content = ""
  depends_on = [module.insight_data_pipeline_composer_bucket]
}

resource "google_storage_bucket_object" "dataflow_prefixes" {
  for_each = toset([
    "job/.keep"
  ])
  bucket  = local.bucket_dataflow
  name    = each.key
  content = ""
  depends_on = [module.insight_data_pipeline_dataflow_bucket]
}