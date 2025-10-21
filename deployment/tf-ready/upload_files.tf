locals {
  composer_files = {
    "dags/dag_ms_member_short_term_init.py" = "${path.module}/files/dags/dag_ms_member_short_term_init.py"
    "dags/dag_ms_member_short_term.py"      = "${path.module}/files/dags/dag_ms_member_short_term.py"
    "config/ms_member_short_term_init.yaml" = "${path.module}/files/config/ms_member_short_term_init.yaml"
    "config/ms_member_short_term.yaml"      = "${path.module}/files/config/ms_member_short_term.yaml"
  }
  dataflow_files = {
    "job/ms_member_short_term.py" = "${path.module}/files/job/ms_member_short_term.py"
  }
}

resource "google_storage_bucket_object" "composer_files" {
  for_each = local.composer_files
  bucket   = local.bucket_composer
  name     = each.key
  source   = each.value
  depends_on = [
    module.insight_data_pipeline_composer_bucket
  ]
}

resource "google_storage_bucket_object" "dataflow_files" {
  for_each = local.dataflow_files
  bucket   = local.bucket_dataflow
  name     = each.key
  source   = each.value
  depends_on = [
    module.insight_data_pipeline_dataflow_bucket
  ]
}