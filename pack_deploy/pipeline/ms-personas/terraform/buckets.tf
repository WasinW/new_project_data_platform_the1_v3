# ============================================
# STORAGE BUCKETS
# ============================================

module "insight_data_pipeline_composer_bucket" {
  source  = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/gcs-bucket?ref=main"
  project = "${var.domain}-${terraform.workspace}"
  name    = "the1-insight-${terraform.workspace}-data-pipeline-composer"
}

module "insight_data_pipeline_dataflow_bucket" {
  source  = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/gcs-bucket?ref=main"
  project = "${var.domain}-${terraform.workspace}"
  name    = "the1-insight-${terraform.workspace}-data-pipeline-dataflow"
}