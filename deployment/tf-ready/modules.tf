module "insight_data_pipeline_composer_bucket" {
  source  = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/gcs-bucket?ref=main"
  project = "${var.domain}-${terraform.workspace}"
  name    = local.bucket_composer
}

module "insight_data_pipeline_dataflow_bucket" {
  source  = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/gcs-bucket?ref=main"
  project = "${var.domain}-${terraform.workspace}"
  name    = local.bucket_dataflow
}

module "insight_dataflow_common_repo" {
  source        = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/artifact-registry?ref=main"
  project_id    = "${var.domain}-${terraform.workspace}"
  location      = var.region
  repository_id = local.artifact_repo
  description   = "Image repository for Insight Dataflow Common"
  format        = "DOCKER"
  immutable_tags = false
}