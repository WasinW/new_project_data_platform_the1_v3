# ============================================
# ARTIFACT REGISTRY
# ============================================

module "insight_dataflow_common_repo" {
  source         = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/artifact-registry?ref=main"
  project_id     = "${var.domain}-${terraform.workspace}"
  location       = var.region
  repository_id  = "insight-dataflow-common"
  description    = "Image repository for Insight Dataflow Common"
  format         = "DOCKER"
  immutable_tags = false
}