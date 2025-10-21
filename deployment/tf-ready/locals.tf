locals {
  ws              = terraform.workspace
  bucket_composer = "the1-insight-${local.ws}-data-pipeline-composer"
  bucket_dataflow = "the1-insight-${local.ws}-data-pipeline-dataflow"
  artifact_repo   = "insight-dataflow-common"
  dataset_id      = "insight"
  table_id        = "personas"
}