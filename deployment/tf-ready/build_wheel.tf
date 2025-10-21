# Build the dataflow_common Python package into a wheel and upload it to GCS
resource "null_resource" "build_dataflow_common" {
  # Change version here to force rebuild
  triggers = {
    version = "1.0.0"
  }
  provisioner "local-exec" {
    command = <<-EOT
      set -e
      cd ${path.module}/dataflow_common
      python -m pip install --upgrade pip setuptools wheel build
      python -m build --wheel
    EOT
  }
}

resource "google_storage_bucket_object" "upload_wheel" {
  name   = "packages/dataflow_common/dataflow_common-1.0.0-py3-none-any.whl"
  bucket = local.bucket_composer
  source = "${path.module}/dataflow_common/dist/dataflow_common-1.0.0-py3-none-any.whl"
  depends_on = [
    null_resource.build_dataflow_common,
    module.insight_data_pipeline_composer_bucket
  ]
}