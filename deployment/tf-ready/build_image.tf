# Build a Docker image from dataflow_common and push to Artifact Registry
resource "null_resource" "build_docker_image" {
  triggers = {
    version = "1.0.0"
  }
  provisioner "local-exec" {
    command = <<-EOT
      set -e
      REGION=${var.region}
      PROJECT=${var.domain}-${terraform.workspace}
      REPO=${local.artifact_repo}
      IMAGE_TAG=1.0.0

      # Authenticate Docker with Artifact Registry
      gcloud auth configure-docker ${REGION}-docker.pkg.dev -q
      # Build and push image
      docker build -t ${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/dataflow-common:${IMAGE_TAG} ${path.module}/dataflow_common
      docker push ${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/dataflow-common:${IMAGE_TAG}
    EOT
  }
  depends_on = [
    module.insight_dataflow_common_repo,
    null_resource.build_dataflow_common
  ]
}