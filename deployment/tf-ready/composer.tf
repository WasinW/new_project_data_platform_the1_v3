resource "google_composer_environment" "env" {
  project = "${var.domain}-${terraform.workspace}"
  name    = "cmp2-${terraform.workspace}"
  region  = var.region

  config {
    software_config {
      image_version = "composer-2.7.4-airflow-2.9.1"
      env_variables = {
        # Example environment variables; add your own as needed
        PROJECT_ID = "${var.domain}-${terraform.workspace}"
      }
    }
    environment_size = "ENVIRONMENT_SIZE_SMALL"
  }
}

# Optionally set Airflow variables via gcloud (edit as needed)
resource "null_resource" "airflow_variables" {
  provisioner "local-exec" {
    command = <<-EOT
      # Example: set Airflow variables for ms_member_short_term
      gcloud composer environments run cmp2-${terraform.workspace} --location ${var.region} variables -- --set ms_member_source personas --set run_mode short_term
    EOT
  }
  depends_on = [google_composer_environment.env]
}