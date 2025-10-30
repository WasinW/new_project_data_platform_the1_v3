module "secret-manager" {
  source  = "GoogleCloudPlatform/secret-manager/google"
  version = "~> 0.9"
  project_id = var.project_id
  secrets = [
    {
      name = "data-pipeline-aws-access-key"
      # ไม่ใส่ secret_data - จะ add manual ทีหลัง
    },
    {
      name = "data-pipeline-aws-secret-key"
      # ไม่ใส่ secret_data - จะ add manual ทีหลัง
    }
  ]
}

# data_pipeline_aws_access_key_secret_name
# data_pipeline_aws_secret_key_secret_name