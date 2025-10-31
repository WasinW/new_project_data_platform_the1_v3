# ============================================
# STORAGE BUCKETS - ใช้ module ของทีม
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

# ============================================
# ARTIFACT REGISTRY
# ============================================
module "insight_dataflow_common_repo" {
  source        = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/artifact-registry?ref=main"
  project_id    = "${var.domain}-${terraform.workspace}"
  location      = var.region
  repository_id = "insight-dataflow-common"
  description   = "Image repository for Insight Dataflow Common"
  format        = "DOCKER"
  immutable_tags = false
}

# ============================================
# BIGQUERY TABLE
# ============================================

# resource "google_bigquery_table" "stg_ms_member" {
#   project    = var.project_id
#   dataset_id = google_bigquery_dataset.insight.dataset_id
#   table_id   = "stg_ms_member"
#   schema     = file("${path.module}/schemas/stg_ms_member.json")
  
#   deletion_protection = var.environment == "prod"
# }

# BigQuery Data Transfer Service
resource "google_bigquery_data_transfer_config" "mapping_transfer" {
  project        = var.project_id
  location       = var.region
  data_source_id = "amazon_s3"
  display_name   = "mapping_reconcile_transfer"
  destination_dataset_id = "insight"
  
  params = {
    destination_table_name_template = "stg_mapping_reconcile"
    # var.s3_mapping_path
    data_path                      = var.s3_mapping_path
    access_key_id                  = var.aws_access_key_id
    secret_access_key              = var.aws_secret_access_key
    file_format                    = "PARQUET"
    max_bad_records               = "0"
    skip_leading_rows             = "1"
  }
  
  schedule = "every day 06:00"
  disabled = false
}

resource "google_bigquery_data_transfer_config" "member_transfer" {
  project                = "${var.domain}-${terraform.workspace}"
  location               = var.region
  data_source_id         = "amazon_s3"
  display_name           = "ms_member_transfer"
  destination_dataset_id = "insight"
  
  params = {
    destination_table_name_template = "stg_ms_member"
    data_path                      = "s3://t1-analytics/refined/insights/ms_member_dev/**" 
    access_key_id                  = var.aws_access_key_id
    secret_access_key              = var.aws_secret_access_key
    file_format                    = "PARQUET"
    max_bad_records               = "0"
    skip_leading_rows             = "1"
  }
  
  schedule = "every day 06:00"
  disabled = false
}



# resource "google_bigquery_dataset" "insight" {
#   project  = "${var.domain}-${terraform.workspace}"
#   dataset_id = "insight"
#   location   = "asia-southeast1"
#   delete_contents_on_destroy = true
#   labels = { team = "insight" }
# }

# resource "google_bigquery_table" "personas" {
#   project   = "${var.domain}-${terraform.workspace}"
#   dataset_id = google_bigquery_dataset.insight.dataset_id
#   table_id   = "personas"
#   schema     = file("${path.module}/schemas/personas.json")  # หรือ inline JSON ได้
#   deletion_protection = false
# }


# ----------------------------------------------------------------------------------------
# ============================================
# CLOUD COMPOSER
# ============================================
resource "google_composer_environment" "main" {
  name    = "t1-airflow-composer-${terraform.workspace}"
  region  = var.region
  project = "${var.domain}-${terraform.workspace}"
  
  config {
    # Node configuration
    node_config {
      network         = var.composer_network
      subnetwork      = var.composer_subnetwork
      service_account = google_service_account.dataflow_sa.email
      
      # Machine specs from the image
      machine_type = "n1-standard-1"  # 1 vCPU, 3.75 GB memory
      disk_size_gb = 100
    }
    
    # Software configuration
    software_config {
      image_version = "composer-3-airflow-2.10.5"
      
      # Python packages
      pypi_packages = {
        apache-airflow-providers-google = ">=10.3.0"
        pandas = ">=1.5.3"
        pyarrow = ">=14.0.0"
      }
      
      # Airflow configuration overrides
      airflow_config_overrides = {
        "webserver-expose_config" = "True"
        "core-dags_are_paused_at_creation" = "True"
        "core-max_active_runs_per_dag" = "1"
      }
      
      # Environment variables for Airflow (initial setup)
      env_variables = {
        GCP_PROJECT_ID = "${var.domain}-${terraform.workspace}"
        ENVIRONMENT    = terraform.workspace
        # GCP_PROJECT_ID = var.project_id
        # ENVIRONMENT = "dev"
      }
    }
    
    # Workloads configuration
    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      
      web_server {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
      }
      
      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 10
        min_count  = 1
        max_count  = 3
      }
      
      triggerer {
        count      = 1
        cpu        = 0.5
        memory_gb  = 1
      }
    }
    
    # Private IP configuration
    private_environment_config {
      enable_private_endpoint = false
      enable_privately_used_public_ips = true
    }
    
    # Database configuration
    database_config {
      machine_type = "db-n1-standard-2"
    }
    
    # Web server network access
    web_server_network_access_control {
      allowed_ip_range {
        value = "0.0.0.0/0"  # เปลี่ยนเป็น specific IP ranges ใน production
        description = "Allow all for development"
      }
    }
    
    # Maintenance window
    maintenance_window {
      start_time = "2025-01-01T00:00:00Z"
      end_time   = "2025-01-01T04:00:00Z"
      recurrence = "FREQ=WEEKLY;BYDAY=SAT"
    }
    
    # Data retention
    data_retention_config {
      task_logs_retention_storage_days = 60
    }
    
    # Resilience mode
    resilience_mode = "STANDARD"
  }
  
  # Storage bucket (DAGs location)
  storage_config {
    bucket = module.insight_data_pipeline_composer_bucket.name  # Reference module output
    # bucket = google_storage_bucket.composer_bucket.name
  }
  
  depends_on = [
    google_project_iam_member.dataflow_sa_composer
  ]
}

# Composer bucket
resource "google_storage_bucket" "composer_bucket" {
  name          = "t1-airflow-composer-bucket"
  location      = var.region
  force_destroy = false
  
  lifecycle_rule {
    condition {
      age = 60
    }
    action {
      type = "Delete"
    }
  }
}

# Service Account for Dataflow
resource "google_service_account" "dataflow_sa" {
  account_id   = "t1-ins-dev-sa-data"
  display_name = "Dataflow Service Account"
  description  = "Service account for Dataflow and Composer operations"
}

# IAM roles for service account
resource "google_project_iam_member" "dataflow_sa_composer" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_sa_dataflow" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_sa_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# Output Airflow URI
output "airflow_uri" {
  value = google_composer_environment.main.config[0].airflow_uri
  description = "Airflow web UI URL"
}

output "gcs_bucket" {
  value = google_storage_bucket.composer_bucket.name
  description = "GCS bucket for DAGs"
}
# ----------------------------------------------------------------------------------------
# ตรงนี้ผมจะได้ bucket : xxxx-data-pipeline-composer , xxxx-data-pipeline-dataflow
# ซึ่้งผม คิดไว้ว่า path ข้างในจะเป็นงี้ 
# 1. gs://xxxx-data-pipeline-composer/
#   - dags/
#     - dag_ms_member_short_term_init.py
#     - dag_ms_member_short_term.py
#   - config/
#     - ms_member_short_init.yaml
#     - ms_member_short.yaml
#   - packages/
#     - dataflow_common/dataflow_common-1.0.0-py3-none-any.whl (เดี๋ยวอันนี้ deploy ตามมา)
# 2. gs://xxxx-data-pipeline-dataflow/
#   - job/
#     - ms_member_short_term.py

# การที่ผมอยากจะทำต่อคือ หลังจาก provision bucket เสร็จแล้ว 
# ผมอยาก
# 0. provision bucket 
# 1. provision directory and scripts  โครงสร้างข้างบนในแต่ละ bucket
# 2. provision table in bq 
# 3. provision service bq data transfer service
# 4. upload scripts files to bucket ตามโครงสร้างข้างบน whl ยังไม่ต้อง upload ตอนนี้ ผมอยาก build จาก python dataflow_common/setup.py bdist_wheel แล้วเอามา upload ทีหลัง ด้วย tf 
# 5. build dataflow_common จาก python dataflow_common/setup.py bdist_wheel
# 6. upload dataflow_common-1.0.0-py3-none-any.whl to gs://xxxx-data-pipeline-composer/packages/dataflow_common/
# 7. build docker image from dataflow_common and push to artifact registry created from module "insight_dataflow_common_repo"
# 8. provision composer airflow and variables
# ----------------------------------------------------------------------------------------
# terraform : (deploy ผ่าน pipeline/terraform.gitlab-ci.yml )
#   - provision bucket
#   - provision artifact registry
#   - create bq dataset 
#   - provision service bq data transfer service
#   # - provision bq real table (ms_personas)

# gitlab_ci: (deploy ผ่าน pipeline/data_pipeline.gitlab-ci.yml (ทำเอง))
#   - create sub directories in bucket
#   - copy scripts to bucket
#   - create bq table (using in short term - mid term)
#   - build dataflow_common wheel and docker image and upload to bucket +
#   - build docker image and push to artifact registry
# ----------------------------------------------------------------------------------------
