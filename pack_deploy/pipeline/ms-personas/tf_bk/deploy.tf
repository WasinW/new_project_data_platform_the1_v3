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
  source         = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/artifact-registry?ref=main"
  project_id     = "${var.domain}-${terraform.workspace}"
  location       = var.region
  repository_id  = "insight-dataflow-common"
  description    = "Image repository for Insight Dataflow Common"
  format         = "DOCKER"
  immutable_tags = false
}

# ============================================
# BIGQUERY TABLES (สร้างก่อน DTS)
# ============================================

# Staging table for mapping reconcile
resource "google_bigquery_table" "stg_mapping_reconcile" {
  project    = "${var.domain}-${terraform.workspace}"
  dataset_id = var.bigquery_dataset_id
  table_id   = "stg_mapping_reconcile"
  
  # Time partitioning (optional)
  time_partitioning {
    type  = "DAY"
    field = "UPDATED_DATE"
  }
  
  # Schema definition
  schema = jsonencode([
    {
      name = "RECONCILE_COLUMN_NAME"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "RECONCILE_SRC_COLUMN_NAME"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "RECONCILE_RETRIEVED"
      type = "BOOLEAN"
      mode = "NULLABLE"
    },
    {
      name = "RECONCILE_ORIGINAL"
      type = "BOOLEAN"
      mode = "NULLABLE"
    },
    {
      name = "UPDATED_DATE"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "CREATED_DATE"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    }
  ])
  
  deletion_protection = terraform.workspace == "prod" ? true : false
}

# ============================================
# BIGQUERY DATA TRANSFER SERVICE
# ใช้ existing dataset ผ่าน variable
# ============================================
resource "google_bigquery_data_transfer_config" "mapping_transfer" {
  project                = "${var.domain}-${terraform.workspace}"
  location               = var.region
  data_source_id         = "amazon_s3"
  display_name           = "mapping_reconcile_transfer"
  destination_dataset_id = var.bigquery_dataset_id  # ใช้ variable แทน
  
  params = {
    destination_table_name_template = "stg_mapping_reconcile"
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
  destination_dataset_id = var.bigquery_dataset_id  # ใช้ variable แทน
  
  params = {
    destination_table_name_template = "stg_ms_member"
    data_path                      = var.s3_member_path
    access_key_id                  = var.aws_access_key_id
    secret_access_key              = var.aws_secret_access_key
    file_format                    = "PARQUET"
    max_bad_records               = "0"
    skip_leading_rows             = "1"
  }
  
  schedule = "every day 06:00"
  disabled = false
}

# ============================================
# CLOUD COMPOSER
# ใช้ existing service account ผ่าน variable
# ============================================
resource "google_composer_environment" "main" {
  name    = "t1-airflow-composer-${terraform.workspace}"
  region  = var.region
  project = "${var.domain}-${terraform.workspace}"
  
  config {
    node_config {
      network         = var.composer_network
      subnetwork      = var.composer_subnetwork
      service_account = var.dataflow_service_account  # ใช้ existing SA
      machine_type    = "n1-standard-1"
      disk_size_gb    = 100
    }
    
    software_config {
      image_version = "composer-3-airflow-2.10.5-build.15"
      
      pypi_packages = {
        apache-airflow-providers-google = ">=10.3.0"
        pandas                          = ">=1.5.3"
        pyarrow                         = ">=14.0.0"
      }
      
      airflow_config_overrides = {
        "webserver-expose_config"           = "True"
        "core-dags_are_paused_at_creation" = "True"
        "core-max_active_runs_per_dag"     = "1"
      }
      
      env_variables = {
        GCP_PROJECT_ID = "${var.domain}-${terraform.workspace}"
        ENVIRONMENT    = terraform.workspace
      }
    }
    
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
        count     = 1
        cpu       = 0.5
        memory_gb = 1
      }
    }
    
    private_environment_config {
      enable_private_endpoint           = true
      enable_privately_used_public_ips = false
    }
    
    database_config {
      machine_type = "db-n1-standard-2"
    }
    
    web_server_network_access_control {
      allowed_ip_range {
        value       = var.allowed_ip_ranges
        description = "Allowed IP ranges"
      }
    }
    
    maintenance_window {
      start_time = "2025-01-25T00:00:00Z"
      end_time   = "2025-01-25T04:00:00Z"
      recurrence = "FREQ=WEEKLY;BYDAY=SAT"
    }
    
    data_retention_config {
      task_logs_retention_storage_days = 60
    }
    
    resilience_mode = "STANDARD"
  }
  
  storage_config {
    bucket = module.insight_data_pipeline_composer_bucket.name
  }
}
