# ============================================
# CLOUD COMPOSER ENVIRONMENT
# ============================================

resource "google_composer_environment" "main" {
  name    = "t1-airflow-composer-${terraform.workspace}"
  region  = var.region
  project = "${var.domain}-${terraform.workspace}"
  
  config {
    node_config {
      network         = var.composer_network
      subnetwork      = var.composer_subnetwork
      service_account = var.dataflow_service_account
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