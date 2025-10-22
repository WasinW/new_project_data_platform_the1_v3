<!-- # ----------------------------------------------------------------------------------------
# terraform : (deploy ผ่าน pipeline/terraform.gitlab-ci.yml )
#   - provision bucket
#   - provision artifact registry
#   - provision service bq data transfer service
#   # - create bq dataset 
#   # - provision bq real table (ms_personas)

# gitlab_ci: (deploy ผ่าน pipeline/data_pipeline.gitlab-ci.yml (ทำเอง))
#   - create sub directories in bucket
#   - copy scripts to bucket
#   - create bq table (using in short term - mid term)
#   - build dataflow_common wheel and docker image and upload to bucket +
#   - build docker image and push to artifact registry
# ---------------------------------------------------------------------------------------- -->

1. gitlab
2. pipeline/data_pipeline.yaml
3. infrastructure/data_pipeline/*.tf

1. terraform : 
    - bucket : using "modules/gcs-bucket"
    - artifact registry : using "modules/artifact-registry"
    - service bq data transfer service : using "google_bigquery_data_transfer_config"
    <!-- - bq dataset : using "google_bigquery_table" -->
2. gitlab cli :
    - create