# Existing resources
bigquery_dataset_id      = "insight"
dataflow_service_account = "t1-ins-dev-sa-data@the1-insight-${terraform.workspace}.iam.gserviceaccount.com"

# AWS credentials
aws_access_key_id     = "AKIARGXU4IOJUDHOY4WQ"
aws_secret_access_key = "YOUR_SECRET_KEY"

# Override defaults if needed
s3_mapping_path = "s3://t1-analytics/refined/insights/mapping_reconcile_${terraform.workspace}/ms_personas/**"
s3_member_path  = "s3://t1-analytics/refined/insights/ms_member_${terraform.workspace}/**"