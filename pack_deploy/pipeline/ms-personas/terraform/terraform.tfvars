# Existing resources
bigquery_dataset_id      = "insight"
dataflow_service_account = "t1-ins-dev-sa-data@the1-insight-${terraform.workspace}.iam.gserviceaccount.com"
composer_network         = "projects/the1-network-prod/global/networks/dataflow"
composer_subnetwork      = "projects/the1-network-prod/regions/asia-southeast1/subnetworks/dataflow-private"

# S3 paths for production
s3_mapping_path = "s3://t1-analytics/refined/insights/mapping_reconcile_prod/ms_personas/**"
s3_member_path  = "s3://t1-analytics/refined/insights/ms_member_prod/**"

# AWS credentials
aws_access_key_id     = "AKIARGXU4IOJUDHOY4WQ"
aws_secret_access_key = "YOUR_SECRET_KEY"

# Override defaults if needed
s3_mapping_path = "s3://t1-analytics/refined/insights/mapping_reconcile_${terraform.workspace}/ms_personas/**"
s3_member_path  = "s3://t1-analytics/refined/insights/ms_member_${terraform.workspace}/**"

# Access control (ควร restrict ใน prod)
allowed_ip_ranges = "10.0.0.0/8"  # หรือ specific IPs