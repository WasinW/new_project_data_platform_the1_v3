# Deployment Guide

## Prerequisites

### Required Google Cloud APIs

Enable the following APIs in your GCP project:

```bash
gcloud services enable \
  compute.googleapis.com \
  dataflow.googleapis.com \
  bigquery.googleapis.com \
  bigquerydatatransfer.googleapis.com \
  pubsub.googleapis.com \
  bigtable.googleapis.com \
  storage.googleapis.com \
  composer.googleapis.com \
  secretmanager.googleapis.com \
  datacatalog.googleapis.com
```

### Service Account Setup

Create a service account with required permissions:

```bash
# Create service account
gcloud iam service-accounts create sa-dataflow-pipeline \
  --display-name="Dataflow Pipeline Service Account"

# Grant required roles
PROJECT_ID=your-project-id
SA_EMAIL=sa-dataflow-pipeline@${PROJECT_ID}.iam.gserviceaccount.com

# BigQuery roles
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.jobUser"

# Dataflow roles
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/dataflow.worker"

# Pub/Sub roles
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.editor"

# Storage roles
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.objectAdmin"

# Bigtable roles (if using)
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigtable.reader"
```

### Storage Setup

Create required storage buckets:

```bash
# Create buckets
gsutil mb -p ${PROJECT_ID} -l asia-southeast1 gs://t1-airflow-composer-bucket
gsutil mb -p ${PROJECT_ID} -l asia-southeast1 gs://t1-dataflow-framework-bucket
gsutil mb -p ${PROJECT_ID} -l asia-southeast1 gs://t1-insight-audit-bucket

# Set bucket permissions
gsutil iam ch serviceAccount:${SA_EMAIL}:objectAdmin gs://t1-dataflow-framework-bucket
gsutil iam ch serviceAccount:${SA_EMAIL}:objectAdmin gs://t1-insight-audit-bucket
```

### BigQuery Setup

Create required datasets:

```bash
# Create datasets
bq mk --location=asia-southeast1 --dataset ${PROJECT_ID}:insight
bq mk --location=asia-southeast1 --dataset ${PROJECT_ID}:insight_dev

# Create audit tables
bq query --use_legacy_sql=false < sql/create_audit_tables.sql
```

### Pub/Sub Setup (for streaming)

```bash
# Create topics
gcloud pubsub topics create personas-updates-dev
gcloud pubsub topics create personas-updates-staging
gcloud pubsub topics create personas-updates-prod

# Create subscriptions
gcloud pubsub subscriptions create personas-updates-sub-dev \
  --topic=personas-updates-dev

gcloud pubsub subscriptions create personas-updates-sub-staging \
  --topic=personas-updates-staging

gcloud pubsub subscriptions create personas-updates-sub-prod \
  --topic=personas-updates-prod
```

### Bigtable Setup (optional, for streaming enrichment)

```bash
# Create instance
gcloud bigtable instances create member-cache \
  --cluster=member-cache-c1 \
  --cluster-zone=asia-southeast1-a \
  --display-name="Member Cache" \
  --instance-type=PRODUCTION

# Create table
cbt -instance=member-cache createtable member_data

# Create column family
cbt -instance=member-cache createfamily member_data cf
```

## Deployment Steps

### 1. Prepare Configuration Files

Update configuration files for your environment:

```bash
# Edit configuration files
vi composer/config/ms_member/batch/short_term_hourly.yaml
vi composer/config/ms_member/streaming/streaming_realtime.yaml
vi composer/config/ms_member/reconcile/full_patch_config.yaml
vi composer/config/ms_member/init/pl_init_config.yaml

# Update project_id, datasets, buckets, etc.
```

### 2. Deploy Configuration Files

```bash
# Upload configuration to Composer bucket
gsutil -m cp -r composer/config gs://${COMPOSER_BUCKET}/dags/composer/

# Verify upload
gsutil ls -r gs://${COMPOSER_BUCKET}/dags/composer/config/
```

### 3. Deploy Dataflow Code

```bash
# Package common modules
cd dataflow
tar -czf dataflow_common.tar.gz dataflow_common/
gsutil cp dataflow_common.tar.gz gs://t1-dataflow-framework-bucket/packages/

# Upload pipeline files
gsutil cp FW/unified_dataflow_pipeline_bigtable.py \
  gs://t1-dataflow-framework-bucket/framework/

# Upload other Dataflow scripts
gsutil cp FW/dataflow_data_quality.py \
  gs://t1-dataflow-framework-bucket/framework/

gsutil cp FW/dataflow_reconciled.py \
  gs://t1-dataflow-framework-bucket/framework/
```

### 4. Deploy Airflow DAGs

```bash
# Upload DAG files to Composer
gsutil cp composer/dags/*.py gs://${COMPOSER_BUCKET}/dags/

# Set environment variables in Composer
gcloud composer environments update your-composer-env \
  --location=asia-southeast1 \
  --update-env-variables \
    SHORT_TERM_CONFIG=/home/airflow/gcs/dags/composer/config/ms_member/batch/short_term_hourly.yaml,\
    STREAMING_CONFIG=/home/airflow/gcs/dags/composer/config/ms_member/streaming/streaming_realtime.yaml,\
    RECONCILE_CONFIG=/home/airflow/gcs/dags/composer/config/ms_member/reconcile/full_patch_config.yaml,\
    INIT_CONFIG=/home/airflow/gcs/dags/composer/config/ms_member/init/pl_init_config.yaml
```

### 5. Create Storage Transfer Service Jobs

```bash
# Create STS job for member data
gcloud transfer jobs create \
  s3://your-source-bucket/ms_member/ \
  gs://demo-central-the1-staging/initiate/ms_member/ \
  --name="ms-member-transfer" \
  --schedule-starts="2024-01-01T00:00:00Z" \
  --schedule-repeats-every="P1D"

# Note the job ID for configuration
```

### 6. Initialize Data (First Run)

```bash
# Trigger initialization DAG
gcloud composer environments run your-composer-env \
  --location=asia-southeast1 \
  dags trigger -- initiate_pipeline_airflow_only
```

### 7. Start Batch Processing

```bash
# Enable short-term hourly DAG
gcloud composer environments run your-composer-env \
  --location=asia-southeast1 \
  dags unpause -- ms_member_short_term_hourly
```

### 8. Start Streaming Processing (Optional)

```bash
# Set Airflow variables for streaming
gcloud composer environments run your-composer-env \
  --location=asia-southeast1 \
  variables set -- TERM_TYPE mid

gcloud composer environments run your-composer-env \
  --location=asia-southeast1 \
  variables set -- ENVIRONMENT staging

# Trigger streaming DAG
gcloud composer environments run your-composer-env \
  --location=asia-southeast1 \
  dags trigger -- ms_member_mid_term_streaming
```

## Validation

### 1. Check DAG Status

```bash
# List all DAGs
gcloud composer environments run your-composer-env \
  --location=asia-southeast1 \
  dags list

# Check DAG state
gcloud composer environments run your-composer-env \
  --location=asia-southeast1 \
  dags state -- ms_member_short_term_hourly 2024-01-01
```

### 2. Verify Data Processing

```sql
-- Check staging tables
SELECT 
  COUNT(*) as record_count,
  MAX(ingested_at) as last_ingested
FROM `project.insight_dev.stg_ms_member`
WHERE DATE(ingested_at) = CURRENT_DATE();

-- Check audit logs
SELECT *
FROM `project.insight_dev.audit_job_log`
WHERE DATE(job_time) = CURRENT_DATE()
ORDER BY job_time DESC
LIMIT 10;
```

### 3. Monitor Dataflow Jobs

```bash
# List Dataflow jobs
gcloud dataflow jobs list --region=asia-southeast1

# Check job status
gcloud dataflow jobs show JOB_ID --region=asia-southeast1
```

## Environment-Specific Deployment

### Development Environment

```yaml
# Update config files with dev settings
gcp:
  project_id: the1-insight-dev
  environment: dev
datasets:
  staging_dataset: insight_dev
  refined_dataset: insight_dev
```

### Staging Environment

```yaml
# Update config files with staging settings
gcp:
  project_id: the1-insight-staging
  environment: staging
datasets:
  staging_dataset: insight_staging
  refined_dataset: insight_staging
```

### Production Environment

```yaml
# Update config files with prod settings
gcp:
  project_id: the1-insight-prod
  environment: prod
datasets:
  staging_dataset: insight_prod
  refined_dataset: insight_prod
dataflow:
  machine_type: n1-highmem-4  # Larger machines for prod
  max_num_workers: 20
```

## Rollback Procedures

### DAG Rollback

```bash
# Pause problematic DAG
gcloud composer environments run your-composer-env \
  --location=asia-southeast1 \
  dags pause -- dag_name

# Restore previous version
gsutil cp gs://${COMPOSER_BUCKET}/dags/backup/dag_name.py \
  gs://${COMPOSER_BUCKET}/dags/dag_name.py

# Unpause DAG
gcloud composer environments run your-composer-env \
  --location=asia-southeast1 \
  dags unpause -- dag_name
```

### Configuration Rollback

```bash
# Restore previous configuration
gsutil cp -r gs://${COMPOSER_BUCKET}/dags/composer/config.backup/ \
  gs://${COMPOSER_BUCKET}/dags/composer/config/
```

### Data Rollback

```sql
-- Use BigQuery time travel
CREATE OR REPLACE TABLE `project.dataset.table_restored` AS
SELECT * 
FROM `project.dataset.table`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```

## Monitoring Setup

### Create Monitoring Dashboard

```bash
# Create custom dashboard
gcloud monitoring dashboards create --config-from-file=monitoring/dashboard.yaml
```

### Set Up Alerts

```bash
# Create alert policy for pipeline failures
gcloud alpha monitoring policies create --policy-from-file=monitoring/alerts.yaml
```

## Troubleshooting

### Common Issues

1. **DAG Import Errors**
```bash
# Check Airflow logs
gcloud composer environments run your-composer-env \
  --location=asia-southeast1 \
  logs list
```

2. **Dataflow Job Failures**
```bash
# Get job logs
gcloud dataflow jobs show JOB_ID --region=asia-southeast1 --format=json | jq .
```

3. **Permission Issues**
```bash
# Check service account permissions
gcloud projects get-iam-policy ${PROJECT_ID} \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:${SA_EMAIL}"
```

### Health Checks

```bash
# Check Composer environment health
gcloud composer environments describe your-composer-env \
  --location=asia-southeast1

# Check BigQuery dataset access
bq ls -d ${PROJECT_ID}:

# Check GCS bucket access
gsutil ls gs://t1-dataflow-framework-bucket/
```

## Maintenance

### Regular Tasks

1. **Weekly**: Review audit logs for anomalies
2. **Monthly**: Clean up old Dataflow job artifacts
3. **Quarterly**: Review and optimize configurations
4. **Annually**: Update dependencies and versions

### Backup Schedule

```bash
# Daily backup of configurations
gsutil -m rsync -r gs://${COMPOSER_BUCKET}/dags/composer/config/ \
  gs://${BACKUP_BUCKET}/config/$(date +%Y%m%d)/

# Weekly backup of DAGs
gsutil -m rsync -r gs://${COMPOSER_BUCKET}/dags/ \
  gs://${BACKUP_BUCKET}/dags/$(date +%Y%m%d)/
```

## Security Checklist

- [ ] Service accounts have minimum required permissions
- [ ] Secrets are stored in Secret Manager
- [ ] VPC Service Controls configured (if required)
- [ ] Data encryption at rest enabled
- [ ] Audit logging enabled
- [ ] Network security configured
- [ ] Access controls reviewed

## Support

For deployment issues:
1. Check deployment logs in Cloud Logging
2. Review error messages in Airflow UI
3. Contact the data engineering team
4. Create a support ticket with error details
