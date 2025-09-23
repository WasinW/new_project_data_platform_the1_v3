python dataflow/FW/unified_dataflow_pipeline_bigtable.py \
  --project_id=the1-insight-dev \
  --src_project_id=the1-insight-dev \
  --term_type=short \
  --mode=batch \
  --env=dev \
  --source_dataset=insight \
  --staging_dataset=insight_dev \
  --refined_dataset=insight_dev \
  --source_table=personas_sync \
  --stg_source_table=stg_ms_personas \
  --stg_origin_table=stg_ms_member \
  --refined_ongoing_table=ms_personas \
  --audit_table=audit_job_log \
  --mapping_table=mapping_reconcile \
  --error_table=dq_errors \
  --min_batch_size=100 \
  --max_batch_size=500 \
  --enrichment_batch_size=500 \
  --partition_filter="DATE(_PARTITIONTIME) = CURRENT_DATE()" \
  --read_method=DIRECT_READ \
  --write_method=FILE_LOADS \
  --max_errors_percent=0.05 \
  --validation_rules='[{"type":"required","field":"member_number"}]' \
  --bq_priority=INTERACTIVE \
  --bq_write_disposition=WRITE_TRUNCATE \
  --bq_create_disposition=CREATE_IF_NEEDED \
  --metrics_enabled \
  --audit_enabled \
  --error_tracking_enabled \
  --max_retries=3 \
  --initial_backoff=1 \
  --max_backoff=60 \
  --runner=DataflowRunner \
  --region=asia-southeast1 \
  --temp_location=gs://t1-insight-audit-bucket/audit_log/dataflow/temp \
  --staging_location=gs://t1-insight-audit-bucket/audit_log/dataflow/staging \
  --machine_type=n1-standard-2 \
  --max_num_workers=5 \
  --save_main_session \
  --setup_file=dataflow/setup.py \
  --job_name=ms-member-short-batch-20241218-1234 \
  --service_account_email=t1-ins-dev-sa-data@the1-insight-dev.iam.gserviceaccount.com \
  --network=projects/the1-network-stg/global/networks/the1-vpc-net-share-stg \
  --subnetwork=regions/asia-southeast1/subnetworks/the1-subnet-dataflow-stg	 \
  --no_use_public_ips


gcloud iam service-accounts add-iam-policy-binding \
    t1-ins-dev-sa-data@the1-insight-dev.iam.gserviceaccount.com \
    --member="user:wawasin@the1.co.th" \
    --role="roles/iam.serviceAccountTokenCreator"


DATAFLOW_SA="t1-ins-dev-sa-data@the1-insight-dev.iam.gserviceaccount.com"
gcloud projects get-iam-policy the1-insight-dev
   --flatten="bindings[].members"
   --filter="bindings.members:serviceAccount:t1-ins-dev-sa-data@the1-insight-dev.iam.gserviceaccount.com"
   --format="table(bindings.role)"


gsutil iam ch serviceAccount:${DATAFLOW_SA}:objectViewer gs://t1-insight-audit-bucket

DATAFLOW_SA="t1-ins-dev-sa-data@the1-insight-dev.iam.gserviceaccount.com"
gcloud projects add-iam-policy-binding the1-insight-dev \
  --member="serviceAccount:${DATAFLOW_SA}" \
  --role="roles/dataflow.worker"
# 4. ให้สิทธิ์ Compute (จำเป็นสำหรับ Dataflow workers)
gcloud projects add-iam-policy-binding the1-insight-dev \
  --member="serviceAccount:${DATAFLOW_SA}" \
  --role="roles/compute.viewer"

# 5. ให้สิทธิ์ Service Account User (สำคัญมาก!)
gcloud projects add-iam-policy-binding the1-insight-dev \
  --member="serviceAccount:${DATAFLOW_SA}" \
  --role="roles/iam.serviceAccountUser"



# -- SELECT profiles.dateOfBirth FROM `the1-insight-stg.insight.personas` LIMIT 10
# with personas as (
# SELECT profiles FROM `the1-insight-stg.insight.personas` 
# )

# select profiles.dateOfBirth from personas
# LIMIT 10


python dataflow/FW/unified_dataflow_pipeline_bigtable.py \
  --project_id=the1-insight-dev \
  --src_project_id=the1-insight-dev \
  --term_type=short \
  --mode=batch \
  --env=dev \
  --source_dataset=insight \
  --staging_dataset=insight_dev \
  --refined_dataset=insight_dev \
  --source_table=personas_sync \
  --stg_ongoing_source_table=stg_personas \
  --stg_source_table=stg_ms_personas \
  --stg_origin_table=stg_ms_member \
  --refined_ongoing_table=ms_personas \
  --audit_table=audit_job_log \
  --mapping_table=mapping_reconcile \
  --error_table=dq_errors \
  --min_batch_size=100 \
  --max_batch_size=500 \
  --enrichment_batch_size=500 \
  --partition_filter="DATE(_PARTITIONTIME) = CURRENT_DATE()" \
  --read_method=DIRECT_READ \
  --write_method=FILE_LOADS \
  --max_errors_percent=0.05 \
  --validation_rules='[{"type":"required","field":"member_number"}]' \
  --bq_priority=INTERACTIVE \
  --bq_write_disposition=WRITE_TRUNCATE \
  --bq_create_disposition=CREATE_IF_NEEDED \
  --metrics_enabled \
  --audit_enabled \
  --error_tracking_enabled \
  --max_retries=3 \
  --initial_backoff=1 \
  --max_backoff=60 \
  --runner=DataflowRunner \
  --region=asia-southeast1 \
  --temp_location=gs://t1-insight-audit-bucket/audit_log/dataflow/temp \
  --staging_location=gs://t1-insight-audit-bucket/audit_log/dataflow/staging \
  --machine_type=n1-standard-2 \
  --max_num_workers=5 \
  --save_main_session \
  --setup_file=dataflow/setup.py \
  --job_name=ms-member-short-batch-20241218-1234 \
  --service_account_email=t1-ins-dev-sa-data@the1-insight-dev.iam.gserviceaccount.com \
  --network=projects/the1-network-stg/global/networks/the1-vpc-net-share-stg \
  --subnetwork=regions/asia-southeast1/subnetworks/the1-subnet-dataflow-stg	 \
  --no_use_public_ips

