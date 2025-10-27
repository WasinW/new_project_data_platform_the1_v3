"""
MS Member Short-term Pipeline - Initial/Manual Run
BigQuery Data Transfer -> Dataflow Processing -> S3 Parquet
"""
import datetime 
import time
import logging
import subprocess
from datetime import datetime as dt

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor

# ============================================
# CONFIGURATION
# ============================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load variables once
PROJECT_ID = Variable.get("project_id")
GCP_CONN_ID = "google_cloud_default"
REGION = "asia-southeast1"
JOB_NAME = 'ms-member-short-init'
# ============================================
# HELPER FUNCTIONS
# ============================================
def check_dataflow_setup(**context):
    """Pre-check Dataflow API and list recent jobs"""
    logger.info("Checking Dataflow setup...")
    
    # Check Dataflow API
    result = subprocess.run(
        ['gcloud', 'services', 'list', '--enabled', '--filter', 'name:dataflow.googleapis.com'],
        capture_output=True,
        text=True
    )
    logger.info(f"Dataflow API check: {result.stdout}")
    
    # List recent Dataflow jobs
    result = subprocess.run([
        'gcloud', 'dataflow', 'jobs', 'list',
        f'--region={REGION}',
        '--limit=5',
        '--format=json'
    ], capture_output=True, text=True)
    
    if result.stdout:
        logger.info(f"Recent jobs: {result.stdout[:500]}")
    return True

# ============================================
# DAG DEFINITION
# ============================================
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'ms_member_short_term_init',
    default_args=default_args,
    description='MS Member Pipeline - Initial/Manual Run',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['ms-member', 'bigquery', 'dataflow', 's3', 'manual'],
)

# ============================================
# TASKS
# ============================================
# Task 0: Pre-check
pre_check = PythonOperator(
    task_id='pre_check_dataflow',
    python_callable=check_dataflow_setup,
    dag=dag
)

# # Task 1: Trigger mapping_reconcile transfer from S3 to BigQuery
# trigger_mapping_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
#     task_id="trigger_mapping_reconcile_transfer",
#     project_id=PROJECT_ID,
#     location=REGION,
#     transfer_config_id='{{ var.value.mapping_transfer_config_id }}',
#     requested_run_time={"seconds": int(time.time())},
#     gcp_conn_id=GCP_CONN_ID,
#     deferrable=True,
#     dag=dag,
# )

# # Task 2: Monitor mapping transfer
# monitor_mapping_transfer = BigQueryDataTransferServiceTransferRunSensor(
#     task_id="monitor_mapping_transfer",
#     transfer_config_id='{{ var.value.mapping_transfer_config_id }}',
#     run_id="{{ ti.xcom_pull(task_ids='trigger_mapping_reconcile_transfer', key='run_id') }}",
#     expected_statuses={"SUCCEEDED"},
#     project_id=PROJECT_ID,
#     location=REGION,
#     poke_interval=60,
#     timeout=600,
#     mode="poke",
#     gcp_conn_id=GCP_CONN_ID,
#     dag=dag,
# )

# # Task 3: Trigger MS member transfer
# trigger_member_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
#     task_id="trigger_ms_member_transfer",
#     project_id=PROJECT_ID,
#     location=REGION,
#     transfer_config_id='{{ var.value.member_transfer_config_id }}',
#     requested_run_time={"seconds": int(time.time())},
#     gcp_conn_id=GCP_CONN_ID,
#     deferrable=True,
#     dag=dag,
# )

# # Task 4: Monitor member transfer completion
# monitor_member_transfer = BigQueryDataTransferServiceTransferRunSensor(
#     task_id="monitor_member_transfer",
#     transfer_config_id='{{ var.value.member_transfer_config_id }}',
#     run_id="{{ ti.xcom_pull(task_ids='trigger_ms_member_transfer', key='run_id') }}",
#     expected_statuses={"SUCCEEDED"},
#     project_id=PROJECT_ID,
#     location=REGION,
#     poke_interval=60,
#     timeout=600,
#     mode="poke",
#     gcp_conn_id=GCP_CONN_ID,
#     dag=dag,
# )


# BeamRunPythonPipelineOperator task
dataflow_job = BeamRunPythonPipelineOperator(
    task_id='run_dataflow_pipeline',
    runner='DataflowRunner',
    py_file='{{ var.value.bucket_dataflow }}/jobs/ms_member_short_pipeline.py',

    # Dataflow pipeline options
    # ----------------------------
    # 2) ฝั่ง Dataflow worker
    # ----------------------------
    pipeline_options={
        'project': PROJECT_ID,
        'region': REGION,
        'temp_location': '{{ var.value.bucket_audit }}/audit_log/dataflow/temp',
        'staging_location': '{{ var.value.bucket_audit }}/audit_log/dataflow/staging',
        
        # Network & Security
        'service_account_email': '{{ var.value.dataflow_sa_email }}',
        'use_public_ips': True,
        'subnetwork': '{{ var.value.dataflow_subnetwork }}',
        
        # Worker configuration
        'worker_machine_type': 'n1-standard-2',
        'max_num_workers': 2,
        'save_main_session': True,
        'experiments': ['use_runner_v2','enable_stackdriver_agent_metrics','worker_log_level_debug'],
        # Control log levels
        # 'defaultWorkerLogLevel': 'INFO',    # Worker logs
        # 'sdkHarnessLogLevel': 'WARNING',    # SDK logs
        # 'worker_log_level': 'INFO',         # Your code
        # 'log_level': 'INFO',  # สำหรับ pipeline code ของเรา


        # Container settings
        'sdk_container_image': '{{ var.value.dataflow_common_image }}',
        'sdk_location': 'container',
        
        # Pipeline parameters
        'project_id': PROJECT_ID,
        'mode': 'batch',
        'config_path': '{{ var.value.bucket_config }}/dags/composer/config/ms_member/batch/ms_member_short_init.yaml',
        
        # AWS S3 credentials
        's3_region_name': 'ap-southeast-1',
        's3_access_key_id': '{{ var.value.AWS_ACCESS_KEY_ID }}',
        's3_secret_access_key': '{{ var.value.AWS_SECRET_ACCESS_KEY }}',

    },
    # Python dependencies
    # ----------------------------
    # 1) ฝั่ง Composer (driver)
    # ----------------------------
    # py_requirements_file='/home/airflow/gcs/dags/composer/requirements/beam-composer-reqs.txt',
    py_requirements=[
        'apache-beam[gcp]==2.59.0',
        'google-cloud-bigquery==3.25.0',
        'pyarrow>=12.0.0',
        'pyyaml>=6.0',
        '/home/airflow/gcs/dags/packages/dataflow_common-1.0.0-py3-none-any.whl',
    ],
    py_system_site_packages=False,
    dataflow_config=DataflowConfiguration(
        job_name=JOB_NAME,
        project_id=PROJECT_ID,
        location=REGION,
        wait_until_finished=True,
        gcp_conn_id=GCP_CONN_ID,
        check_if_running='IgnoreJob',
    ),
    deferrable=False,
    dag=dag,
)


wait_dataflow = DataflowJobStatusSensor(
    task_id="wait_for_dataflow_done",
    project_id=PROJECT_ID,
    location=REGION,
    job_id="{{ ti.xcom_pull(task_ids='run_dataflow_pipeline', key='job_id') or "
           "ti.xcom_pull(task_ids='run_dataflow_pipeline', key='dataflow_job_id') }}",
    expected_statuses={"JOB_STATE_DONE"},
    poke_interval=60,
    timeout=10800,  # 3 hours
    mode="reschedule",
    dag=dag,
)

# ============================================
# TASK DEPENDENCIES
# ============================================
# Parallel transfers
# trigger_mapping_transfer >> monitor_mapping_transfer
# trigger_member_transfer >> monitor_member_transfer

# After both transfers complete -> pre-check -> dataflow -> wait
# [monitor_mapping_transfer, monitor_member_transfer] >> pre_check >> dataflow_job >> wait_dataflow
pre_check >> dataflow_job >> wait_dataflow
