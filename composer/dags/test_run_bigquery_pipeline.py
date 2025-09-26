"""
Airflow DAG for testing BigQuery read with VPC Service Controls
ใช้ BeamRunPythonPipelineOperator แทน subprocess
"""
import datetime
from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.utils.dates import days_ago
import logging

# เพิ่มก่อน DAG definition
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# เพิ่ม pre-check task
from airflow.operators.python_operator import PythonOperator

def check_dataflow_setup(**context):
    """Check Dataflow setup before running"""
    import subprocess
    logger.info("Checking Dataflow setup...")
    
    # Check if dataflow API is enabled
    result = subprocess.run(
        ['gcloud', 'services', 'list', '--enabled', '--filter', 'name:dataflow.googleapis.com'],
        capture_output=True,
        text=True
    )
    logger.info(f"Dataflow API check: {result.stdout}")
    
    # List recent Dataflow jobs
    result = subprocess.run([
        'gcloud', 'dataflow', 'jobs', 'list',
        '--region=asia-southeast1',
        '--limit=5',
        '--format=json'
    ], capture_output=True, text=True)
    logger.info(f"Recent jobs: {result.stdout[:500]}")
    return True

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'test_bq_read_beam_operator',
    default_args=default_args,
    description='Test BigQuery read via BeamRunPythonPipelineOperator with VPC SC',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['test', 'bigquery', 'dataflow', 'vpc-sc'],
)
pre_check = PythonOperator(
    task_id='pre_check_dataflow',
    python_callable=check_dataflow_setup,
    dag=dag
)

# BeamRunPythonPipelineOperator task
dataflow_job = BeamRunPythonPipelineOperator(
    task_id='run_bigquery_pipeline',
    runner='DataflowRunner',
    py_file='gs://t1-airflow-composer-bucket/dags/composer/dags/test_bq_read_simple.py',
    pipeline_options={
        'project': 'the1-insight-dev',
        'region': 'asia-southeast1',
        'temp_location': 'gs://t1-insight-audit-bucket/audit_log/dataflow/temp',
        'staging_location': 'gs://t1-insight-audit-bucket/audit_log/dataflow/staging',
        'service_account_email': 't1-ins-dev-sa-data@the1-insight-dev.iam.gserviceaccount.com',
        'no_use_public_ips': True,  # Critical for VPC SC
        'subnetwork': 'regions/asia-southeast1/subnetworks/dataflow-private',  # Short form
        # หรือใช้ full path:
        # 'subnetwork': 'projects/the1-insight-dev/regions/asia-southeast1/subnetworks/dataflow-private',
        'experiments': ['use_runner_v2'],
        'worker_machine_type': 'n1-standard-2',
        'max_num_workers': 2,
        'project_id': 'the1-insight-dev',  # เพิ่มเพื่อให้ script รับไปใช้
    },
    py_requirements=[
        'apache-beam[gcp]==2.59.0',
        'google-cloud-bigquery==3.25.0',
    ],
    py_system_site_packages=False,
    dataflow_config=DataflowConfiguration(
        job_name='vpc-bq-test',  # ลบ timestamp ออกดูก่อน
        project_id='the1-insight-dev',
        location='asia-southeast1',
        wait_until_finished=False,
        check_if_running='IgnoreJob',  # ไม่ check job ที่รันอยู่
        # check_if_running='WaitForRun',
    ),
    deferrable=False,  # ใช้ False ก่อนเพื่อดู logs realtime
    dag=dag,
)
pre_check >> dataflow_job
