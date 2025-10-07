"""
Airflow DAG for testing BigQuery read with VPC Service Controls
ใช้ BeamRunPythonPipelineOperator แทน subprocess
"""
import datetime
import time
from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery_dts import (
    BigQueryDataTransferServiceStartTransferRunsOperator
)
from airflow.providers.google.cloud.sensors.bigquery_dts import (
    BigQueryDataTransferServiceTransferRunSensor
)
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor

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
    'retries': 2,
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

# # Task 1: Trigger mapping_reconcile transfer from S3 to BigQuery
# trigger_mapping_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
#     task_id="trigger_mapping_reconcile_transfer",
#     project_id="the1-insight-dev",
#     location="asia-southeast1",
#     transfer_config_id="68dfd9a1-0000-2476-90c2-240588710eb4",
#     requested_run_time={"seconds": int(time.time())},
#     gcp_conn_id='google_cloud_default',
#     deferrable=True,
#     dag=dag,  # <--- สำคัญ
# )
# # Task 2: Monitor mapping transfer completion
# monitor_mapping_transfer = BigQueryDataTransferServiceTransferRunSensor(
#     task_id="monitor_mapping_transfer",
#     transfer_config_id="68dfd9a1-0000-2476-90c2-240588710eb4",
#     run_id="{{ ti.xcom_pull(task_ids='trigger_mapping_reconcile_transfer', key='run_id') }}",
#     expected_statuses={"SUCCEEDED"},
#     project_id="the1-insight-dev",
#     location="asia-southeast1",
#     # poke_interval="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['poke_interval_seconds'] }}",
#     # timeout="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['mapping_timeout_seconds'] }}",
#     # mode="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['mode'] }}",
#     poke_interval=60,  # ใช้ค่าตัวเลขโดยตรง
#     timeout=600,       # ใช้ค่าตัวเลขโดยตรง  
#     mode="poke",       # ใช้ค่า string โดยตรง
#     gcp_conn_id='google_cloud_default',
#     dag=dag,  # <--- สำคัญ
# )
# # Task 3: Trigger ms_member transfer from S3 to BigQuery
# trigger_member_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
#     task_id="trigger_ms_member_transfer",
#     project_id="the1-insight-dev",
#     location="asia-southeast1",
#     transfer_config_id="68d4c7fd-0000-2de1-9b84-582429c5caa4",
#     requested_run_time={"seconds": int(time.time())},
#     gcp_conn_id='google_cloud_default',
#     deferrable=True,
#     dag=dag,  # <--- สำคัญ
# )
# # Task 4: Monitor member transfer completion
# monitor_member_transfer = BigQueryDataTransferServiceTransferRunSensor(
#     task_id="monitor_member_transfer",
#     transfer_config_id="68d4c7fd-0000-2de1-9b84-582429c5caa4",
#     run_id="{{ ti.xcom_pull(task_ids='trigger_ms_member_transfer', key='run_id') }}",
#     expected_statuses={"SUCCEEDED"},
#     project_id="the1-insight-dev",
#     location="asia-southeast1",
#     # poke_interval="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['poke_interval_seconds'] }}",
#     # timeout="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['member_timeout_seconds'] }}",
#     # mode="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['mode'] }}",
#     poke_interval=60,  # ใช้ค่าตัวเลขโดยตรง
#     timeout=600,       # ใช้ค่าตัวเลขโดยตรง  
#     mode="poke",       # ใช้ค่า string โดยตรง
#     gcp_conn_id='google_cloud_default',
#     dag=dag,  # <--- สำคัญ
# )


# BeamRunPythonPipelineOperator task
dataflow_job = BeamRunPythonPipelineOperator(
    task_id='run_dataflow_pipeline',
    runner='DataflowRunner',
    # py_file='gs://t1-airflow-composer-bucket/dags/composer/dags/test_bq_read_simple4.py',
    py_file='gs://t1-dataflow-framework-bucket/jobs/ms_member_short_pipeline.py',
    # gs://t1-dataflow-framework-bucket/jobs/ms_member_short_pipeline.py
    # ----------------------------
    # 2) ฝั่ง Dataflow worker
    # ----------------------------
    pipeline_options={
        'project': 'the1-insight-dev',
        'region': 'asia-southeast1',
        'temp_location': 'gs://t1-insight-audit-bucket/audit_log/dataflow/temp',
        'staging_location': 'gs://t1-insight-audit-bucket/audit_log/dataflow/staging',
        'service_account_email': 't1-ins-dev-sa-data@the1-insight-dev.iam.gserviceaccount.com',
        'use_public_ips': False,  # Critical for VPC SC and False when install external libs
        'save_main_session': True,
        # 'subnetwork': 'regions/asia-southeast1/subnetworks/dataflow-private',  # Short form
        'subnetwork':'https://www.googleapis.com/compute/v1/projects/the1-network-stg/regions/asia-southeast1/subnetworks/the1-subnet-dataflow-stg',
        # หรือใช้ full path:
        # 'subnetwork': 'projects/the1-insight-dev/regions/asia-southeast1/subnetworks/dataflow-private',
        'experiments': ['use_runner_v2'],
        'worker_machine_type': 'n1-standard-2',
        'max_num_workers': 2,
        'project_id': 'the1-insight-dev',  # เพิ่มเพื่อให้ script รับไปใช้
        'mode': 'batch',
        # ====== S3 credentials ผ่าน S3Options ของ Beam AWS I/O ======
        # --- เพิ่ม S3Options ---
        's3_region_name': 'ap-southeast-1',
        's3_access_key_id': '{{ var.value.AWS_ACCESS_KEY_ID }}',       # แนะนำดึงจาก Airflow Variable/Secret
        's3_secret_access_key': '{{ var.value.AWS_SECRET_ACCESS_KEY }}',
        # ----------------------

        # ADD LIB WORKER OPTIONS
        # **สำคัญ**: ให้ Dataflow ติดตั้งล้อออฟไลน์จาก GCS บน worker
        # ---------------------------------------------------------------
        # ====== ที่สำคัญ: ติดตั้ง dependency บน "worker" แบบออฟไลน์ ======
        # 'extra_packages': [
        #     "gs://t1-airflow-composer-bucket/dags/packages/offline_wheels/boto3-1.34.106-py3-none-any.whl",
        #     # "gs://t1-airflow-composer-bucket/dags/packages/offline_wheels/botocore-1.34.106-py3-none-any.whl",
        #     # "gs://t1-airflow-composer-bucket/dags/packages/offline_wheels/s3transfer-0.10.1-py3-none-any.whl",
        #     # "gs://t1-airflow-composer-bucket/dags/packages/offline_wheels/jmespath-1.0.1-py3-none-any.whl",
        #     # "gs://t1-airflow-composer-bucket/dags/packages/offline_wheels/urllib3-1.26.18-py2.py3-none-any.whl",
        #     # "gs://t1-airflow-composer-bucket/dags/packages/offline_wheels/python_dateutil-2.9.0.post0-py2.py3-none-any.whl",
        #     # "gs://t1-airflow-composer-bucket/dags/packages/offline_wheels/six-1.16.0-py2.py3-none-any.whl",
        #     # "gs://t1-airflow-composer-bucket/dags/packages/offline_wheels/numpy-2.2.6-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
        #     "gs://t1-airflow-composer-bucket/dags/packages/offline_wheels/pyarrow-14.0.2-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
        # ],
        'sdk_container_image': 'asia-southeast1-docker.pkg.dev/the1-insight-dev/dataflow-images/dataflow-common:v1.2',
        'sdk_location': 'container',
        'config_path': 'gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/batch/ms_member_short.yaml',

    },
    # ----------------------------
    # 1) ฝั่ง Composer (driver)
    # ----------------------------
    # py_requirements_file='/home/airflow/gcs/dags/composer/requirements/beam-composer-reqs.txt',
    py_requirements=[
        'apache-beam[gcp]==2.59.0',
        'google-cloud-bigquery==3.25.0',
        # 'apache-beam[aws]==2.59.0',
        'pyarrow>=12.0.0',
        # 'boto3>=1.28.0',
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


wait_dataflow = DataflowJobStatusSensor(
    task_id="wait_for_dataflow_done",
    project_id="the1-insight-dev",
    location="asia-southeast1",
    # บางเวอร์ชัน XCom key อาจเป็น 'job_id' หรือ 'dataflow_job_id' -> ลองดึงทั้งคู่
    job_id="{{ ti.xcom_pull(task_ids='run_dataflow_pipeline', key='job_id') or \
              ti.xcom_pull(task_ids='run_dataflow_pipeline', key='dataflow_job_id') }}",
    expected_statuses={"JOB_STATE_DONE"},
    poke_interval=60,
    timeout=60*60*3,   # 3 ชม. ตาม SLA
    mode="reschedule", # ลดการจับ slot ระหว่างรอ
    dag=dag,
)

# trigger_mapping_transfer >> monitor_mapping_transfer
# trigger_member_transfer >> monitor_member_transfer

# [monitor_mapping_transfer, monitor_member_transfer] >> pre_check >> dataflow_job >> wait_dataflow
pre_check >> dataflow_job >> wait_dataflow
