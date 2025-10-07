"""
Airflow DAG for the `ms_member_short` pipeline using a custom container
that packages both the pipeline and the `dataflow_common` library.  This
DAG illustrates how to run a Dataflow pipeline without relying on
`extra_packages` or external network calls at runtime.
"""

from __future__ import annotations

import datetime
from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import logging


def pre_check_dataflow(**context):
    """Check that the Dataflow API is enabled and print recent jobs."""
    import subprocess
    logging.info("Checking Dataflow API and listing recent jobs...")
    result = subprocess.run([
        'gcloud', 'services', 'list', '--enabled', '--filter', 'name:dataflow.googleapis.com'
    ], capture_output=True, text=True)
    logging.info("Dataflow API:\n%s", result.stdout)
    result = subprocess.run([
        'gcloud', 'dataflow', 'jobs', 'list', '--region=asia-southeast1', '--limit=5', '--format=json'
    ], capture_output=True, text=True)
    logging.info("Recent Dataflow jobs:\n%s", result.stdout[:500])


DEFAULT_ARGS = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=10),
}

dag = DAG(
    'ms_member_short_container_example',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['dataflow', 'custom-container'],
)

pre_check = PythonOperator(
    task_id='pre_check_dataflow',
    python_callable=pre_check_dataflow,
    dag=dag,
)

# Replace these placeholders with your actual Artifact Registry image and config path.
PIPELINE_IMAGE = 'asia-southeast1-docker.pkg.dev/your-project/dataflow-images/ms-member-short:v1.0'
PY_FILE_IN_CONTAINER = '/app/ms_member_short_pipeline.py'
CONFIG_PATH = 'gs://your-bucket/path/to/ms_member_short.yaml'

run_pipeline = BeamRunPythonPipelineOperator(
    task_id='run_dataflow_pipeline',
    runner='DataflowRunner',
    py_file=PY_FILE_IN_CONTAINER,
    pipeline_options={
        'project': 'your-gcp-project',
        'region': 'asia-southeast1',
        'temp_location': 'gs://your-bucket/temp',
        'staging_location': 'gs://your-bucket/staging',
        'service_account_email': 'your-dataflow-sa@your-gcp-project.iam.gserviceaccount.com',
        'sdk_container_image': PIPELINE_IMAGE,
        'sdk_location': 'container',
        'config_path': CONFIG_PATH,
        'save_main_session': True,
        'max_num_workers': 2,
        # Additional options such as S3 credentials can be provided here
    },
    py_requirements=[
        'apache-beam[gcp]==2.59.0',
    ],
    py_system_site_packages=False,
    dag=dag,
)

pre_check >> run_pipeline