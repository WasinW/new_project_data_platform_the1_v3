"""
Airflow DAG for the ms_member_short pipeline using a custom Dataflow container.

This DAG demonstrates how to run a Dataflow pipeline that imports the
``dataflow_common`` library from a custom container image.  It uses
``BeamRunPythonPipelineOperator`` and specifies ``sdk_container_image`` in the
pipeline options instead of ``extra_packages``.  The pipeline reads mapping
and member data from BigQuery, reconciles records, normalizes the schema
according to a JSON or BigQuery schema, and writes Parquet to S3.

Modify the configuration (YAML) and pipeline script to suit other tables or
jobs; the dataflow_common library is generic and reusable.
"""

from __future__ import annotations

import datetime
from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def pre_check_dataflow(**context):
    """Pre-check that Dataflow API is enabled and list recent jobs."""
    import subprocess
    logger.info("Checking Dataflow API and listing recent jobs...")
    result = subprocess.run([
        'gcloud', 'services', 'list', '--enabled', '--filter', 'name:dataflow.googleapis.com'
    ], capture_output=True, text=True)
    logger.info("Dataflow API:
%s", result.stdout)
    result = subprocess.run([
        'gcloud', 'dataflow', 'jobs', 'list',
        '--region=asia-southeast1', '--limit=5', '--format=json'
    ], capture_output=True, text=True)
    logger.info("Recent Dataflow jobs:
%s", result.stdout[:500])


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
    'ms_member_short_custom_container',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['example', 'dataflow', 'custom-container'],
)

pre_check = PythonOperator(
    task_id='pre_check_dataflow',
    python_callable=pre_check_dataflow,
    dag=dag,
)

# Path to the pipeline script on GCS (not included in the container)
PY_FILE = 'gs://your-bucket/path/to/ms_member_short_pipeline.py'

# Path to the YAML configuration on GCS
CONFIG_PATH = 'gs://your-bucket/path/to/ms_member_short.yaml'

# Custom container image (update to your artifact registry path)
CUSTOM_IMAGE = 'asia-southeast1-docker.pkg.dev/your-project/dataflow-images/dataflow-common:latest'

run_pipeline = BeamRunPythonPipelineOperator(
    task_id='run_dataflow_pipeline',
    runner='DataflowRunner',
    py_file=PY_FILE,
    pipeline_options={
        'project': 'your-gcp-project',
        'region': 'asia-southeast1',
        'temp_location': 'gs://your-bucket/temp',
        'staging_location': 'gs://your-bucket/staging',
        'service_account_email': 'your-dataflow-sa@your-gcp-project.iam.gserviceaccount.com',
        'save_main_session': True,
        'max_num_workers': 2,
        'sdk_container_image': CUSTOM_IMAGE,
        # Pass the config path as an option so the pipeline can read it
        'config_path': CONFIG_PATH,
        # Additional options such as AWS credentials can be provided here
    },
    py_requirements=[
        'apache-beam[gcp]==2.59.0',
        # These remain on the driver side only; worker side uses the custom container
    ],
    py_system_site_packages=False,
    dag=dag,
)

pre_check >> run_pipeline
