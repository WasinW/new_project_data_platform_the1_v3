"""
Airflow DAG for Initialization Pipeline (Refactored)
Uses configuration from YAML file
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery_dts import (
    BigQueryDataTransferServiceStartTransferRunsOperator
)
from airflow.providers.google.cloud.sensors.bigquery_dts import (
    BigQueryDataTransferServiceTransferRunSensor
)
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import yaml
import logging
import os
import re
from google.cloud import bigquery_datatransfer_v1
from google.cloud import datacatalog_lineage_v1 as lineage_v1
from google.protobuf import timestamp_pb2

logger = logging.getLogger(__name__)

# Configuration path
CONFIG_PATH = os.environ.get('INIT_CONFIG',
                            '/home/airflow/gcs/dags/composer/config/ms_member/init/pl_init_config.yaml')


def load_config(**context):
    """Load configuration from YAML file"""
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
    
    context['ti'].xcom_push(key='config', value=config)
    logger.info(f"Loaded configuration for {config['pipeline']['name']}")
    return config


def _s3_uri_to_fqn(s3_uri: str) -> str:
    """Convert S3 URI to fully qualified name for lineage"""
    if not s3_uri:
        return "s3:unknown"
    m = re.match(r'^s3://([^/]+)/(.*)$', s3_uri.strip())
    if m:
        bucket, vpath = m.group(1), m.group(2)
        return f"s3:{bucket}.{vpath}"
    if s3_uri.startswith("s3://"):
        return f"s3:{s3_uri[5:]}"
    return f"s3:{s3_uri}"


def _get_transfer_config(project_id: str, location: str, config_id: str):
    """Get transfer configuration details"""
    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    name = f"projects/{project_id}/locations/{location}/transferConfigs/{config_id}"
    return client.get_transfer_config(name=name)


def _get_transfer_run(project_id: str, location: str, config_id: str, run_id: str):
    """Get transfer run details"""
    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    name = f"projects/{project_id}/locations/{location}/transferConfigs/{config_id}/runs/{run_id}"
    return client.get_transfer_run(name=name)


def emit_lineage_for_s3_to_bq(**context):
    """Emit lineage information for S3 to BigQuery transfer"""
    config = context['ti'].xcom_pull(task_ids='load_config', key='config')
    
    if not config['lineage']['enabled']:
        logger.info("Lineage tracking disabled")
        return
    
    transfer_config_id = config['transfer']['member_transfer_config_id']
    run_id = context['ti'].xcom_pull(task_ids='trigger_ms_member_transfer', key='run_id')
    project_id = config['gcp']['project_id']
    location = config['gcp']['location']
    dataset = config['datasets']['staging_dataset']
    table = config['tables']['staging_table']
    
    try:
        # Get transfer config and run details
        cfg = _get_transfer_config(project_id, location, transfer_config_id)
        run = _get_transfer_run(project_id, location, transfer_config_id, run_id)
        
        # Extract S3 URI from params
        params = dict(cfg.params) if cfg.params else {}
        s3_uri = params.get("data_path") or params.get("data_path_template") or ""
        s3_fqn = _s3_uri_to_fqn(s3_uri)
        
        # Create BigQuery FQN
        bq_fqn = f"bigquery:{project_id}.{dataset}.{table}"
        
        # Prepare timestamps
        start_ts = timestamp_pb2.Timestamp()
        end_ts = timestamp_pb2.Timestamp()
        
        if run.start_time:
            start_ts.FromDatetime(run.start_time)
        else:
            start_ts.FromDatetime(datetime.utcnow())
            
        if run.end_time:
            end_ts.FromDatetime(run.end_time)
        else:
            end_ts.FromDatetime(datetime.utcnow())
        
        # Create lineage
        lineage_client = lineage_v1.LineageClient()
        parent = f"projects/{project_id}/locations/{location}"
        
        process = lineage_v1.Process(display_name=f"DTS S3→BQ: {dataset}.{table}")
        process_obj = lineage_client.create_process(parent=parent, process=process)
        
        run_obj = lineage_v1.Run(display_name=f"transfer-run-{run_id}")
        run_obj = lineage_client.create_run(parent=process_obj.name, run=run_obj)
        
        event = lineage_v1.LineageEvent(
            start_time=start_ts,
            end_time=end_ts,
            links=[
                lineage_v1.EventLink(
                    source=lineage_v1.EntityReference(fully_qualified_name=s3_fqn),
                    target=lineage_v1.EntityReference(fully_qualified_name=bq_fqn),
                )
            ],
        )
        lineage_client.create_lineage_event(parent=run_obj.name, lineage_event=event)
        
        logger.info(f"Successfully emitted lineage for transfer: {s3_fqn} -> {bq_fqn}")
        
    except Exception as e:
        logger.error(f"Failed to emit lineage: {str(e)}")
        # Don't fail the DAG if lineage emission fails
        pass


# Load configuration
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

# Define default arguments
default_args = {
    'owner': config.get('owner', 'data-engineering'),
    'depends_on_past': config.get('depends_on_past', False),
    'start_date': datetime(2024, 1, 1),
    'retries': config.get('retries', 3),
    'retry_delay': timedelta(minutes=config.get('retry_delay_minutes', 5)),
    'retry_exponential_backoff': config.get('retry_exponential_backoff', True),
}

# Define DAG
with DAG(
    config['pipeline']['name'],
    default_args=default_args,
    description=config['pipeline']['description'],
    schedule_interval=config['pipeline']['schedule'],
    catchup=config['job']['catchup'],
    max_active_runs=config['job']['max_active_runs'],
    tags=config['job']['tags'],
) as dag:

    # Load configuration
    load_config_task = PythonOperator(
        task_id='load_config',
        python_callable=load_config,
        provide_context=True
    )
    
    # Trigger ms_member transfer from S3 to BigQuery
    trigger_member_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="trigger_ms_member_transfer",
        project_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}",
        location="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['location'] }}",
        transfer_config_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['transfer']['member_transfer_config_id'] }}",
        requested_run_time={"seconds": int(time.time())},
        gcp_conn_id='google_cloud_default',
        deferrable=True,
    )

    # Monitor member transfer completion
    monitor_member_transfer = BigQueryDataTransferServiceTransferRunSensor(
        task_id="monitor_member_transfer",
        transfer_config_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['transfer']['member_transfer_config_id'] }}",
        run_id="{{ ti.xcom_pull(task_ids='trigger_ms_member_transfer', key='run_id') }}",
        expected_statuses={config['monitoring']['expected_status']},
        project_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}",
        location="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['location'] }}",
        poke_interval="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['poke_interval_seconds'] }}",
        timeout="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['timeout_seconds'] }}",
        mode="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['mode'] }}",
        gcp_conn_id='google_cloud_default',
    )

    # Emit lineage information
    audit_lineage_member = PythonOperator(
        task_id="audit_lineage_member",
        python_callable=emit_lineage_for_s3_to_bq,
        provide_context=True,
    )

    # Define task dependencies
    load_config_task >> trigger_member_transfer >> monitor_member_transfer >> audit_lineage_member
