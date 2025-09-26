"""
Airflow DAG for MS Member Short Term Hourly Batch Pipeline (Refactored v2)
Loads configuration from YAML with defaults support - NO HARD CODED VALUES
"""

from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from datetime import datetime, timedelta
import yaml
import logging
import os
import json
from pathlib import Path

logger = logging.getLogger(__name__)

# Configuration paths - these are the ONLY hard coded paths
CONFIG_PATH = os.environ.get('SHORT_TERM_CONFIG', 
                            '/home/airflow/gcs/dags/composer/config/ms_member/batch/short_term_hourly.yaml')
DEFAULTS_PATH = os.environ.get('DEFAULTS_CONFIG',
                              '/home/airflow/gcs/dags/composer/config/ms_member/common/defaults.yaml')


def merge_configs(base_config: dict, override_config: dict) -> dict:
    """Recursively merge configurations"""
    import copy
    result = copy.deepcopy(base_config)
    
    for key, value in override_config.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_configs(result[key], value)
        else:
            result[key] = value
    
    return result


def load_and_prepare_config(**context):
    """Load configuration from YAML file with defaults and prepare for Dataflow"""
    
    # Load defaults first
    defaults = {}
    if os.path.exists(DEFAULTS_PATH):
        with open(DEFAULTS_PATH, 'r') as f:
            defaults_file = yaml.safe_load(f)
            defaults = defaults_file.get('defaults', {})
        logger.info(f"Loaded defaults from {DEFAULTS_PATH}")
    else:
        logger.warning(f"Defaults file not found at {DEFAULTS_PATH}")
    
    # Load main configuration
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
    
    # Check if config references another defaults file
    if 'defaults_file' in config:
        defaults_ref_path = Path(CONFIG_PATH).parent / config['defaults_file']
        if defaults_ref_path.exists():
            with open(defaults_ref_path, 'r') as f:
                ref_defaults = yaml.safe_load(f)
                defaults = merge_configs(defaults, ref_defaults.get('defaults', {}))
    
    # Merge defaults with config (config overrides defaults)
    final_config = merge_configs(defaults, config)
    
    # Flatten configuration for Dataflow parameters - NO HARD CODED DEFAULTS
    dataflow_params = {
        # GCP settings
        'project_id': final_config['gcp']['project_id'],
        'env': final_config['gcp'].get('environment'),
        
        # Pipeline settings
        'term_type': final_config['pipeline']['term_type'],
        'mode': final_config['pipeline'].get('mode'),
        
        # Datasets - from merged config
        'source_dataset': final_config['datasets'].get('source_dataset'),
        'staging_dataset': final_config['datasets'].get('staging_dataset'),
        'refined_dataset': final_config['datasets'].get('refined_dataset'),
        
        # Tables - from merged config
        'source_table': final_config.get('tables', {}).get('source_table'),
        'stg_source_table': final_config.get('tables', {}).get('stg_source_table'),
        'stg_origin_table': final_config.get('tables', {}).get('stg_origin_table'),
        'refined_ongoing_table': final_config.get('tables', {}).get('refined_ongoing_table'),
        'audit_table': final_config.get('tables', {}).get('audit_table'),
        'mapping_table': final_config.get('tables', {}).get('mapping_table'),
        'error_table': final_config.get('tables', {}).get('error_table'),
        
        # Batch processing settings
        'min_batch_size': final_config.get('batch', {}).get('min_batch_size'),
        'max_batch_size': final_config.get('batch', {}).get('max_batch_size'),
        'enrichment_batch_size': final_config.get('batch', {}).get('enrichment_batch_size'),
        'partition_filter': final_config.get('batch', {}).get('partition_filter'),
        'read_method': final_config.get('batch', {}).get('read_method'),
        'write_method': final_config.get('batch', {}).get('write_method'),
        
        # Data quality settings
        'max_errors_percent': final_config.get('data_quality', {}).get('max_errors_percent'),
        'validation_rules': json.dumps(final_config.get('data_quality', {}).get('validation_rules', [])),
        'split_output': final_config.get('data_quality', {}).get('split_output'),
        'write_errors': final_config.get('data_quality', {}).get('write_errors'),
        
        # BigQuery settings
        'bq_priority': final_config.get('bigquery', {}).get('priority'),
        'bq_write_disposition': final_config.get('bigquery', {}).get('write_disposition'),
        'bq_create_disposition': final_config.get('bigquery', {}).get('create_disposition'),
        
        # Monitoring settings
        'metrics_enabled': final_config.get('monitoring', {}).get('metrics_enabled'),
        'audit_enabled': final_config.get('monitoring', {}).get('audit_enabled'),
        'error_tracking_enabled': final_config.get('monitoring', {}).get('error_tracking_enabled'),
        
        # Retry settings
        'max_retries': final_config.get('retry', {}).get('max_retries'),
        'initial_backoff': final_config.get('retry', {}).get('initial_backoff_seconds'),
        'max_backoff': final_config.get('retry', {}).get('max_backoff_seconds'),
        
        # Service account
        'specific_sa': final_config.get('dataflow', {}).get('service_account'),
        
        # Store complete config as JSON for complex nested structures
        'raw_config': json.dumps(final_config)
    }
    
    # Remove None values - pipeline will use its own defaults if needed
    dataflow_params = {k: v for k, v in dataflow_params.items() if v is not None}
    
    # Store in XCom for other tasks
    context['ti'].xcom_push(key='config', value=final_config)
    context['ti'].xcom_push(key='dataflow_params', value=dataflow_params)
    
    logger.info(f"Loaded configuration for {final_config['pipeline']['name']}")
    logger.info(f"Prepared {len(dataflow_params)} parameters for Dataflow")
    
    return final_config
def prepare_dataflow_config(**context):
    """Prepare configuration for Dataflow job"""
    config = context['ti'].xcom_pull(task_ids='prepare_config', key='config')
    dataflow_params = context['ti'].xcom_pull(task_ids='prepare_config', key='dataflow_params')
    
    # Build pipeline options as string list (not dict)
    # pipeline_options = []
    
    # Required parameters
    # pipeline_options.extend([
    #     f"--project={config['gcp']['project_id']}",
    #     f"--region={config['gcp']['location']}",
    #     "--runner=DataflowRunner",
    #     f"--temp_location={config['storage']['temp_location']}",
    #     f"--staging_location={config['storage']['staging_location']}",
    #     f"--job_name=short-term-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
    #     "--save_main_session",
    #     f"--experiments=['use_runner_v2']",
    #     f"--enable_streaming_engine=False",
    #     f"--worker_zone='asia-southeast1-a'",
    #     # f"--kms_key_name={config['gcp']['location']}",
    # ])
    
    # # VPC Configuration (Critical for VPC-SC)
    # pipeline_options.extend([
    #     "--no_use_public_ips",
    #     "--network=projects/the1-network-dev/global/networks/dataflow",
    #     "--subnetwork=regions/asia-southeast1/subnetworks/dataflow-private",
    #     f"--service_account_email={config['dataflow'].get('service_account')}",
    # ])
    
    # # Dataflow parameters
    # pipeline_options.extend([
    #     f"--machine_type={config['dataflow']['machine_type']}",
    #     f"--max_num_workers={config['dataflow']['max_num_workers']}",
    # ])
    
    # # Add all business logic parameters
    # for key, value in dataflow_params.items():
    #     if value is not None:
    #         pipeline_options.append(f"--{key}={value}")

    pipeline_options = {
        'project': config['gcp']['project_id'],
        'region': config['gcp']['location'],
        'temp_location': config['storage']['temp_location'],
        'staging_location': config['storage']['staging_location'],
        'runner': 'DataflowRunner',
        'save_main_session': True,
        'machine_type': config['dataflow']['machine_type'],
        'max_num_workers': config['dataflow']['max_num_workers'],
        'job_name': f"short-term-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        
        # VPC Settings
        # VPC Settings - สำคัญมากสำหรับ VPC-SC
        'no_use_public_ips': True,
        'network': 'projects/the1-network-dev/global/networks/dataflow',  # เพิ่ม
        'subnetwork': 'regions/asia-southeast1/subnetworks/dataflow-private',
        'service_account_email': config['dataflow'].get('service_account'),
        
        # Worker configuration
        'worker_zone': 'asia-southeast1-a',  # เพิ่ม - สำคัญสำหรับ VPC
        'enable_streaming_engine': False,  # เพิ่ม - ปิดสำหรับ batch job
        
        # Experiments for VPC
        'experiments': ['use_runner_v2'],  # สำคัญสำหรับ VPC-SC
        
        # Extra packages
        'extra_packages': [
            'gs://t1-dataflow-framework-bucket/common/packages/dataflow_common_the1-1.0.0-py3-none-any.whl'
        ],
        
        # Add source_project parameter
        # Add missing parameters
        'source_project': config['gcp']['project_id'],
        'stg_ongoing_source_table': 'stg_personas',
    }
    
    # Add all dataflow params
    pipeline_options.update(dataflow_params)
    
    # Store for next task
    context['ti'].xcom_push(key='pipeline_options', value=pipeline_options)

    return pipeline_options


# Load initial config for DAG setup
initial_config = {}
if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, 'r') as f:
        initial_config = yaml.safe_load(f)

# Load defaults for DAG setup
if os.path.exists(DEFAULTS_PATH):
    with open(DEFAULTS_PATH, 'r') as f:
        defaults = yaml.safe_load(f).get('defaults', {})
        initial_config = merge_configs(defaults, initial_config)

# Define default arguments from merged config
default_args = {
    'owner': initial_config.get('owner'),
    'depends_on_past': initial_config.get('depends_on_past', False),
    'start_date': datetime(2024, 1, 1),
    'retries': initial_config.get('retries', 0),
    'retry_delay': timedelta(minutes=initial_config.get('retry_delay_minutes', 5)),
    'email_on_failure': initial_config.get('email_on_failure', False),
    'email_on_retry': initial_config.get('email_on_retry', False),
}

# Define DAG
with DAG(
    initial_config.get('pipeline', {}).get('name', 'ms_member_pipeline'),
    default_args=default_args,
    description=initial_config.get('pipeline', {}).get('description', ''),
    schedule_interval=initial_config.get('pipeline', {}).get('schedule'),
    catchup=False,
    max_active_runs=1,
    tags=initial_config.get('job', {}).get('tags', []),
) as dag:

    # Start marker
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )
    
    # Load and prepare configuration
    prepare_config_task = PythonOperator(
        task_id='prepare_config',
        python_callable=load_and_prepare_config,
        provide_context=True
    )
    # เพิ่ม task นี้หลัง prepare_config_task
    prepare_dataflow = PythonOperator(
        task_id='prepare_dataflow_config',
        python_callable=prepare_dataflow_config,
        provide_context=True
    )
    # Trigger Dataflow job with all parameters from config
    run_dataflow_batch = BeamRunPythonPipelineOperator(
        task_id='run_dataflow_batch',
        runner='DataflowRunner',
        py_file='gs://t1-dataflow-framework-bucket/framework/unified_dataflow_pipeline_bigtable.py',
        pipeline_options="{{ ti.xcom_pull(task_ids='prepare_dataflow_config', key='pipeline_options') }}",  # Use py_options instead
        dataflow_config=DataflowConfiguration(
            job_name=f"short-term-batch-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            project_id="{{ ti.xcom_pull(task_ids='prepare_config', key='config')['gcp']['project_id'] }}",
            location="{{ ti.xcom_pull(task_ids='prepare_config', key='config')['gcp']['location'] }}",
            wait_until_finished=False,
            check_if_running='IgnoreJob',
        gcp_conn_id='google_cloud_default',
        ),
        py_requirements=[
            'apache-beam[gcp]==2.59.0',
            'google-cloud-bigquery==3.25.0',
            'google-cloud-bigtable==2.23.0',
        ],
        py_system_site_packages=False,
        gcp_conn_id='google_cloud_default',
        # extra_packages=['gs://t1-dataflow-framework-bucket/framework/dataflow_common_the1-1.0.0-py3-none-any.whl'],
    )

    # Data quality check using config values
    data_quality_check = BigQueryInsertJobOperator(
        task_id='data_quality_check',
        configuration={
            "query": {
                "query": """
                -- Data quality check for hourly batch
                WITH quality_metrics AS (
                    SELECT 
                        'stg_ms_personas' as table_name,
                        COUNT(*) as record_count,
                        COUNT(DISTINCT member_number) as unique_members,
                        MAX(ingested_at) as last_ingested
                    FROM `{{ ti.xcom_pull(task_ids='prepare_config', key='config')['gcp']['project_id'] }}.{{ ti.xcom_pull(task_ids='prepare_config', key='config')['datasets']['staging_dataset'] }}.{{ ti.xcom_pull(task_ids='prepare_config', key='config')['tables']['stg_source_table'] }}`
                    WHERE DATE(ingested_at) = CURRENT_DATE()
                        AND DATETIME(ingested_at) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ ti.xcom_pull(task_ids='prepare_config', key='config')['data_quality']['checks'][2]['max_hours'] }} HOUR)
                    
                    UNION ALL
                    
                    SELECT 
                        'stg_ms_member' as table_name,
                        COUNT(*) as record_count,
                        COUNT(DISTINCT member_number) as unique_members,
                        MAX(ingested_at) as last_ingested
                    FROM `{{ ti.xcom_pull(task_ids='prepare_config', key='config')['gcp']['project_id'] }}.{{ ti.xcom_pull(task_ids='prepare_config', key='config')['datasets']['staging_dataset'] }}.{{ ti.xcom_pull(task_ids='prepare_config', key='config')['tables']['stg_origin_table'] }}`
                    WHERE DATE(ingested_at) = CURRENT_DATE()
                        AND DATETIME(ingested_at) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ ti.xcom_pull(task_ids='prepare_config', key='config')['data_quality']['checks'][2]['max_hours'] }} HOUR)
                )
                SELECT 
                    *,
                    CASE 
                        WHEN record_count < {{ ti.xcom_pull(task_ids='prepare_config', key='config')['data_quality']['checks'][0]['warning_threshold'] }} THEN 'WARNING: Low record count'
                        ELSE 'OK'
                    END as status,
                    CURRENT_DATETIME() as check_timestamp
                FROM quality_metrics
                """,
                "useLegacySql": False,
                "priority": "{{ ti.xcom_pull(task_ids='prepare_config', key='config')['bigquery']['priority'] }}"
            }
        },
        gcp_conn_id='google_cloud_default',
        location="{{ ti.xcom_pull(task_ids='prepare_config', key='config')['gcp']['location'] }}",
    )
    
    # End marker
    end_pipeline = DummyOperator(
        task_id='end_pipeline'
    )
    
    # Define DAG flow
    # start_pipeline >> prepare_config_task >> run_dataflow_batch >> data_quality_check >> end_pipeline
    start_pipeline >> prepare_config_task >> prepare_dataflow >> run_dataflow_batch >> data_quality_check >> end_pipeline
