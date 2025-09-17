"""
Airflow DAG for MS Member Short Term Hourly Batch Pipeline (Refactored)
Loads configuration from YAML file
"""

from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yaml
import logging
import os

logger = logging.getLogger(__name__)

# Configuration paths
CONFIG_PATH = os.environ.get('SHORT_TERM_CONFIG', 
                            '/home/airflow/gcs/dags/composer/config/ms_member/batch/short_term_hourly.yaml')


def load_config(**context):
    """Load configuration from YAML file"""
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
    
    # Store in XCom for other tasks
    context['ti'].xcom_push(key='config', value=config)
    logger.info(f"Loaded configuration for {config['pipeline']['name']}")
    return config


# Define default arguments from config
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

default_args = {
    'owner': config.get('owner', 'data-engineering'),
    'depends_on_past': config.get('depends_on_past', False),
    'start_date': datetime(2024, 1, 1),
    'retries': config.get('retries', 2),
    'retry_delay': timedelta(minutes=config.get('retry_delay_minutes', 5)),
    'email_on_failure': config.get('email_on_failure', False),
    'email_on_retry': config.get('email_on_retry', False),
}

# Define DAG
with DAG(
    config['pipeline']['name'],
    default_args=default_args,
    description=config['pipeline']['description'],
    schedule_interval=config['pipeline']['schedule'],
    catchup=False,
    max_active_runs=1,
    tags=config['job']['tags'],
) as dag:

    # Start marker
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )
    
    # Load configuration
    load_config_task = PythonOperator(
        task_id='load_config',
        python_callable=load_config,
        provide_context=True
    )
    
    # Trigger Dataflow job
    run_dataflow_batch = BeamRunPythonPipelineOperator(
        task_id='run_dataflow_batch',
        py_file="{{ ti.xcom_pull(task_ids='load_config', key='config')['storage']['dataflow_file'] }}",
        pipeline_options={
            'project': "{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}",
            'region': "{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['location'] }}",
            'runner': 'DataflowRunner',
            'temp_location': "{{ ti.xcom_pull(task_ids='load_config', key='config')['storage']['temp_location'] }}",
            'staging_location': "{{ ti.xcom_pull(task_ids='load_config', key='config')['storage']['staging_location'] }}",
            'machine_type': "{{ ti.xcom_pull(task_ids='load_config', key='config')['dataflow']['machine_type'] }}",
            'max_num_workers': "{{ ti.xcom_pull(task_ids='load_config', key='config')['dataflow']['max_num_workers'] }}",
            'job_name': "{{ ti.xcom_pull(task_ids='load_config', key='config')['job']['name_template'] }}",
            'save_main_session': "{{ ti.xcom_pull(task_ids='load_config', key='config')['dataflow']['save_main_session'] }}",
            # Custom pipeline arguments
            'project_id': "{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}",
            'term_type': "{{ ti.xcom_pull(task_ids='load_config', key='config')['pipeline']['term_type'] }}",
            'env': "{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['environment'] }}",
            'source_dataset': "{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['source_dataset'] }}",
            'staging_dataset': "{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['staging_dataset'] }}",
            'refined_dataset': "{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['refined_dataset'] }}",
            'specific_sa': "{{ ti.xcom_pull(task_ids='load_config', key='config')['dataflow'].get('service_account', '') }}",
        },
        execution_timeout=timedelta(minutes=config['dataflow']['execution_timeout_minutes']),
        py_requirements=config['dataflow']['python_requirements'],
        py_interpreter='python3',
        gcp_conn_id='google_cloud_default',
    )

    # Data quality check
    data_quality_check = BigQueryInsertJobOperator(
        task_id='data_quality_check',
        configuration={
            "query": {
                "query": f"""
                -- Data quality check for hourly batch
                WITH quality_metrics AS (
                    SELECT 
                        'stg_ms_personas' as table_name,
                        COUNT(*) as record_count,
                        COUNT(DISTINCT member_number) as unique_members,
                        MAX(ingested_at) as last_ingested
                    FROM `{{{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}}}.{{{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['staging_dataset'] }}}}.stg_ms_personas`
                    WHERE DATE(ingested_at) = CURRENT_DATE()
                        AND DATETIME(ingested_at) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{{{ ti.xcom_pull(task_ids='load_config', key='config')['data_quality']['checks'][2]['max_hours'] }}}} HOUR)
                    
                    UNION ALL
                    
                    SELECT 
                        'stg_ms_member' as table_name,
                        COUNT(*) as record_count,
                        COUNT(DISTINCT member_number) as unique_members,
                        MAX(ingested_at) as last_ingested
                    FROM `{{{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}}}.{{{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['staging_dataset'] }}}}.stg_ms_member`
                    WHERE DATE(ingested_at) = CURRENT_DATE()
                        AND DATETIME(ingested_at) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{{{ ti.xcom_pull(task_ids='load_config', key='config')['data_quality']['checks'][2]['max_hours'] }}}} HOUR)
                )
                SELECT 
                    *,
                    CASE 
                        WHEN record_count < {{{{ ti.xcom_pull(task_ids='load_config', key='config')['data_quality']['checks'][0]['warning_threshold'] }}}} THEN 'WARNING: Low record count'
                        ELSE 'OK'
                    END as status,
                    CURRENT_DATETIME() as check_timestamp
                FROM quality_metrics
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id='google_cloud_default',
        location="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['location'] }}",
    )
    
    # End marker
    end_pipeline = DummyOperator(
        task_id='end_pipeline'
    )
    
    # Define DAG flow
    start_pipeline >> load_config_task >> run_dataflow_batch >> data_quality_check >> end_pipeline
