"""
Airflow DAG for MS Member Streaming Pipeline (Refactored)
Manages streaming Dataflow jobs for real-time data processing
Loads configuration from YAML file
"""

from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateTopicOperator,
    PubSubCreateSubscriptionOperator
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import yaml
import logging
import os

logger = logging.getLogger(__name__)

# Configuration path
CONFIG_PATH = os.environ.get('STREAMING_CONFIG', 
                            '/home/airflow/gcs/dags/composer/config/ms_member/streaming/streaming_realtime.yaml')


def load_config(**context):
    """Load configuration from YAML file"""
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
    
    # Get term type and environment from Airflow Variables or use defaults
    from airflow.models import Variable
    config['pipeline']['term_type'] = Variable.get('TERM_TYPE', default_var=config['pipeline']['term_type'])
    config['gcp']['environment'] = Variable.get('ENVIRONMENT', default_var=config['gcp']['environment'])
    
    # Update dynamic values
    env = config['gcp']['environment']
    config['streaming']['pubsub']['topic'] = config['streaming']['pubsub']['topic_template'].format(env=env)
    config['streaming']['pubsub']['subscription'] = config['streaming']['pubsub']['subscription_template'].format(env=env)
    
    context['ti'].xcom_push(key='config', value=config)
    logger.info(f"Loaded configuration for streaming pipeline")
    return config


def check_streaming_job_status(**context):
    """Check if streaming job is already running"""
    config = context['ti'].xcom_pull(task_ids='load_config', key='config')
    
    try:
        from google.cloud import dataflow_v1beta3
        
        client = dataflow_v1beta3.JobsV1Beta3Client()
        
        request = dataflow_v1beta3.ListJobsRequest(
            project_id=config['gcp']['project_id'],
            location=config['gcp']['location'],
            filter="STATE_RUNNING"
        )
        
        jobs = client.list_jobs(request=request)
        
        for job in jobs:
            if f"ms-member-{config['pipeline']['term_type']}" in job.name:
                logger.info(f"Found existing streaming job: {job.name}")
                context['ti'].xcom_push(key='existing_job_id', value=job.id)
                context['ti'].xcom_push(key='existing_job_name', value=job.name)
                return 'monitor_existing_job'
    except Exception as e:
        logger.warning(f"Could not check for existing jobs: {str(e)}")
    
    logger.info("No existing streaming job found")
    return 'start_new_streaming_job.create_pubsub_topic'


def validate_streaming_health(**context):
    """Validate streaming pipeline health metrics"""
    config = context['ti'].xcom_pull(task_ids='load_config', key='config')
    
    from google.cloud import bigquery
    
    client = bigquery.Client(project=config['gcp']['project_id'])
    
    # Check data freshness
    query = f"""
    WITH freshness_check AS (
        SELECT 
            '{config['datasets']['staging_dataset']}.stg_ms_member' as table_name,
            MAX(ingested_at) as last_ingested,
            DATETIME_DIFF(CURRENT_DATETIME(), MAX(ingested_at), MINUTE) as minutes_since_update
        FROM `{config['gcp']['project_id']}.{config['datasets']['staging_dataset']}.stg_ms_member`
        WHERE DATE(ingested_at) = CURRENT_DATE()
        
        UNION ALL
        
        SELECT 
            '{config['datasets']['refined_dataset']}.ms_personas' as table_name,
            MAX(ingested_at) as last_ingested,
            DATETIME_DIFF(CURRENT_DATETIME(), MAX(ingested_at), MINUTE) as minutes_since_update
        FROM `{config['gcp']['project_id']}.{config['datasets']['refined_dataset']}.ms_personas`
        WHERE DATE(ingested_at) = CURRENT_DATE()
    )
    SELECT 
        *,
        CASE 
            WHEN minutes_since_update > {config['health_check']['data_freshness']['alert_minutes']} THEN 'ALERT: Data stale'
            WHEN minutes_since_update > {config['health_check']['data_freshness']['warning_minutes']} THEN 'WARNING: Check pipeline'
            ELSE 'OK'
        END as health_status
    FROM freshness_check
    """
    
    result = client.query(query).result()
    
    health_metrics = []
    for row in result:
        health_metrics.append({
            'table': row.table_name,
            'last_update': str(row.last_ingested),
            'minutes_stale': row.minutes_since_update,
            'status': row.health_status
        })
        
        if 'ALERT' in row.health_status:
            logger.error(f"Pipeline health alert: {row.table_name} - {row.health_status}")
        elif 'WARNING' in row.health_status:
            logger.warning(f"Pipeline health warning: {row.table_name} - {row.health_status}")
    
    return health_metrics


def monitor_existing_streaming_job(**context):
    """Monitor health of existing streaming job"""
    job_id = context['ti'].xcom_pull(task_ids='check_streaming_job_status', key='existing_job_id')
    job_name = context['ti'].xcom_pull(task_ids='check_streaming_job_status', key='existing_job_name')
    
    logger.info(f"Monitoring existing job: {job_name} (ID: {job_id})")
    
    return {'status': 'monitored', 'job_id': job_id, 'job_name': job_name}


# Load configuration
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

# Define default arguments
default_args = {
    'owner': config.get('owner', 'data-engineering'),
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': config.get('retries', 1),
    'retry_delay': timedelta(minutes=config.get('retry_delay_minutes', 5)),
    'email_on_failure': config.get('email_on_failure', True),
    'email_on_retry': config.get('email_on_retry', True),
    'email': config.get('email_recipients', []),
}

# Define DAG
with DAG(
    config['pipeline']['name_template'].format(term_type=config['pipeline']['term_type']),
    default_args=default_args,
    description=config['pipeline']['description'],
    schedule_interval=config['pipeline']['schedule'],
    catchup=config['job']['catchup'],
    max_active_runs=config['job']['max_active_runs'],
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
    
    # Check if streaming job is already running
    check_job_status = BranchPythonOperator(
        task_id='check_streaming_job_status',
        python_callable=check_streaming_job_status,
        provide_context=True,
    )
    
    # Branch 1: Start new streaming job
    with TaskGroup('start_new_streaming_job') as new_job_group:
        
        # Ensure Pub/Sub topic exists
        create_topic = PubSubCreateTopicOperator(
            task_id='create_pubsub_topic',
            topic="{{ ti.xcom_pull(task_ids='load_config', key='config')['streaming']['pubsub']['topic'] }}",
            project_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}",
            fail_if_exists=False,
            gcp_conn_id='google_cloud_default',
        )
        
        # Ensure Pub/Sub subscription exists
        create_subscription = PubSubCreateSubscriptionOperator(
            task_id='create_pubsub_subscription',
            topic="{{ ti.xcom_pull(task_ids='load_config', key='config')['streaming']['pubsub']['topic'] }}",
            subscription="{{ ti.xcom_pull(task_ids='load_config', key='config')['streaming']['pubsub']['subscription'] }}",
            project_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}",
            fail_if_exists=False,
            gcp_conn_id='google_cloud_default',
        )
        
        # Start streaming Dataflow job
        start_streaming_job = BeamRunPythonPipelineOperator(
            task_id='start_dataflow_streaming',
            py_file="{{ ti.xcom_pull(task_ids='load_config', key='config')['storage']['dataflow_file'] }}",
            pipeline_options={
                'project': "{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}",
                'region': "{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['location'] }}",
                'runner': 'DataflowRunner',
                'temp_location': "{{ ti.xcom_pull(task_ids='load_config', key='config')['storage']['temp_location'] }}",
                'staging_location': "{{ ti.xcom_pull(task_ids='load_config', key='config')['storage']['staging_location'] }}",
                'machine_type': "{{ ti.xcom_pull(task_ids='load_config', key='config')['dataflow']['machine_type'] }}",
                'max_num_workers': "{{ ti.xcom_pull(task_ids='load_config', key='config')['dataflow']['max_num_workers'] }}",
                'enable_streaming_engine': "{{ ti.xcom_pull(task_ids='load_config', key='config')['streaming']['enable_streaming_engine'] }}",
                'streaming': True,
                'job_name': "{{ ti.xcom_pull(task_ids='load_config', key='config')['dataflow']['job_name_template'] }}",
                'save_main_session': "{{ ti.xcom_pull(task_ids='load_config', key='config')['dataflow']['save_main_session'] }}",
                'setup_file': "{{ ti.xcom_pull(task_ids='load_config', key='config')['storage'].get('setup_file', '') }}",
                # Custom pipeline arguments
                'project_id': "{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}",
                'term_type': "{{ ti.xcom_pull(task_ids='load_config', key='config')['pipeline']['term_type'] }}",
                'env': "{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['environment'] }}",
                'source_dataset': "{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['source_dataset'] }}",
                'staging_dataset': "{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['staging_dataset'] }}",
                'refined_dataset': "{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['refined_dataset'] }}",
                'pubsub_topic': "projects/{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}/topics/{{ ti.xcom_pull(task_ids='load_config', key='config')['streaming']['pubsub']['topic'] }}",
                'window_duration_hours': "{{ ti.xcom_pull(task_ids='load_config', key='config')['streaming']['window_duration_hours'] }}",
            },
            py_requirements="{{ ti.xcom_pull(task_ids='load_config', key='config')['dataflow']['python_requirements'] }}",
            py_interpreter='python3',
            gcp_conn_id='google_cloud_default',
        )
        
        # Initial health check
        initial_health_check = PythonOperator(
            task_id='initial_health_check',
            python_callable=validate_streaming_health,
            provide_context=True,
            trigger_rule='all_done',
        )
        
        create_topic >> create_subscription >> start_streaming_job >> initial_health_check
    
    # Branch 2: Monitor existing job
    monitor_existing_job = PythonOperator(
        task_id='monitor_existing_job',
        python_callable=monitor_existing_streaming_job,
        provide_context=True,
    )
    
    # Convergence point
    streaming_health_check = PythonOperator(
        task_id='streaming_health_check',
        python_callable=validate_streaming_health,
        provide_context=True,
        trigger_rule='none_failed_or_skipped',
    )
    
    # Write health metrics to BigQuery
    log_health_metrics = BigQueryInsertJobOperator(
        task_id='log_health_metrics',
        configuration={
            "query": {
                "query": f"""
                INSERT INTO `{{{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}}}.{{{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['staging_dataset'] }}}}.streaming_health_log`
                SELECT 
                    CURRENT_TIMESTAMP() as check_timestamp,
                    '{{{{ ti.xcom_pull(task_ids='load_config', key='config')['pipeline']['term_type'] }}}}' as term_type,
                    '{{{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['environment'] }}}}' as environment,
                    '{{{{ ti.xcom_pull(task_ids='streaming_health_check') }}}}' as health_metrics,
                    CASE 
                        WHEN '{{{{ ti.xcom_pull(task_ids='streaming_health_check') }}}}' LIKE '%ALERT%' THEN 'CRITICAL'
                        WHEN '{{{{ ti.xcom_pull(task_ids='streaming_health_check') }}}}' LIKE '%WARNING%' THEN 'WARNING'
                        ELSE 'HEALTHY'
                    END as overall_status
                """,
                "useLegacySql": False,
                "priority": "INTERACTIVE"
            }
        },
        gcp_conn_id='google_cloud_default',
        location="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['location'] }}",
    )
    
    # End marker
    end_pipeline = DummyOperator(
        task_id='end_pipeline',
        trigger_rule='none_failed_or_skipped'
    )
    
    # Define DAG flow
    start_pipeline >> load_config_task >> check_job_status
    check_job_status >> [new_job_group, monitor_existing_job]
    [new_job_group, monitor_existing_job] >> streaming_health_check
    streaming_health_check >> log_health_metrics >> end_pipeline
