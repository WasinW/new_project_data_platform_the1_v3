"""
Airflow DAG for Reconciliation Pipeline (Refactored)
Uses configuration from YAML file for better maintainability
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery_dts import (
    BigQueryDataTransferServiceStartTransferRunsOperator
)
from airflow.providers.google.cloud.sensors.bigquery_dts import (
    BigQueryDataTransferServiceTransferRunSensor
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryGetDataOperator
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
CONFIG_PATH = os.environ.get('RECONCILE_CONFIG',
                            '/home/airflow/gcs/dags/composer/config/ms_member/reconcile/full_patch_config.yaml')


def load_config(**context):
    """Load configuration from YAML file"""
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
    
    context['ti'].xcom_push(key='config', value=config)
    logger.info(f"Loaded configuration for {config['pipeline']['name']}")
    return config


class ReconciliationSQLGenerator:
    """Class to generate reconciliation SQL statements"""
    
    def __init__(self, project_id, dataset_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        
    def generate_reconciliation_sql(self, mapping_records):
        """Generate SQL for both ms_personas and ms_member reconciliation"""
        
        # Initialize column lists
        list_stmt_reconcile_column = []
        list_stmt_reconciled_confirm_column = []
        
        # Process each mapping record
        for record in mapping_records:
            data_schema = record['data_schema']
            tech_schema = record.get('tech_schema', '') or data_schema
            reconcile_sts = record['reconcile_sts']
            reconciled_confirm = record['reconciled_confirm']
            
            # For ms_personas (reconcile new columns)
            if reconcile_sts == 'Y':
                list_stmt_reconcile_column.append(
                    f"COALESCE(n.{tech_schema}, o.{data_schema}) AS {tech_schema}"
                )
            else:
                list_stmt_reconcile_column.append(
                    f"o.{data_schema} AS {tech_schema}"
                )
            
            # For ms_member (backfill confirmed columns)
            if reconciled_confirm == 'Y':
                list_stmt_reconciled_confirm_column.append(
                    f"COALESCE(n.{tech_schema}, o.{data_schema}) AS {data_schema}"
                )
            else:
                list_stmt_reconciled_confirm_column.append(
                    f"o.{data_schema} AS {data_schema}"
                )
        
        # Generate SQL for ms_personas
        stmt_reconcile_column = f"""
        -- Reconciliation SQL for ms_personas
        -- Generated at: {datetime.now().isoformat()}
        CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_id}.ms_personas` AS
        SELECT 
            o.member_id,
            {chr(10) + '    , '.join(list_stmt_reconcile_column)},
            CURRENT_TIMESTAMP() AS reconciled_timestamp,
            '{datetime.now().strftime("%Y%m%d")}' AS reconciliation_batch
        FROM `{self.project_id}.{self.dataset_id}.ms_member` o
        LEFT JOIN `{self.project_id}.{self.dataset_id}.ms_personas` n
            ON o.member_id = n.member_id
        """
        
        # Generate SQL for ms_member backfill
        stmt_reconciled_confirm_column = f"""
        -- Backfill SQL for ms_member
        -- Generated at: {datetime.now().isoformat()}
        MERGE `{self.project_id}.{self.dataset_id}.ms_member` t
        USING (
            SELECT 
                o.member_id,
                {chr(10) + '        , '.join(list_stmt_reconciled_confirm_column)}
            FROM `{self.project_id}.{self.dataset_id}.ms_member` o
            LEFT JOIN `{self.project_id}.{self.dataset_id}.ms_personas` n
                ON o.member_id = n.member_id
        ) s
        ON t.member_id = s.member_id
        WHEN MATCHED THEN
            UPDATE SET 
                {', '.join([f"{col.split(' AS ')[1]} = s.{col.split(' AS ')[1]}" 
                           for col in list_stmt_reconciled_confirm_column 
                           if 'COALESCE' in col])},
                backfill_timestamp = CURRENT_TIMESTAMP()
        """
        
        return {
            'personas_sql': stmt_reconcile_column,
            'member_sql': stmt_reconciled_confirm_column,
            'summary': {
                'total_columns': len(mapping_records),
                'reconciling': sum(1 for r in mapping_records if r['reconcile_sts'] == 'Y'),
                'confirmed': sum(1 for r in mapping_records if r['reconciled_confirm'] == 'Y')
            }
        }


def _s3_uri_to_fqn(s3_uri: str) -> str:
    """Convert S3 URI to fully qualified name"""
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
    """Get transfer configuration"""
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
    table = config['tables']['ms_member_table']
    
    try:
        # Get transfer details
        cfg = _get_transfer_config(project_id, location, transfer_config_id)
        run = _get_transfer_run(project_id, location, transfer_config_id, run_id)
        
        # Extract S3 URI
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
        
        # Emit lineage
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
        
        logger.info(f"Successfully emitted lineage for {s3_fqn} -> {bq_fqn}")
        
    except Exception as e:
        logger.error(f"Failed to emit lineage: {str(e)}")
        pass


def process_mapping_and_generate_sql(**context):
    """Task function to process mapping data and generate SQL"""
    config = context['ti'].xcom_pull(task_ids='load_config', key='config')
    ti = context['ti']
    
    # Get mapping data from previous task
    mapping_data = ti.xcom_pull(task_ids='get_mapping_data')
    
    if not mapping_data:
        raise ValueError("No mapping data received from BigQuery")
    
    logger.info(f"Processing {len(mapping_data)} mapping records")
    
    # Initialize SQL generator
    generator = ReconciliationSQLGenerator(
        config['gcp']['project_id'],
        config['datasets']['staging_dataset']
    )
    
    # Generate SQL statements
    sql_dict = generator.generate_reconciliation_sql(mapping_data)
    
    logger.info(f"Generated SQL for reconciliation:")
    logger.info(f"- Reconciling columns: {sql_dict['summary']['reconciling']}")
    logger.info(f"- Confirmed columns: {sql_dict['summary']['confirmed']}")
    
    # Push SQL to XCom for next tasks
    ti.xcom_push(key='personas_sql', value=sql_dict['personas_sql'])
    ti.xcom_push(key='member_sql', value=sql_dict['member_sql'])
    ti.xcom_push(key='sql_summary', value=sql_dict['summary'])
    
    return sql_dict['summary']


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
    
    # Task 1: Trigger mapping_reconcile transfer from S3 to BigQuery
    trigger_mapping_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="trigger_mapping_reconcile_transfer",
        project_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}",
        location="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['location'] }}",
        transfer_config_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['transfer']['mapping_transfer_config_id'] }}",
        requested_run_time={"seconds": int(time.time())},
        gcp_conn_id='google_cloud_default',
        deferrable=True,
    )

    # Task 2: Monitor mapping transfer completion
    monitor_mapping_transfer = BigQueryDataTransferServiceTransferRunSensor(
        task_id="monitor_mapping_transfer",
        transfer_config_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['transfer']['mapping_transfer_config_id'] }}",
        run_id="{{ ti.xcom_pull(task_ids='trigger_mapping_reconcile_transfer', key='run_id') }}",
        expected_statuses={"SUCCEEDED"},
        project_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}",
        location="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['location'] }}",
        # poke_interval="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['poke_interval_seconds'] }}",
        # timeout="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['mapping_timeout_seconds'] }}",
        # mode="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['mode'] }}",
        poke_interval=60,  # ใช้ค่าตัวเลขโดยตรง
        timeout=600,       # ใช้ค่าตัวเลขโดยตรง  
        mode="poke",       # ใช้ค่า string โดยตรง
        gcp_conn_id='google_cloud_default',
    )

    # Task 3: Trigger ms_member transfer from S3 to BigQuery
    trigger_member_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="trigger_ms_member_transfer",
        project_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}",
        location="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['location'] }}",
        transfer_config_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['transfer']['member_transfer_config_id'] }}",
        requested_run_time={"seconds": int(time.time())},
        gcp_conn_id='google_cloud_default',
        deferrable=True,
    )

    # Task 4: Monitor member transfer completion
    monitor_member_transfer = BigQueryDataTransferServiceTransferRunSensor(
        task_id="monitor_member_transfer",
        transfer_config_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['transfer']['member_transfer_config_id'] }}",
        run_id="{{ ti.xcom_pull(task_ids='trigger_ms_member_transfer', key='run_id') }}",
        expected_statuses={"SUCCEEDED"},
        project_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}",
        location="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['location'] }}",
        # poke_interval="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['poke_interval_seconds'] }}",
        # timeout="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['member_timeout_seconds'] }}",
        # mode="{{ ti.xcom_pull(task_ids='load_config', key='config')['monitoring']['mode'] }}",
        poke_interval=60,  # ใช้ค่าตัวเลขโดยตรง
        timeout=600,       # ใช้ค่าตัวเลขโดยตรง  
        mode="poke",       # ใช้ค่า string โดยตรง
        gcp_conn_id='google_cloud_default',
    )

    # Emit lineage for member transfer
    audit_lineage_member = PythonOperator(
        task_id="audit_lineage_member",
        python_callable=emit_lineage_for_s3_to_bq,
        provide_context=True,
    )

    # Task 5: Get mapping data from BigQuery
    get_mapping_data = BigQueryGetDataOperator(
        task_id='get_mapping_data',
        dataset_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['staging_dataset'] }}",
        table_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['tables']['mapping_table'] }}",
        project_id="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}",
        max_results="{{ ti.xcom_pull(task_ids='load_config', key='config')['queries']['max_mapping_results'] }}",
        selected_fields="{{ ','.join(ti.xcom_pull(task_ids='load_config', key='config')['queries']['mapping_fields']) }}",
        gcp_conn_id='google_cloud_default',
        location="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['location'] }}",
    )

    # Task 6: Process mapping and generate SQL
    generate_sql = PythonOperator(
        task_id='generate_reconciliation_sql',
        python_callable=process_mapping_and_generate_sql,
        provide_context=True,
    )

    # Task 7: Execute reconciliation SQL for ms_personas
    execute_personas_reconciliation = BigQueryInsertJobOperator(
        task_id="execute_personas_reconciliation",
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(task_ids='generate_reconciliation_sql', key='personas_sql') }}",
                "useLegacySql": "{{ ti.xcom_pull(task_ids='load_config', key='config')['bigquery']['use_legacy_sql'] }}",
                "priority": "{{ ti.xcom_pull(task_ids='load_config', key='config')['bigquery']['priority'] }}",
                "labels": {
                    "pipeline": "reconciliation",
                    "table": "ms_personas",
                    "execution_date": "{{ ds }}"
                }
            }
        },
        gcp_conn_id='google_cloud_default',
        location="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['location'] }}",
    )

    # Task 8: Execute backfill SQL for ms_member
    execute_member_backfill = BigQueryInsertJobOperator(
        task_id="execute_member_backfill",
        configuration={
            "query": {
                "query": """
                {% set summary = ti.xcom_pull(task_ids='generate_reconciliation_sql', key='sql_summary') %}
                {% if summary and summary['confirmed'] > 0 %}
                    {{ ti.xcom_pull(task_ids='generate_reconciliation_sql', key='member_sql') }}
                {% else %}
                    SELECT 'No confirmed columns to backfill' as message
                {% endif %}
                """,
                "useLegacySql": False,
                "priority": "{{ ti.xcom_pull(task_ids='load_config', key='config')['bigquery']['priority'] }}",
                "labels": {
                    "pipeline": "reconciliation",
                    "table": "ms_member",
                    "execution_date": "{{ ds }}"
                }
            }
        },
        gcp_conn_id='google_cloud_default',
        location="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['location'] }}",
    )

    # Task 9: Validation check
    validate_reconciliation = BigQueryInsertJobOperator(
        task_id="validate_reconciliation",
        configuration={
            "query": {
                "query": f"""
                -- Validation query
                WITH validation_summary AS (
                    SELECT 
                        'ms_personas' as table_name,
                        COUNT(*) as record_count,
                        MAX(reconciled_timestamp) as last_reconciled
                    FROM `{{{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}}}.{{{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['staging_dataset'] }}}}.{{{{ ti.xcom_pull(task_ids='load_config', key='config')['tables']['ms_personas_table'] }}}}` 
                    
                    UNION ALL
                    
                    SELECT 
                        'ms_member' as table_name,
                        COUNT(*) as record_count,
                        MAX(backfill_timestamp) as last_reconciled
                    FROM `{{{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}}}.{{{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['staging_dataset'] }}}}.{{{{ ti.xcom_pull(task_ids='load_config', key='config')['tables']['ms_member_table'] }}}}` 
                ),
                mapping_summary AS (
                    SELECT 
                        COUNT(*) as total_columns,
                        SUM(CASE WHEN reconcile_sts = 'Y' THEN 1 ELSE 0 END) as reconciling,
                        SUM(CASE WHEN reconciled_confirm = 'Y' THEN 1 ELSE 0 END) as confirmed
                    FROM `{{{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}}}.{{{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['staging_dataset'] }}}}.{{{{ ti.xcom_pull(task_ids='load_config', key='config')['tables']['mapping_table'] }}}}` 
                    WHERE data_table = 'ms_member'
                        AND update_dt = (SELECT MAX(update_dt) FROM `{{{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}}}.{{{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['staging_dataset'] }}}}.{{{{ ti.xcom_pull(task_ids='load_config', key='config')['tables']['mapping_table'] }}}}`)
                )
                SELECT 
                    v.*,
                    m.total_columns,
                    m.reconciling,
                    m.confirmed,
                    CURRENT_TIMESTAMP() as validation_timestamp
                FROM validation_summary v
                CROSS JOIN mapping_summary m
                """,
                "useLegacySql": False,
                "writeDisposition": "{{ ti.xcom_pull(task_ids='load_config', key='config')['bigquery']['write_disposition'] }}",
                "destinationTable": {
                    "projectId": "{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['project_id'] }}",
                    "datasetId": "{{ ti.xcom_pull(task_ids='load_config', key='config')['datasets']['staging_dataset'] }}",
                    "tableId": "{{ ti.xcom_pull(task_ids='load_config', key='config')['tables']['validation_log_table'] }}"
                }
            }
        },
        gcp_conn_id='google_cloud_default',
        location="{{ ti.xcom_pull(task_ids='load_config', key='config')['gcp']['location'] }}",
    )

    # Define task dependencies
    load_config_task >> trigger_mapping_transfer >> monitor_mapping_transfer
    load_config_task >> trigger_member_transfer >> monitor_member_transfer >> audit_lineage_member
    
    [monitor_mapping_transfer, audit_lineage_member] >> get_mapping_data
    get_mapping_data >> generate_sql
    generate_sql >> [execute_personas_reconciliation, execute_member_backfill]
    [execute_personas_reconciliation, execute_member_backfill] >> validate_reconciliation
