#!/usr/bin/env python
"""
Optimized Real-time Streaming Pipeline for MS Member Data
Version: 2.0 - Refactored with Clear Steps
"""

import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window, trigger
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from google.cloud import bigquery, bigtable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURATION
# ============================================
class PipelineConfig:
    """Centralized configuration"""
    # GCP Settings
    PROJECT_ID = "the1-insight-dev"
    DATASET = "insight_dev"
    REGION = "asia-southeast1"
    
    # PubSub Settings
    SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/personas-updates"
    DLQ_TOPIC = f"projects/{PROJECT_ID}/topics/personas-dlq"
    
    # BigTable Settings
    BT_INSTANCE = "personas-instance"
    BT_TABLE = "personas-data"
    BT_COLUMN_FAMILY = "profiles"  # ใช้แค่ profiles
    
    # BigQuery Settings
    BQ_OUTPUT_TABLE = f"{PROJECT_ID}.{DATASET}.ms_personas_streaming"
    BQ_MAPPING_TABLE = f"{PROJECT_ID}.{DATASET}.stg_mapping_reconcile"
    
    # Iceberg Settings (for future BigLake setup)
    ICEBERG_BUCKET = "t1-insight-data-bucket"
    ICEBERG_PATH = f"gs://{ICEBERG_BUCKET}/iceberg/table/ms_personas"
    
    # Cache Settings
    CACHE_TTL_SECONDS = 3600  # 1 hour
    
    # Retry Settings
    MAX_RETRY_ATTEMPTS = 10
    RETRY_INTERVAL_SECONDS = 30  # Fixed interval
    
    # Default Values for Data Types
    DEFAULT_VALUES = {
        'string': '',
        'integer': 0,
        'float': 0.0,
        'boolean': False,
        'timestamp': None,
        'array': [],
        'object': {}
    }

# ============================================
# STEP 1: Query and Cache Mapping
# ============================================
class MappingCache(beam.DoFn):
    """
    Step 1: Query mapping from BigQuery and cache it
    Cache refreshes every hour
    """
    _cache = None
    _cache_time = 0
    
    def __init__(self, project_id, dataset):
        self.project_id = project_id
        self.dataset = dataset
        self._bq_client = None
    
    def setup(self):
        """Initialize BigQuery client"""
        self._bq_client = bigquery.Client(project=self.project_id)
    
    def process(self, element):
        """Query and cache mapping data"""
        current_time = time.time()
        
        # Check if cache is still valid
        if (MappingCache._cache and 
            (current_time - MappingCache._cache_time < PipelineConfig.CACHE_TTL_SECONDS)):
            logger.info("Using cached mapping")
            yield MappingCache._cache
            return
        
        # Build query
        query = f"""
        SELECT 
            RECONCILE_COLUMN_NAME,
            PERSONAS_MAPPING_COLUMN_NAME,
            RECONCILE_RETRIEVED,
            RECONCILE_CONFIRMED
        FROM `{self.project_id}.{self.dataset}.stg_mapping_reconcile`
        WHERE COALESCE(UPDATED_DATE, '1999-12-31') = (
            SELECT COALESCE(MAX(UPDATED_DATE), '1999-12-31')
            FROM `{self.project_id}.{self.dataset}.stg_mapping_reconcile`
        )
        """
        
        logger.info(f"Querying mapping from BigQuery")
        
        try:
            # Execute query
            query_job = self._bq_client.query(query)
            rows = list(query_job.result())
            
            # Build mapping dictionary
            mapping_dict = {}
            for row in rows:
                dest_col = row.RECONCILE_COLUMN_NAME
                src_col = row.PERSONAS_MAPPING_COLUMN_NAME
                
                if dest_col and src_col:
                    # Parse nested path (e.g., "profiles.memberId" -> ["profiles", "memberId"])
                    mapping_dict[dest_col] = {
                        'source_path': src_col.split('.'),
                        'reconcile_retrieved': bool(row.RECONCILE_RETRIEVED),
                        'reconcile_confirmed': bool(row.RECONCILE_CONFIRMED)
                    }
            
            # Update cache
            MappingCache._cache = mapping_dict
            MappingCache._cache_time = current_time
            
            logger.info(f"Cached {len(mapping_dict)} mapping entries")
            yield mapping_dict
            
        except Exception as e:
            logger.error(f"Failed to query mapping: {e}")
            # Return existing cache if available
            if MappingCache._cache:
                yield MappingCache._cache
            else:
                yield {}

# ============================================
# STEP 2 & 3: Process PubSub and Extract PersonasId
# ============================================
class ProcessPubSubMessage(beam.DoFn):
    """
    Step 2-3: Consume PubSub and extract personasId
    """
    def process(self, element):
        """Process incoming PubSub message"""
        try:
            # Parse message
            if hasattr(element, 'data'):
                data = element.data
                if isinstance(data, bytes):
                    data = data.decode('utf-8')
                attributes = element.attributes or {}
            else:
                data = element if isinstance(element, str) else json.dumps(element)
                attributes = {}
            
            # Parse JSON
            message = json.loads(data) if isinstance(data, str) else data
            
            # Extract personasId (ใช้ personasId แทน member_id)
            personas_id = message.get('personasId')
            
            if not personas_id:
                raise ValueError("No personasId found in message")
            
            # Output successful extraction
            yield beam.pvalue.TaggedOutput('success', {
                'personas_id': personas_id,
                'original_message': message,
                'attributes': attributes,
                'received_at': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Failed to process message: {e}")
            # Send to DLQ
            yield beam.pvalue.TaggedOutput('dlq', {
                'data': str(element),
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'retry_count': 0
            })

# ============================================
# STEP 4: BigTable Lookup
# ============================================
class BigTableLookup(beam.DoFn):
    """
    Step 4: Lookup data from BigTable using personasId
    """
    def __init__(self, project_id, instance_id, table_id, column_family='profiles'):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.column_family = column_family
        self._table = None
    
    def setup(self):
        """Initialize BigTable client"""
        client = bigtable.Client(project=self.project_id)
        instance = client.instance(self.instance_id)
        self._table = instance.table(self.table_id)
    
    def process(self, element):
        """Lookup data from BigTable"""
        personas_id = element.get('personas_id')
        
        if not personas_id:
            logger.warning("No personas_id in element, skipping")
            return
        
        try:
            # Row key is just personasId (no prefix)
            row_key = str(personas_id).encode()
            
            # Read from BigTable
            row = self._table.read_row(row_key)
            
            if row is None:
                logger.info(f"No data found for personasId: {personas_id}, skipping")
                return  # Skip if not found
            
            # Parse BigTable data
            result = {
                'personas_id': personas_id,
                'original_message': element.get('original_message', {}),
                'bigtable_data': {}
            }
            
            # Extract data from profiles column family
            if self.column_family in row.cells:
                family_data = row.cells[self.column_family]
                
                for column, cells in family_data.items():
                    col_name = column.decode('utf-8')
                    cell_value = cells[0].value.decode('utf-8')
                    
                    # Try to parse as JSON
                    try:
                        parsed_value = json.loads(cell_value)
                    except:
                        parsed_value = cell_value
                    
                    result['bigtable_data'][col_name] = parsed_value
            
            logger.info(f"Retrieved data for personasId: {personas_id}")
            yield result
            
        except Exception as e:
            logger.error(f"BigTable lookup failed for {personas_id}: {e}")
            # Could send to DLQ here if needed

# ============================================
# STEP 5: Flatten and Apply Mapping
# ============================================
class ApplyMapping(beam.DoFn):
    """
    Step 5: Flatten nested JSON and apply column mapping
    """
    
    @staticmethod
    def get_nested_value(data, path):
        """Navigate nested dictionary using path"""
        value = data
        for key in path:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
            if value is None:
                break
        return value
    
    @staticmethod
    def infer_data_type(value):
        """Infer data type from value"""
        if value is None:
            return 'string'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, list):
            return 'array'
        elif isinstance(value, dict):
            return 'object'
        elif isinstance(value, str):
            # Check if it's a timestamp
            try:
                datetime.fromisoformat(value.replace('Z', '+00:00'))
                return 'timestamp'
            except:
                return 'string'
        else:
            return 'string'
    
    def process(self, element, mapping_dict):
        """Apply mapping to flatten and rename fields"""
        if not mapping_dict:
            logger.warning("No mapping dictionary provided")
            yield element
            return
        
        # Combine original message and bigtable data
        source_data = {}
        source_data.update(element.get('original_message', {}))
        source_data.update(element.get('bigtable_data', {}))
        
        # Apply mapping
        mapped_record = {}
        
        for dest_column, mapping_config in mapping_dict.items():
            source_path = mapping_config.get('source_path', [])
            
            # Get value from nested path
            value = self.get_nested_value(source_data, source_path)
            
            # Apply default value if None
            if value is None:
                data_type = self.infer_data_type(value)
                value = PipelineConfig.DEFAULT_VALUES.get(data_type, None)
            
            mapped_record[dest_column] = value
        
        # Add metadata
        mapped_record['_personas_id'] = element.get('personas_id')
        mapped_record['_processed_at'] = datetime.now().isoformat()
        mapped_record['_pipeline_version'] = '2.0'
        
        # Ensure primary key exists
        if 'member_number' not in mapped_record:
            # Try to extract from profiles.memberId or similar
            member_id = self.get_nested_value(
                source_data, 
                ['profiles', 'memberId']
            )
            if member_id:
                mapped_record['member_number'] = member_id
        
        logger.info(f"Mapped record for personas_id: {element.get('personas_id')}")
        yield mapped_record

# ============================================
# STEP 6: Write to BigQuery with CDC
# ============================================
class BigQueryWriter:
    """Helper class for BigQuery writing configurations"""
    
    @staticmethod
    def get_table_schema():
        """Define BigQuery table schema"""
        # This would normally be more comprehensive
        return 'SCHEMA_AUTODETECT'
    
    @staticmethod
    def prepare_for_bigquery(element):
        """Prepare record for BigQuery insertion"""
        # Remove internal fields starting with _
        clean_record = {
            k: v for k, v in element.items() 
            if not k.startswith('_') or k == '_processed_at'
        }
        
        # Rename _processed_at to processed_at for BigQuery
        if '_processed_at' in clean_record:
            clean_record['processed_at'] = clean_record.pop('_processed_at')
        
        return clean_record

# ============================================
# STEP 7: DLQ Retry Mechanism
# ============================================
class DLQRetryHandler(beam.DoFn):
    """
    Handle DLQ messages with retry logic
    Fixed interval retry with max attempts
    """
    
    def process(self, element):
        """Process DLQ message and decide on retry"""
        retry_count = element.get('retry_count', 0)
        
        if retry_count >= PipelineConfig.MAX_RETRY_ATTEMPTS:
            logger.error(f"Max retries reached for message: {element.get('data')}")
            # Send to permanent DLQ or alert
            yield beam.pvalue.TaggedOutput('permanent_dlq', element)
        else:
            # Increment retry count
            element['retry_count'] = retry_count + 1
            element['next_retry_at'] = (
                datetime.now() + 
                timedelta(seconds=PipelineConfig.RETRY_INTERVAL_SECONDS)
            ).isoformat()
            
            logger.info(f"Scheduling retry {retry_count + 1} for message")
            yield beam.pvalue.TaggedOutput('retry', element)

# ============================================
# MAIN PIPELINE
# ============================================
def create_pipeline_options():
    """Create pipeline options"""
    return PipelineOptions([
        f'--project={PipelineConfig.PROJECT_ID}',
        f'--region={PipelineConfig.REGION}',
        '--runner=DataflowRunner',
        '--streaming',
        '--enable_streaming_engine',
        '--worker_machine_type=n1-standard-2',
        '--max_num_workers=10',
        '--autoscaling_algorithm=THROUGHPUT_BASED',
        '--use_public_ips=false',
        '--enable_hot_key_logging',
        f'--temp_location=gs://{PipelineConfig.PROJECT_ID}-dataflow-temp/temp',
        f'--staging_location=gs://{PipelineConfig.PROJECT_ID}-dataflow-temp/staging',
    ])

def run_streaming_pipeline():
    """Main pipeline execution"""
    
    options = create_pipeline_options()
    
    with beam.Pipeline(options=options) as pipeline:
        
        # ============================================
        # STEP 1: Create Mapping Side Input
        # ============================================
        mapping_side_input = (
            pipeline
            | 'TriggerMappingUpdate' >> PeriodicImpulse(
                fire_interval=PipelineConfig.CACHE_TTL_SECONDS
            )
            | 'QueryMapping' >> beam.ParDo(
                MappingCache(
                    PipelineConfig.PROJECT_ID,
                    PipelineConfig.DATASET
                )
            )
            | 'WindowMapping' >> beam.WindowInto(
                window.GlobalWindows(),
                trigger=trigger.Repeatedly(
                    trigger.AfterProcessingTime(1)
                ),
                accumulation_mode=trigger.AccumulationMode.DISCARDING
            )
        )
        
        # ============================================
        # STEP 2-3: Read PubSub and Extract PersonasId
        # ============================================
        messages = (
            pipeline
            | 'ReadFromPubSub' >> ReadFromPubSub(
                subscription=PipelineConfig.SUBSCRIPTION,
                with_attributes=True,
                id_label='message_id',
                timestamp_attribute='publish_time'
            )
            | 'ProcessMessages' >> beam.ParDo(
                ProcessPubSubMessage()
            ).with_outputs('success', 'dlq')
        )
        
        # ============================================
        # STEP 4: BigTable Lookup
        # ============================================
        enriched = (
            messages.success
            | 'LookupBigTable' >> beam.ParDo(
                BigTableLookup(
                    PipelineConfig.PROJECT_ID,
                    PipelineConfig.BT_INSTANCE,
                    PipelineConfig.BT_TABLE,
                    PipelineConfig.BT_COLUMN_FAMILY
                )
            )
        )
        
        # ============================================
        # STEP 5: Apply Mapping
        # ============================================
        mapped_records = (
            enriched
            | 'ApplyMapping' >> beam.ParDo(
                ApplyMapping(),
                mapping_dict=beam.pvalue.AsSingleton(mapping_side_input)
            )
        )
        
        # ============================================
        # STEP 6: Write to BigQuery
        # ============================================
        
        # 6.1: Native CDC Table (with primary key)
        _ = (
            mapped_records
            | 'PrepareForBQ' >> beam.Map(BigQueryWriter.prepare_for_bigquery)
            | 'WriteToBigQuery_CDC' >> WriteToBigQuery(
                table=PipelineConfig.BQ_OUTPUT_TABLE,
                schema=BigQueryWriter.get_table_schema(),
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                method=WriteToBigQuery.Method.STORAGE_WRITE_API,
                use_cdc_writes=True,
                primary_key=['member_number']
            )
        )
        
        # 6.2: Future Iceberg/BigLake Configuration
        # Note: This requires BigLake connection setup first
        """
        # Create BigLake external table:
        CREATE OR REPLACE EXTERNAL TABLE `{project}.{dataset}.ms_personas_iceberg`
        WITH CONNECTION `{project}.{region}.biglake_connection`
        OPTIONS (
            format = 'ICEBERG',
            uris = ['{PipelineConfig.ICEBERG_PATH}']
        );
        """
        
        # ============================================
        # STEP 7: DLQ Handling with Retry
        # ============================================
        dlq_processed = (
            messages.dlq
            | 'ProcessDLQ' >> beam.ParDo(
                DLQRetryHandler()
            ).with_outputs('retry', 'permanent_dlq')
        )
        
        # Retry messages (would need additional logic to re-process)
        _ = (
            dlq_processed.retry
            | 'SerializeRetry' >> beam.Map(json.dumps)
            | 'PublishRetry' >> WriteToPubSub(
                topic=PipelineConfig.DLQ_TOPIC
            )
        )
        
        # Permanent failures (could write to GCS or alert)
        _ = (
            dlq_processed.permanent_dlq
            | 'LogPermanentDLQ' >> beam.Map(
                lambda x: logger.critical(f"Permanent DLQ: {x}")
            )
        )
        
        logger.info("Pipeline started successfully")

# ============================================
# ENTRY POINT
# ============================================
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_streaming_pipeline()