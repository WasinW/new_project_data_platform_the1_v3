#!/usr/bin/env python
"""
Real-time streaming pipeline for MS Member data - Optimized Version
Using Side Input Pattern and Enrichment API
"""

import json
import logging
import time
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window, trigger
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery , BigQueryDisposition
from apache_beam.transforms.periodicsequence import PeriodicSequence
import pyarrow as pa
import pyarrow.parquet as pq
import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = "the1-insight-dev"
DATASET = "insight_dev"
REGION = "asia-southeast1"

# Pub/Sub settings
SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/personas-updates"
DLQ_TOPIC = f"projects/{PROJECT_ID}/topics/personas-dlq"

# BigTable settings
BT_INSTANCE = "personas-instance"
BT_TABLE = "personas-data"

# Output settings
BQ_OUTPUT_TABLE = f"{PROJECT_ID}.{DATASET}.ms_personas_streaming"
S3_BUCKET = "t1-analytics"
S3_PREFIX = "refined/insights/ms_personas_streaming"

# AWS credentials (should use Secret Manager in production)
AWS_ACCESS_KEY = "YOUR_AWS_ACCESS_KEY"
AWS_SECRET_KEY = "YOUR_AWS_SECRET_KEY"

# ============================================
# PART 1: Side Input Pattern for BigQuery Mapping
# ============================================

def create_mapping_query() -> str:
    """Generate the mapping query"""
    return f"""
    SELECT RECONCILE_COLUMN_NAME,
           PERSONAS_MAPPING_COLUMN_NAME,
           RECONCILE_RETRIEVED,
           RECONCILE_CONFIRMED
    FROM `{PROJECT_ID}.{DATASET}.stg_mapping_reconcile`
    WHERE COALESCE(UPDATED_DATE, '1999-12-31') = (
        SELECT COALESCE(MAX(UPDATED_DATE), '1999-12-31')
        FROM `{PROJECT_ID}.{DATASET}.stg_mapping_reconcile`
    )
    """

class BuildMappingDict(beam.CombineFn):
    """Combine function to build mapping dictionary from rows"""
    
    def create_accumulator(self):
        return {}
    
    def add_input(self, accumulator, row):
        dest_col = row.get('RECONCILE_COLUMN_NAME')
        src_col = row.get('PERSONAS_MAPPING_COLUMN_NAME')
        
        if dest_col and src_col:
            accumulator[dest_col] = {
                'src_path': src_col.split('.'),
                'reconcile': bool(row.get('RECONCILE_RETRIEVED')),
                'confirmed': bool(row.get('RECONCILE_CONFIRMED'))
            }
        return accumulator
    
    def merge_accumulators(self, accumulators):
        result = {}
        for acc in accumulators:
            result.update(acc)
        return result
    
    def extract_output(self, accumulator):
        return accumulator

def create_mapping_side_input(p):
    """Create a slowly changing side input for mapping"""
    
    # Use PeriodicSequence to trigger updates every hour
    mapping_updates = (
        p
        | 'TriggerHourly' >> PeriodicSequence(
            start=beam.utils.timestamp.Timestamp.now(),
            stop=beam.utils.timestamp.Timestamp.of(datetime.max),
            fire_interval=3600  # Every hour
        )
        | 'CreateQuery' >> beam.Map(lambda _: create_mapping_query())
        | 'ReadFromBigQuery' >> beam.FlatMap(
            lambda query: beam.io.ReadFromBigQuery(
                query=query,
                use_standard_sql=True,
                project=PROJECT_ID,
                gcs_location=f'gs://{PROJECT_ID}-dataflow-temp/temp'
            ).expand(beam.Pipeline())  # Note: This is a simplified approach
        )
        | 'BuildMapping' >> beam.CombineGlobally(BuildMappingDict()).without_defaults()
        | 'WindowIntoGlobal' >> beam.WindowInto(
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterProcessingTime(1)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING
        )
    )
    
    return beam.pvalue.AsSingleton(mapping_updates)

# Alternative: Simpler approach using a DoFn with caching
class QueryMappingWithCache(beam.DoFn):
    """Simplified version with internal caching"""
    
    _cache = None
    _cache_time = 0
    CACHE_TTL = 3600
    
    def __init__(self):
        from google.cloud import bigquery
        self._bq_client = None
        
    def setup(self):
        from google.cloud import bigquery
        self._bq_client = bigquery.Client(project=PROJECT_ID)
    
    def process(self, element):
        current_time = time.time()
        
        # Check cache
        if QueryMappingWithCache._cache and \
           (current_time - QueryMappingWithCache._cache_time < self.CACHE_TTL):
            yield QueryMappingWithCache._cache
            return
        
        # Query and build mapping
        query_job = self._bq_client.query(create_mapping_query())
        rows = list(query_job.result())
        
        mapping_dict = {}
        for row in rows:
            dest_col = row.RECONCILE_COLUMN_NAME
            src_col = row.PERSONAS_MAPPING_COLUMN_NAME
            
            if dest_col and src_col:
                mapping_dict[dest_col] = {
                    'src_path': src_col.split('.'),
                    'reconcile': bool(row.RECONCILE_RETRIEVED),
                    'confirmed': bool(row.RECONCILE_CONFIRMED)
                }
        
        # Update cache
        QueryMappingWithCache._cache = mapping_dict
        QueryMappingWithCache._cache_time = current_time
        
        logger.info(f"Updated mapping cache with {len(mapping_dict)} entries")
        yield mapping_dict

# ============================================
# PART 2: Optimized BigTable Enrichment
# ============================================

# For Beam 2.54+: Using Enrichment API
try:
    from apache_beam.transforms.enrichment import Enrichment
    from apache_beam.transforms.enrichment_handlers.bigtable import BigTableEnrichmentHandler
    
    def create_bigtable_enrichment():
        """Create BigTable enrichment handler"""
        return BigTableEnrichmentHandler(
            project_id=PROJECT_ID,
            instance_id=BT_INSTANCE,
            table_id=BT_TABLE,
            row_key_fn=lambda element: f"member#{element['member_id']}".encode(),
            column_families=['profiles', 'attributes']  # Specify needed families
        )
    
    USE_ENRICHMENT_API = True
except ImportError:
    USE_ENRICHMENT_API = False
    logger.warning("Enrichment API not available, using fallback")

# Fallback: Optimized batch lookup for older Beam versions
class OptimizedBigTableLookup(beam.DoFn):
    """Optimized BigTable lookup with batching and connection pooling"""
    
    def __init__(self, project_id, instance_id, table_id):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self._table = None
        
    def setup(self):
        from google.cloud import bigtable
        client = bigtable.Client(project=self.project_id)
        instance = client.instance(self.instance_id)
        self._table = instance.table(self.table_id)
    
    def process(self, batch_elements):
        """Process batch of elements efficiently"""
        if not batch_elements:
            return
        
        # Prepare row keys
        row_keys = []
        key_to_element = {}
        
        for element in batch_elements:
            member_id = element.get('member_id')
            if member_id:
                row_key = f"member#{member_id}"
                row_keys.append(row_key.encode())
                key_to_element[row_key] = element
        
        if not row_keys:
            return
        
        # Batch read from BigTable
        from google.cloud.bigtable.row_set import RowSet
        row_set = RowSet()
        for key in row_keys:
            row_set.add_row_key(key)
        
        # Read rows
        rows = self._table.read_rows(row_set=row_set)
        found_keys = set()
        
        for row in rows:
            row_key = row.row_key.decode('utf-8')
            found_keys.add(row_key)
            member_id = row_key.replace('member#', '')
            
            # Parse row data
            enriched_data = {'member_number': member_id}
            
            for family_id, columns in row.cells.items():
                family_name = family_id
                for column, cells in columns.items():
                    col_name = column.decode('utf-8')
                    value = cells[0].value.decode('utf-8')
                    
                    # Try to parse JSON
                    try:
                        value = json.loads(value)
                    except:
                        pass
                    
                    enriched_data[f"{family_name}:{col_name}"] = value
            
            yield enriched_data
        
        # Handle not found records
        for row_key, element in key_to_element.items():
            if row_key not in found_keys:
                yield {
                    'member_number': element.get('member_id'),
                    'found_in_bigtable': False
                }

# ============================================
# PART 3: Message Processing
# ============================================

class ProcessPubSubMessage(beam.DoFn):
    """Process Pub/Sub message with better error handling"""
    
    def process(self, element):
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
            
            message = json.loads(data) if isinstance(data, str) else data
            
            # Extract member_id with multiple strategies
            member_id = None
            
            # Strategy 1: Direct field
            member_id = message.get('member_id') or message.get('memberId')
            
            # Strategy 2: From profiles
            if not member_id and 'profiles' in message:
                profiles = message['profiles']
                if isinstance(profiles, str):
                    profiles = json.loads(profiles)
                member_id = profiles.get('memberId') or profiles.get('member_id')
            
            # Strategy 3: From nested payload
            if not member_id and 'payload' in message:
                payload = message['payload']
                if isinstance(payload, str):
                    payload = json.loads(payload)
                member_id = payload.get('member_id') or payload.get('memberId')
            
            if not member_id:
                raise ValueError("No member_id found in message")
            
            yield beam.pvalue.TaggedOutput('success', {
                'member_id': member_id,
                'message': message,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Failed to process message: {e}")
            yield beam.pvalue.TaggedOutput('dlq', {
                'data': str(element),
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })

class ApplyMappingOptimized(beam.DoFn):
    """Apply mapping with better null handling"""
    
    def process(self, element, mapping_dict):
        if not mapping_dict:
            logger.warning("Empty mapping dict, returning original")
            yield element
            return
        
        mapped_record = {}
        
        # Apply mapping
        for dest_col, config in mapping_dict.items():
            src_path = config.get('src_path', [])
            
            # Navigate nested path
            value = element
            for key in src_path:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    value = None
                    break
            
            mapped_record[dest_col] = value
        
        # Ensure primary key
        if 'member_number' not in mapped_record:
            mapped_record['member_number'] = (
                element.get('member_number') or 
                element.get('member_id')
            )
        
        # Add timestamp
        mapped_record['_processed_at'] = datetime.now().isoformat()
        
        yield mapped_record

# ============================================
# PART 4: Optimized S3 Writer with Batching
# ============================================

class BatchedS3ParquetWriter(beam.DoFn):
    """Write to S3 with micro-batching for better performance"""
    
    def __init__(self, bucket, prefix, aws_access_key, aws_secret_key, 
                 batch_size=10, batch_timeout=5):
        self.bucket = bucket
        self.prefix = prefix
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self._s3_client = None
        self._buffer = []
        self._buffer_start_time = None
        self._created_partitions = set()
    
    def setup(self):
        self._s3_client = boto3.client(
            's3',
            region_name='ap-southeast-1',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key
        )
        
    def start_bundle(self):
        self._buffer = []
        self._buffer_start_time = time.time()
    
    def process(self, element, window=beam.DoFn.WindowParam):
        # Add to buffer
        self._buffer.append((element, window))
        
        # Check if should flush
        should_flush = (
            len(self._buffer) >= self.batch_size or
            (self._buffer_start_time and 
             time.time() - self._buffer_start_time > self.batch_timeout)
        )
        
        if should_flush:
            yield from self._flush_buffer()
    
    def finish_bundle(self):
        if self._buffer:
            yield from self._flush_buffer()
    
    def _flush_buffer(self):
        if not self._buffer:
            return
        
        # Group by window
        from collections import defaultdict
        window_groups = defaultdict(list)
        
        for element, window_param in self._buffer:
            window_groups[window_param].append(element)
        
        # Write each window's data
        for window_param, elements in window_groups.items():
            # Get partition path
            window_start = window_param.start.to_utc_datetime()
            partition_path = (
                f"{self.prefix}/year={window_start.year:04d}/"
                f"month={window_start.month:02d}/"
                f"day={window_start.day:02d}/"
                f"hour={window_start.hour:02d}"
            )
            
            # Create partition marker
            self._ensure_partition(partition_path)
            
            # Write batch as single parquet file
            try:
                filename = f"batch_{uuid.uuid4().hex}_{int(time.time())}.parquet"
                file_key = f"{partition_path}/{filename}"
                
                # Remove internal fields
                clean_elements = [
                    {k: v for k, v in elem.items() if not k.startswith('_')}
                    for elem in elements
                ]
                
                # Convert to PyArrow table
                table = pa.Table.from_pylist(clean_elements)
                
                # Write to buffer
                import io
                buffer = io.BytesIO()
                pq.write_table(table, buffer, compression='snappy')
                
                # Upload to S3
                self._s3_client.put_object(
                    Bucket=self.bucket,
                    Key=file_key,
                    Body=buffer.getvalue()
                )
                
                logger.info(f"Wrote batch of {len(elements)} records to {file_key}")
                
                yield beam.pvalue.TaggedOutput('success', {
                    'path': file_key,
                    'count': len(elements),
                    'timestamp': datetime.now().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Failed to write batch: {e}")
                yield beam.pvalue.TaggedOutput('failed', {
                    'error': str(e),
                    'count': len(elements)
                })
        
        # Clear buffer
        self._buffer = []
        self._buffer_start_time = time.time()
    
    def _ensure_partition(self, partition_path):
        """Create partition marker if needed"""
        marker_key = f"{partition_path}/_SUCCESS"
        if marker_key not in self._created_partitions:
            try:
                self._s3_client.put_object(
                    Bucket=self.bucket,
                    Key=marker_key,
                    Body=json.dumps({
                        'created_at': datetime.now().isoformat(),
                        'pipeline': 'ms_member_streaming'
                    }).encode('utf-8')
                )
                self._created_partitions.add(marker_key)
                logger.info(f"Created partition: {partition_path}")
            except Exception as e:
                logger.error(f"Failed to create partition: {e}")

def write_sink(pcoll, mode, opts):
    if mode == "native_cdc":
        return pcoll | "BQ_CDC" >> WriteToBigQuery(
            table=opts["bq_table_ms_personas"],
            schema=opts["schema"],
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
            use_cdc_writes=True,
            primary_key=opts["primary_key"],
        )
    elif mode == "biglake_append":
        return pcoll | "BQ_BigLake_Append" >> WriteToBigQuery(
            table=opts["bq_table_ms_personas_ext_apd"],
            schema=opts["schema"],
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
        )
    elif mode == "iceberg_external_append":
        # PSEUDOCODE: Managed I/O ICEBERG write
        rows = pcoll | beam.Map(lambda d: beam.Row(**d))
        return rows | "Iceberg_Append" >> beam.io.managed.WriteToIceberg(opts["iceberg_cfg"])
    # elif mode == "iceberg_external_cdc":
    #     # ทางชัวร์: SCD2 (append) ด้วย Managed I/O
    #     # หรือ True CDC: ให้ Flink pipeline ทำ equality deletes
    #     ...
    else:
        raise ValueError(f"Unknown sink mode: {mode}")

# ============================================
# MAIN PIPELINE
# ============================================

def run_streaming_pipeline():
    """Main pipeline with optimizations"""
    
    # Pipeline options
    options = PipelineOptions([
        f'--project={PROJECT_ID}',
        f'--region={REGION}',
        '--runner=DataflowRunner',
        '--streaming',
        '--enable_streaming_engine',
        '--worker_machine_type=n1-standard-2',
        '--max_num_workers=10',
        '--autoscaling_algorithm=THROUGHPUT_BASED',
        '--use_public_ips=false',  # Use private IPs
        '--enable_hot_key_logging',  # Monitor hot keys
        f'--temp_location=gs://{PROJECT_ID}-dataflow-temp/temp',
        f'--staging_location=gs://{PROJECT_ID}-dataflow-temp/staging',
    ])
    
    with beam.Pipeline(options=options) as p:
        
        # Step 1: Create mapping side input (updates every hour)
        # Using simplified approach with caching DoFn
        mapping_side_input = (
            p
            | 'TriggerMappingUpdate' >> beam.transforms.PeriodicImpulse(
                fire_interval=3600
            )
            | 'QueryMappingWithCache' >> beam.ParDo(QueryMappingWithCache())
            | 'WindowMappingGlobal' >> beam.WindowInto(
                window.GlobalWindows(),
                trigger=trigger.Repeatedly(trigger.AfterProcessingTime(1)),
                accumulation_mode=trigger.AccumulationMode.DISCARDING
            )
        )
        
        # Step 2: Consume from Pub/Sub
        messages = (
            p
            | 'ReadFromPubSub' >> ReadFromPubSub(
                subscription=SUBSCRIPTION,
                with_attributes=True,
                id_label='message_id',
                timestamp_attribute='publish_time'
            )
            | 'ProcessMessages' >> beam.ParDo(ProcessPubSubMessage()).with_outputs(
                'success', 'dlq'
            )
        )
        
        # Handle DLQ
        _ = (
            messages.dlq
            | 'SerializeDLQ' >> beam.Map(json.dumps)
            | 'WriteToDLQ' >> WriteToPubSub(topic=DLQ_TOPIC)
        )
        
        # Step 3: Enrich from BigTable
        if USE_ENRICHMENT_API:
            # Using new Enrichment API
            enriched = (
                messages.success
                | 'EnrichWithAPI' >> Enrichment(create_bigtable_enrichment())
            )
        else:
            # Using optimized batch lookup
            enriched = (
                messages.success
                | 'BatchForBigTable' >> beam.BatchElements(
                    min_batch_size=10,
                    max_batch_size=100,
                    max_latency_secs=1
                )
                | 'LookupBigTable' >> beam.ParDo(
                    OptimizedBigTableLookup(PROJECT_ID, BT_INSTANCE, BT_TABLE)
                )
            )
        
        # Step 4: Apply mapping with side input
        mapped_records = (
            enriched
            | 'ApplyMapping' >> beam.ParDo(
                ApplyMappingOptimized(),
                mapping_dict=beam.pvalue.AsSingleton(mapping_side_input)
            )
        )
        
        # Step 5.1: Write to BigQuery with better error handling
        bq_write = (
            mapped_records
            | 'PrepareForBQ' >> beam.Map(
                lambda x: {k: v for k, v in x.items() if not k.startswith('_')}
            )
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=BQ_OUTPUT_TABLE,
                schema='SCHEMA_AUTODETECT',
                write_disposition='WRITE_APPEND',
                create_disposition='CREATE_IF_NEEDED',
                method=WriteToBigQuery.Method.STREAMING_INSERTS,
                insert_retry_strategy='RETRY_ON_TRANSIENT_ERROR'
            )
        )
        # ----------------------------------------------------------------------------------------------
        # -------------------------------------- BQ WRITE TYPE -----------------------------------------
        # ----------------------------------------------------------------------------------------------
        # -------------------   Write to BigQuery :: CDC NATIVE TABLE  ---------------------------------
        # CREATE TABLE `proj.dataset.ms_personas_ntv_cdc` (
        #   member_id STRING,
        #   ...,
        #   PRIMARY KEY (member_id) NOT ENFORCED
        # );

        result_bq_ntv_cdc = (
            mapped_records  # PCollection[dict] ที่ใส่ _CHANGE_TYPE = 'UPSERT'/'DELETE' แล้ว
            | "Write CDC to BQ" >> WriteToBigQuery(
                table= f"{PROJECT_ID}.{DATASET}.ms_personas_ntv_cdc",
                # schema=bq_schema,  # ระบุ schema ให้ตรง
                schema='SCHEMA_AUTODETECT',
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                method=WriteToBigQuery.Method.STORAGE_WRITE_API,
                use_cdc_writes=True,                 # เปิด CDC mode
                primary_key=["member_number"],           # ดึงจากคอนฟิกของคุณ
                # expansion_service: ใช้ default GCP expansion service ได้
            )
        )
        bq_ntv_cdc_dlq_rows = result_bq_ntv_cdc.failed_rows()                 # เอาไปลง Pub/Sub / GCS DLQ
        bq_ntv_cdc_dlq_rows_with_errors = result_bq_ntv_cdc.failed_rows_with_errors()

        # -------------------   Write to BigQuery :: APPEND EXTERNAL TABLE  ---------------------------------
        # CREATE TABLE `proj.ds.ms_personas_ext_apd` (
        #   member_id STRING, ...
        # )
        # WITH CONNECTION `proj.region.conn_id`
        # OPTIONS (file_format='PARQUET', table_format='ICEBERG', storage_uri='gs://bucket/prefix');

        result_bq_ext_apd = (
            mapped_records
            | "Write Append to BigLake Iceberg" >> WriteToBigQuery(
                table= f"{PROJECT_ID}.{DATASET}.ms_personas_ext_apd",
                schema=bq_schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                method=WriteToBigQuery.Method.STORAGE_WRITE_API
            )
        )
        # -------------------   Write to BigQuery :: CDC EXTERNAL TABLE  ---------------------------------

        # ----------------------------------------------------------------------------------------------
        # Step 5.2: Write to S3 with batching
        s3_write = (
            mapped_records
            | 'WindowForS3' >> beam.WindowInto(
                window.FixedWindows(3600),
                trigger=trigger.AfterWatermark(
                    early_firings=trigger.AfterProcessingTime(60)
                ),
                accumulation_mode=trigger.AccumulationMode.DISCARDING
            )
            | 'WriteToS3Batched' >> beam.ParDo(
                BatchedS3ParquetWriter(
                    S3_BUCKET, S3_PREFIX, 
                    AWS_ACCESS_KEY, AWS_SECRET_KEY,
                    batch_size=100,  # Batch 100 records
                    batch_timeout=10  # Or 10 seconds
                )
            ).with_outputs('success', 'failed')
        )
        
        # Monitor S3 write metrics
        _ = (
            s3_write.success
            | 'CountS3Success' >> beam.Map(
                lambda x: logger.info(f"S3 batch written: {x['count']} records")
            )
        )
        
        _ = (
            s3_write.failed
            | 'LogS3Failures' >> beam.Map(
                lambda x: logger.error(f"S3 write failed: {x['error']}")
            )
        )
    
    logger.info("Optimized pipeline started successfully")

if __name__ == '__main__':
    run_streaming_pipeline()