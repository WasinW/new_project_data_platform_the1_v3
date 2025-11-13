#!/usr/bin/env python
"""MS Member realtime pipeline with cache (Case 3: Tech provided incomplete data).

This pipeline caches origin data from BigQuery and merges with new streaming data
when Tech hasn't provided all columns. It handles:
1. Loading mapping configuration (old vs new columns)
2. Caching origin data from ms_member table (25M records)
3. Processing streaming messages from PubSub
4. Merging new data with cached origin data
5. Writing to S3 and BigQuery with original schema
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, ReadFromBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows, GlobalWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, Repeatedly
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.pvalue import AsDict

LOGGER = logging.getLogger(__name__)


class LoadMappingDoFn(beam.DoFn):
    """Load mapping configuration from BigQuery."""
    
    def __init__(self, project_id: str, dataset: str, table: str):
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.mapping_sts_cache = {}  # {'org_col1':'old','org_col2':'new',...}
        self.mapping_col_cache = {}  # {'org_col1':'new_col1','org_col2':'new_col2',...}
        self.last_refresh = None
        
    def process(self, element):
        from google.cloud import bigquery
        
        # Refresh every hour
        now = datetime.utcnow()
        if self.last_refresh is None or (now - self.last_refresh) > timedelta(hours=1):
            client = bigquery.Client(project=self.project_id)
            query = f"""
            SELECT 
                RECONCILE_COLUMN_NAME,
                PERSONAS_MAPPING_COLUMN_NAME,
                RECONCILE_RETRIEVED
            FROM `{self.project_id}.{self.dataset}.{self.table}`
            WHERE RECONCILE_CONFIRMED = TRUE
            """
            
            self.mapping_sts_cache.clear()
            self.mapping_col_cache.clear()
            
            for row in client.query(query):
                new_col = row['PERSONAS_MAPPING_COLUMN_NAME']
                origin_col = row['RECONCILE_COLUMN_NAME']
                is_new = row['RECONCILE_RETRIEVED']
                
                # Build status mapping (old/new)
                self.mapping_sts_cache[origin_col] = 'new' if is_new else 'old'
                
                # Build column name mapping
                if new_col:  # Only if there's a mapping
                    self.mapping_col_cache[new_col] = new_col
                    
            self.last_refresh = now
            LOGGER.info(f"Loaded {len(self.mapping_sts_cache)} mappings")
            
        yield {
            'mapping_sts': dict(self.mapping_sts_cache),
            'mapping_col': dict(self.mapping_col_cache)
        }


class LoadOriginCacheDoFn(beam.DoFn):
    """Load origin data from BigQuery ms_member table."""
    
    def __init__(self, project_id: str, dataset: str, table: str = 'ms_member'):
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.origin_cache = {}
        self.last_refresh = None
        
    def process(self, element, mapping_sts):
        from google.cloud import bigquery
        
        # Refresh cache every hour
        now = datetime.utcnow()
        if self.last_refresh is None or (now - self.last_refresh) > timedelta(hours=1):
            client = bigquery.Client(project=self.project_id)
            
            # Get only 'old' columns from mapping
            old_columns = [col for col, status in mapping_sts.items() if status == 'old']
            
            if old_columns:
                columns_str = ', '.join(old_columns)
                query = f"""
                SELECT personasId, {columns_str}
                FROM `{self.project_id}.{self.dataset}.{self.table}`
                WHERE personasId IS NOT NULL
                """
                
                LOGGER.info(f"Loading origin cache with columns: {old_columns}")
                self.origin_cache.clear()
                
                count = 0
                for row in client.query(query):
                    personas_id = str(row['personasId'])
                    row_dict = {col: row[col] for col in old_columns if col in row}
                    self.origin_cache[personas_id] = row_dict
                    count += 1
                    
                    if count % 1000000 == 0:
                        LOGGER.info(f"Loaded {count} records into cache")
                
                self.last_refresh = now
                LOGGER.info(f"Origin cache loaded with {count} records")
                
        yield dict(self.origin_cache)


class ParsePubSubMessageDoFn(beam.DoFn):
    """Parse PubSub message and extract ID."""
    
    def process(self, element):
        try:
            message_data = json.loads(element.data.decode('utf-8'))
            
            # Extract message ID (could be member_id, memberId, or personasId)
            message_id = (
                message_data.get('personasId') or
                message_data.get('member_id') or 
                message_data.get('memberId')
            )
            
            if not message_id:
                LOGGER.error(f"No ID found in message: {message_data}")
                return
                
            yield {
                'message_id': str(message_id),
                'message_data': message_data,
                'attributes': element.attributes if hasattr(element, 'attributes') else {}
            }
            
        except Exception as e:
            LOGGER.error(f"Error parsing message: {e}")
            yield beam.pvalue.TaggedOutput('dlq', {
                'error': str(e),
                'message': element.data.decode('utf-8') if element.data else None
            })


class EnrichFromBigtableDoFn(beam.DoFn):
    """Enrich message with data from Bigtable."""
    
    def __init__(self, project_id: str, instance_id: str, table_id: str):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.client = None
        self.table = None
        
    def setup(self):
        from google.cloud import bigtable
        
        self.client = bigtable.Client(project=self.project_id)
        instance = self.client.instance(self.instance_id)
        self.table = instance.table(self.table_id)
        
    def process(self, element):
        try:
            message_id = element['message_id']
            row_key = f"personas#{message_id}".encode('utf-8')
            
            row = self.table.read_row(row_key)
            if row:
                # Extract all column families and cells
                enriched_data = {}
                for cf_name, cf_data in row.cells.items():
                    for col_name, cells in cf_data.items():
                        if cells:
                            # Get latest cell value
                            enriched_data[col_name.decode('utf-8')] = cells[0].value.decode('utf-8')
                
                element['message_details'] = enriched_data
                LOGGER.debug(f"Enriched message {message_id} with {len(enriched_data)} fields")
            else:
                LOGGER.warning(f"No data found in Bigtable for {message_id}")
                element['message_details'] = element.get('message_data', {})
                
            yield element
            
        except Exception as e:
            LOGGER.error(f"Error enriching from Bigtable: {e}")
            element['message_details'] = element.get('message_data', {})
            yield element


class MergeWithCacheDoFn(beam.DoFn):
    """Merge streaming data with cached origin data."""
    
    def process(self, element, mapping_sts, mapping_col, origin_cache):
        try:
            message_id = element['message_id']
            message_details = element.get('message_details', {})
            
            # Step 1: Extract message rows
            message_rows = {k: v for k, v in message_details.items() if v is not None}
            
            # Step 2: Remap column names from tech schema to personas schema
            message_rows_reschemas = {}
            reverse_mapping = {v: k for k, v in mapping_col.items() if v}  # Reverse mapping
            
            for tech_col, value in message_rows.items():
                if tech_col in reverse_mapping:
                    new_col = reverse_mapping[tech_col]
                    message_rows_reschemas[new_col] = value
                else:
                    # Keep as is if no mapping
                    message_rows_reschemas[tech_col] = value
            
            # Step 3: Create new data dict (only new columns)
            ms_personas_new_dict = {}
            for col, value in message_rows_reschemas.items():
                if mapping_sts.get(col) == 'new':
                    ms_personas_new_dict[col] = value
            
            # Step 4: Merge with origin cache
            ms_personas_rows = {}
            
            # Start with origin data if exists
            if message_id in origin_cache:
                ms_personas_rows = dict(origin_cache[message_id])
                LOGGER.debug(f"Found origin data for {message_id}")
            
            # Override with new data
            ms_personas_rows.update(ms_personas_new_dict)
            
            # Always include the ID
            ms_personas_rows['personasId'] = message_id
            
            yield {
                'personasId': message_id,
                'data': ms_personas_rows,
                'timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            LOGGER.error(f"Error merging data: {e}")
            yield beam.pvalue.TaggedOutput('dlq', {
                'error': str(e),
                'element': element
            })


class WriteToS3DoFn(beam.DoFn):
    """Write to S3 in batches."""
    
    def __init__(self, bucket: str, prefix: str, aws_access_key: str, aws_secret_key: str,
                 batch_size: int = 100):
        self.bucket = bucket
        self.prefix = prefix
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.batch_size = batch_size
        self.batch = []
        
    def setup(self):
        import boto3
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key
        )
        
    def process(self, element):
        self.batch.append(element)
        
        if len(self.batch) >= self.batch_size:
            self._write_batch()
            
        yield element  # Pass through
        
    def finish_bundle(self):
        if self.batch:
            self._write_batch()
            
    def _write_batch(self):
        if not self.batch:
            return
            
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
        key = f"{self.prefix}/batch_{timestamp}.json"
        
        # Convert to JSONL format
        jsonl_data = '\n'.join([json.dumps(record['data']) for record in self.batch])
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=jsonl_data.encode('utf-8'),
                ContentType='application/x-ndjson'
            )
            LOGGER.info(f"Wrote {len(self.batch)} records to s3://{self.bucket}/{key}")
        except Exception as e:
            LOGGER.error(f"Failed to write to S3: {e}")
        finally:
            self.batch.clear()


def run(argv: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(description="MS Member realtime pipeline with cache")
    
    # Core arguments
    parser.add_argument("--project_id", default="the1-insight-dev")
    parser.add_argument("--dataset", default="insight_dev")
    parser.add_argument("--region", default="asia-southeast1")
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--dlq_topic", required=True)
    
    # BigQuery arguments
    parser.add_argument("--bq_table", required=True, help="Target BigQuery table")
    parser.add_argument("--origin_table", default="ms_member", help="Origin data table")
    parser.add_argument("--mapping_table", default="stg_mapping_reconcile")
    
    # Bigtable arguments
    parser.add_argument("--bigtable_instance", required=True)
    parser.add_argument("--bigtable_table", required=True)
    
    # S3 arguments
    parser.add_argument("--s3_bucket", required=True)
    parser.add_argument("--s3_prefix", default="refined/ms_member_streaming_cache")
    parser.add_argument("--aws_access_key", default=os.environ.get("AWS_ACCESS_KEY_ID"))
    parser.add_argument("--aws_secret_key", default=os.environ.get("AWS_SECRET_ACCESS_KEY"))
    parser.add_argument("--s3_batch_size", type=int, default=100)
    
    # Cache and window settings
    parser.add_argument("--window_duration", type=int, default=3600, help="Window duration in seconds")
    parser.add_argument("--cache_refresh_interval", type=int, default=3600, help="Cache refresh interval in seconds")
    
    parser.add_argument("--log_level", default="INFO")
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, known_args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    
    LOGGER.info("Starting cache pipeline with args: %s", known_args)
    
    # Pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    std_opts = pipeline_options.view_as(StandardOptions)
    std_opts.streaming = True
    setup_opts = pipeline_options.view_as(SetupOptions)
    setup_opts.save_main_session = True
    
    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Load mapping configuration (refreshed periodically)
        mapping_impulse = (
            p 
            | "MappingImpulse" >> PeriodicImpulse(
                start_timestamp=0,
                stop_timestamp=float('inf'),
                fire_interval=known_args.cache_refresh_interval
            )
        )
        
        mapping = (
            mapping_impulse
            | "LoadMapping" >> beam.ParDo(
                LoadMappingDoFn(
                    project_id=known_args.project_id,
                    dataset=known_args.dataset,
                    table=known_args.mapping_table
                )
            )
        )
        
        mapping_sts_side = (
            mapping 
            | "ExtractMappingSts" >> beam.Map(lambda x: (None, x['mapping_sts']))
            | "LatestMappingSts" >> beam.CombinePerKey(lambda values: list(values)[-1])
            | "MappingStsAsDict" >> beam.Map(lambda x: x[1])
        )
        
        mapping_col_side = (
            mapping 
            | "ExtractMappingCol" >> beam.Map(lambda x: (None, x['mapping_col']))
            | "LatestMappingCol" >> beam.CombinePerKey(lambda values: list(values)[-1])
            | "MappingColAsDict" >> beam.Map(lambda x: x[1])
        )
        
        # Step 2: Load origin cache (refreshed periodically)
        origin_impulse = (
            p 
            | "OriginImpulse" >> PeriodicImpulse(
                start_timestamp=0,
                stop_timestamp=float('inf'),
                fire_interval=known_args.cache_refresh_interval
            )
        )
        
        origin_cache_side = (
            origin_impulse
            | "LoadOriginCache" >> beam.ParDo(
                LoadOriginCacheDoFn(
                    project_id=known_args.project_id,
                    dataset=known_args.dataset,
                    table=known_args.origin_table
                ),
                mapping_sts=beam.pvalue.AsSingleton(mapping_sts_side)
            )
            | "LatestOriginCache" >> beam.CombineGlobally(lambda values: list(values)[-1])
        )
        
        # Step 3: Read from PubSub
        messages = (
            p 
            | "ReadPubSub" >> ReadFromPubSub(
                subscription=known_args.subscription,
                with_attributes=True
            )
            | "ParseMessages" >> beam.ParDo(ParsePubSubMessageDoFn()).with_outputs('dlq', main='success')
        )
        
        # Handle DLQ
        _ = (
            messages.dlq
            | "DLQToJSON" >> beam.Map(json.dumps)
            | "WriteDLQ" >> WriteToPubSub(topic=known_args.dlq_topic)
        )
        
        # Step 4: Enrich from Bigtable
        enriched = (
            messages.success
            | "EnrichFromBigtable" >> beam.ParDo(
                EnrichFromBigtableDoFn(
                    project_id=known_args.project_id,
                    instance_id=known_args.bigtable_instance,
                    table_id=known_args.bigtable_table
                )
            )
        )
        
        # Step 5: Apply windowing
        windowed = (
            enriched
            | "ApplyWindow" >> beam.WindowInto(
                FixedWindows(known_args.window_duration),
                trigger=Repeatedly(
                    AfterWatermark(
                        early=AfterProcessingTime(delay=30),
                        late=AfterProcessingTime(delay=60)
                    )
                ),
                accumulation_mode=beam.AccumulationMode.DISCARDING
            )
        )
        
        # Step 6: Merge with cache
        merged = (
            windowed
            | "MergeWithCache" >> beam.ParDo(
                MergeWithCacheDoFn(),
                mapping_sts=beam.pvalue.AsSingleton(mapping_sts_side),
                mapping_col=beam.pvalue.AsSingleton(mapping_col_side),
                origin_cache=beam.pvalue.AsSingleton(origin_cache_side)
            ).with_outputs('dlq', main='success')
        )
        
        # Handle merge errors
        _ = (
            merged.dlq
            | "MergeDLQToJSON" >> beam.Map(json.dumps)
            | "WriteMergeDLQ" >> WriteToPubSub(topic=known_args.dlq_topic)
        )
        
        # Step 7: Write to S3 (original schema)
        if known_args.s3_bucket and known_args.aws_access_key and known_args.aws_secret_key:
            _ = (
                merged.success
                | "WriteToS3" >> beam.ParDo(
                    WriteToS3DoFn(
                        bucket=known_args.s3_bucket,
                        prefix=known_args.s3_prefix,
                        aws_access_key=known_args.aws_access_key,
                        aws_secret_key=known_args.aws_secret_key,
                        batch_size=known_args.s3_batch_size
                    )
                )
            )
        
        # Step 8: Write to BigQuery (original schema)
        _ = (
            merged.success
            | "ExtractData" >> beam.Map(lambda x: x['data'])
            | "WriteToBigQuery" >> WriteToBigQuery(
                table=known_args.bq_table,
                schema='SCHEMA_AUTODETECT',
                write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND,
                create_disposition=WriteToBigQuery.CreateDisposition.CREATE_IF_NEEDED
            )
        )
    
    LOGGER.info("Pipeline initialization complete")


if __name__ == "__main__":
    run()
