#!/usr/bin/env python
"""MS Member realtime pipeline without cache (Case 2: Tech provided complete data).

This pipeline processes streaming data directly without caching when Tech has 
provided all columns. It handles:
1. Consuming messages from PubSub
2. Enriching with Bigtable data
3. Applying schema mapping/transformation
4. Writing to S3 (original schema) and BigQuery (new schema)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions

LOGGER = logging.getLogger(__name__)


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


class TransformToPersonasSchemaDoFn(beam.DoFn):
    """Transform message to MS Personas schema format."""
    
    def __init__(self, schema_mapping: Optional[Dict[str, str]] = None):
        """
        Args:
            schema_mapping: Optional manual mapping dict for fixing schemas
                           e.g., {'member_number': 'member_id', 'is_email': 'has_email'}
        """
        self.schema_mapping = schema_mapping or {}
        
    def process(self, element):
        try:
            message_id = element['message_id']
            message_details = element.get('message_details', {})
            
            # Extract message rows
            message_rows = {k: v for k, v in message_details.items() if v is not None}
            
            # Apply schema mapping if provided
            ms_personas = {}
            for key, value in message_rows.items():
                # Use mapping if exists, otherwise keep original
                mapped_key = self.schema_mapping.get(key, key)
                ms_personas[mapped_key] = value
            
            # Ensure ID is always present
            ms_personas['personasId'] = message_id
            
            yield {
                'personasId': message_id,
                'original_data': ms_personas,  # For S3 (original schema)
                'tech_data': message_details,   # For BQ (new/tech schema)
                'timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            LOGGER.error(f"Error transforming data: {e}")
            yield beam.pvalue.TaggedOutput('dlq', {
                'error': str(e),
                'element': element
            })


class WriteToS3DoFn(beam.DoFn):
    """Write to S3 in batches with original schema."""
    
    def __init__(self, bucket: str, prefix: str, aws_access_key: str, aws_secret_key: str,
                 batch_size: int = 100, batch_timeout: int = 10):
        self.bucket = bucket
        self.prefix = prefix
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.batch = []
        self.last_write = datetime.utcnow()
        
    def setup(self):
        import boto3
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key
        )
        
    def process(self, element):
        self.batch.append(element)
        
        # Write batch if size reached or timeout
        now = datetime.utcnow()
        time_since_last_write = (now - self.last_write).total_seconds()
        
        if len(self.batch) >= self.batch_size or time_since_last_write >= self.batch_timeout:
            self._write_batch()
            self.last_write = now
            
        yield element  # Pass through
        
    def finish_bundle(self):
        if self.batch:
            self._write_batch()
            
    def _write_batch(self):
        if not self.batch:
            return
            
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
        key = f"{self.prefix}/batch_{timestamp}.json"
        
        # Convert to JSONL format - use original_data for S3
        jsonl_data = '\n'.join([json.dumps(record['original_data']) for record in self.batch])
        
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


def load_schema_mapping(mapping_file: Optional[str] = None) -> Dict[str, str]:
    """Load schema mapping from file or environment.
    
    This is for manual fixing of schema differences between Tech and Personas.
    The mapping should be provided by DAGs or configuration.
    
    Format: {'tech_column': 'personas_column', ...}
    """
    if mapping_file and os.path.exists(mapping_file):
        with open(mapping_file, 'r') as f:
            return json.load(f)
    
    # Try environment variable
    mapping_json = os.environ.get('SCHEMA_MAPPING')
    if mapping_json:
        return json.loads(mapping_json)
    
    # Default mapping (example - should be replaced with actual mapping)
    return {
        # Example mappings - replace with actual from team discussion
        # 'is_email': 'has_email',
        # 'is_mobile': 'has_mobile',
        # Add more mappings as needed
    }


def run(argv: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(description="MS Member realtime pipeline without cache")
    
    # Core arguments
    parser.add_argument("--project_id", default="the1-insight-dev")
    parser.add_argument("--dataset", default="insight_dev")
    parser.add_argument("--region", default="asia-southeast1")
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--dlq_topic", required=True)
    
    # BigQuery arguments
    parser.add_argument("--bq_table", required=True, help="Target BigQuery table (tech schema)")
    parser.add_argument("--bq_write_method", default="STREAMING_INSERTS",
                       choices=["STREAMING_INSERTS", "STORAGE_WRITE_API"])
    
    # Bigtable arguments
    parser.add_argument("--bigtable_instance", required=True)
    parser.add_argument("--bigtable_table", required=True)
    
    # S3 arguments
    parser.add_argument("--s3_bucket", required=True)
    parser.add_argument("--s3_prefix", default="refined/ms_member_streaming_no_cache")
    parser.add_argument("--aws_access_key", default=os.environ.get("AWS_ACCESS_KEY_ID"))
    parser.add_argument("--aws_secret_key", default=os.environ.get("AWS_SECRET_ACCESS_KEY"))
    parser.add_argument("--s3_batch_size", type=int, default=100)
    parser.add_argument("--s3_batch_timeout", type=int, default=10)
    
    # Schema mapping
    parser.add_argument("--schema_mapping_file", help="JSON file with schema mapping")
    parser.add_argument("--schema_mapping_json", help="JSON string with schema mapping")
    
    parser.add_argument("--log_level", default="INFO")
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, known_args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    
    LOGGER.info("Starting no-cache pipeline with args: %s", known_args)
    
    # Load schema mapping
    schema_mapping = {}
    if known_args.schema_mapping_json:
        schema_mapping = json.loads(known_args.schema_mapping_json)
    elif known_args.schema_mapping_file:
        schema_mapping = load_schema_mapping(known_args.schema_mapping_file)
    else:
        schema_mapping = load_schema_mapping()
    
    LOGGER.info(f"Using schema mapping: {schema_mapping}")
    
    # Pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    std_opts = pipeline_options.view_as(StandardOptions)
    std_opts.streaming = True
    setup_opts = pipeline_options.view_as(SetupOptions)
    setup_opts.save_main_session = True
    
    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Read from PubSub
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
        
        # Step 2: Enrich from Bigtable
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
        
        # Step 3: Transform to Personas schema
        transformed = (
            enriched
            | "TransformToPersonasSchema" >> beam.ParDo(
                TransformToPersonasSchemaDoFn(schema_mapping=schema_mapping)
            ).with_outputs('dlq', main='success')
        )
        
        # Handle transformation errors
        _ = (
            transformed.dlq
            | "TransformDLQToJSON" >> beam.Map(json.dumps)
            | "WriteTransformDLQ" >> WriteToPubSub(topic=known_args.dlq_topic)
        )
        
        # Step 4: Write to S3 (original/personas schema)
        if known_args.s3_bucket and known_args.aws_access_key and known_args.aws_secret_key:
            _ = (
                transformed.success
                | "WriteToS3" >> beam.ParDo(
                    WriteToS3DoFn(
                        bucket=known_args.s3_bucket,
                        prefix=known_args.s3_prefix,
                        aws_access_key=known_args.aws_access_key,
                        aws_secret_key=known_args.aws_secret_key,
                        batch_size=known_args.s3_batch_size,
                        batch_timeout=known_args.s3_batch_timeout
                    )
                )
            )
        
        # Step 5: Write to BigQuery (new/tech schema)
        write_method = (
            WriteToBigQuery.Method.STREAMING_INSERTS 
            if known_args.bq_write_method == "STREAMING_INSERTS" 
            else WriteToBigQuery.Method.STORAGE_WRITE_API
        )
        
        _ = (
            transformed.success
            | "ExtractTechData" >> beam.Map(lambda x: x['tech_data'])
            | "WriteToBigQuery" >> WriteToBigQuery(
                table=known_args.bq_table,
                schema='SCHEMA_AUTODETECT',
                write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND,
                create_disposition=WriteToBigQuery.CreateDisposition.CREATE_IF_NEEDED,
                method=write_method
            )
        )
    
    LOGGER.info("Pipeline initialization complete")


if __name__ == "__main__":
    run()
