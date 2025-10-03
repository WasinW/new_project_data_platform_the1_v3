#!/usr/bin/env python
"""
Unified Dataflow Pipeline for THE1 Member Data (Airflow Version)
Supports: Short Term (Batch), Mid Term (Streaming), Long Term (Streaming)
Designed for Airflow BeamRunPythonPipelineOperator execution
"""

import logging
import json
import sys
import os
from datetime import datetime
import apache_beam as beam
from apache_beam import combiners
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from typing import Optional, Dict, Any, List
from apache_beam.io import filesystems
import tempfile

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
# Debug imports

# Continue with normal imports
# from dataflow_common import (
#     CommonPipelineConfig,
#     DataflowJobConfig,
#     ReadFromBigQueryStep,
#     ReadFromPubSubStep,
#     BigtableEnrichmentStep,
#     ColumnMappingStep,
#     DataQualityStep,
#     WriteToBigQueryStep,
#     AuditLoggingStep,
#     CreateMappingSideInput,
#     WindowedAuditLogger,
#     CDCUpsertFormatter,
#     MergeQueryGenerator, 
#     MergeQueryExecutor
# )
# logger.info("Successfully imported dataflow_common package")
try:
    from dataflow_common import (
        CommonPipelineConfig,
        DataflowJobConfig,
        ReadFromBigQueryStep,
        ReadFromPubSubStep,
        BigtableEnrichmentStep,
        ColumnMappingStep,
        DataQualityStep,
        WriteToBigQueryStep,
        AuditLoggingStep,
        CreateMappingSideInput,
        WindowedAuditLogger,
        CDCUpsertFormatter,
        MergeQueryGenerator, 
        MergeQueryExecutor
    )
except ImportError:
    # fallback: parse --extra_packages to find dataflow_common wheel
    for arg in sys.argv:
        if arg.startswith('--extra_packages='):
            pkgs = arg.split('=')[1].split(',')
            for pkg in pkgs:
                if 'dataflow_common' in pkg:
                    # download wheel from GCS to a temp file
                    local_path = tempfile.mktemp(prefix='dataflow_common_', suffix='.whl')
                    with filesystems.FileSystems.open(pkg) as src, open(local_path, 'wb') as dst:
                        dst.write(src.read())
                    sys.path.insert(0, local_path)
                    break
    # try importing again
    from dataflow_common import (
        CommonPipelineConfig,
        DataflowJobConfig,
        ReadFromBigQueryStep,
        ReadFromPubSubStep,
        BigtableEnrichmentStep,
        ColumnMappingStep,
        DataQualityStep,
        WriteToBigQueryStep,
        AuditLoggingStep,
        CreateMappingSideInput,
        WindowedAuditLogger,
        CDCUpsertFormatter,
        MergeQueryGenerator, 
        MergeQueryExecutor
    )
    logging.info("Loaded dataflow_common from downloaded wheel")

def log_count(count):
    """Helper function to log count results"""
    logging.info(f"✅ SUCCESS: Got {count} records from BigQuery")
    return count


def validate_required_params(config: CommonPipelineConfig, mode: str) -> List[str]:
    """Validate that all required parameters are present based on mode"""
    issues = []
    
    # Common required fields
    required_common = [
        'project_id', 'env', 'term_type', 'mode',
        'source_dataset', 'staging_dataset', 'refined_dataset',
        'source_table', 'stg_ongoing_source_table', 'stg_source_table', 'stg_origin_table',
        'refined_ongoing_table', 'audit_table', 'mapping_table'
    ]
    
    for field in required_common:
        if not config.get(field):
            issues.append(f"Missing required field: {field}")
    
    # Mode-specific validation
    if mode == 'batch':
        batch_fields = [
            'min_batch_size', 'max_batch_size', 'enrichment_batch_size',
            'read_method', 'write_method'
        ]
        for field in batch_fields:
            if config.get(field) is None:
                issues.append(f"Missing batch field: {field}")
    
    elif mode == 'streaming':
        streaming_fields = [
            'pubsub_topic', 'window_duration_seconds',
            'early_trigger_seconds', 'late_trigger_seconds',
            'allowed_lateness_seconds', 'accumulation_mode'
        ]
        for field in streaming_fields:
            if config.get(field) is None:
                issues.append(f"Missing streaming field: {field}")
        
        # Mid-term specific
        if config.get('term_type') == 'mid':
            if not config.get('mapping_refresh_interval_seconds'):
                issues.append("Missing mapping_refresh_interval_seconds for mid-term")
    
    return issues


def build_batch_pipeline(pipeline: beam.Pipeline, config: CommonPipelineConfig):
    """Build batch pipeline for short term"""
    logger.info(f"Building batch pipeline for term: {config.get('term_type')}")
    
    try:
        # Log configuration
        logger.info("=== Pipeline Configuration ===")
        logger.info(f"source_project: {config.get('source_project')}")
        logger.info(f"source_dataset: {config.get('source_dataset')}")
        logger.info(f"source_table: {config.get('source_table')}")
        logger.info(f"project_id: {config.get('project_id')}")
        logger.info(f"staging_dataset: {config.get('staging_dataset')}")
        logger.info(f"stg_source_table: {config.get('stg_source_table')}")
        logger.info(f"stg_ongoing_source_table: {config.get('stg_ongoing_source_table')}")
        logger.info(f"read_method: {config.get('read_method')}")
        logger.info(f"write_method: {config.get('write_method')}")
        
        # Build table references
        source_project = config.get('source_project')
        source_dataset = config.get('source_dataset')
        source_table = config.get('source_table')
        project_id = config.get('project_id')
        staging_dataset = config.get('staging_dataset')
        stg_source_table = config.get('stg_source_table')
        stg_ongoing_source_table = config.get('stg_ongoing_source_table')
        mapping_table = config.get('mapping_table')
        read_method = config.get('read_method', 'EXPORT')
        
        # Full table names
        source_table_full = f"{source_project}.{source_dataset}.{source_table}"
        target_table_full = f"{project_id}.{staging_dataset}.{stg_source_table}"
        
        # Get batch limit from config or use default
        batch_limit = config.get('batch_limit') or 1000
        
        # Step 1.1: Read source data from BigQuery
        logger.info(f"Reading source data from: {source_table_full}")
        
        source_query = f"""
            SELECT * EXCEPT(RN_PK)
            FROM (
                SELECT *
                , ROW_NUMBER() OVER(PARTITION BY JSON_VALUE(profiles, '$.memberId') ORDER BY TIMESTAMP DESC) RN_PK
                FROM `{project}.insight_dev.personas_test`
            ) AS LAST_UPD
            WHERE RN_PK = 1 
        """
        read_step = ReadFromBigQueryStep({
            'enabled': True,
            'step_name': 'ReadSource',
            'project': project_id,
            'src_project': source_project,
            'dataset': source_dataset,
            'src_table': source_table_full,
            'tgt_table': target_table_full,
            'query': source_query,
            'method': read_method,
            'gcs_location': config.get('gcs_location') or config.get('temp_location') or 'gs://t1-insight-audit-bucket/audit_log/dataflow/temp'
        })
        
        source_data = read_step.execute(pipeline)
        
        
        # source_data = (
        #     pipeline
        #     | 'ReadFromBigQuery_SRC_DATA' >> ReadFromBigQuery(
        #         query=source_query,
        #         use_standard_sql=True,
        #         project=project_id,
        #         gcs_location='gs://t1-insight-audit-bucket/audit_log/dataflow/temp'
        #         # gcs_location=config.get('gcs_location') or config.get('temp_location')
        #     )
        #     | 'Count_SRC_DATA' >> beam.combiners.Count.Globally()
        #     | 'LogResults_SRC_DATA' >> beam.Map(log_count)  # Use function instead of lambda
        # )


        # Count source records
        # _ = (source_data
        #     | 'Count_Source' >> combiners.Count.Globally()
        #     | 'Log_Source_Count' >> beam.Map(lambda c: logger.info(f"[METRIC] source_count={c}")))

        # Step 1.2: Read mapping table
        logger.info(f"Reading mapping from: {project_id}.{staging_dataset}.{mapping_table}")
        
        mapping_query = f"""
            SELECT *
            FROM `{project_id}.{staging_dataset}.{mapping_table}`
            WHERE COALESCE(UPDATED_DATE, '1999-12-31') = (
                SELECT COALESCE(MAX(UPDATED_DATE), '1999-12-31')
                FROM `{project_id}.{staging_dataset}.{mapping_table}`
            )
        """
        
        read_mapping_step = ReadFromBigQueryStep({
            'enabled': True,
            'step_name': 'ReadMapping',
            'project': project_id,
            'src_project': project_id,
            'dataset': staging_dataset,
            'query': mapping_query,
            'method': read_method,
            'gcs_location': config.get('gcs_location') or config.get('temp_location')
        })
        
        mapping_data = read_mapping_step.execute(pipeline)
        
        # mapping_data = (
        #     pipeline
        #     | 'ReadFromBigQuery_MAPPING' >> ReadFromBigQuery(
        #         query=mapping_query,
        #         use_standard_sql=True,
        #         project=project_id,
        #         # gcs_location=config.get('gcs_location') or config.get('temp_location')
        #         gcs_location='gs://t1-insight-audit-bucket/audit_log/dataflow/temp'
        #     )
        #     | 'Count_MAPPING' >> beam.combiners.Count.Globally()
        #     | 'LogResults_MAPPING' >> beam.Map(log_count)  # Use function instead of lambda

        # )

        # Count mapping records
        # _ = (mapping_data
        #     | 'Count_Mapping' >> combiners.Count.Globally()
        #     | 'Log_Mapping_Count' >> beam.Map(lambda c: logger.info(f"[METRIC] mapping_count={c}")))
        
        mapping_with_default = (
            mapping_data 
            | 'EnsureNotEmpty' >> beam.FlatMap(
                lambda x: [x] if x else [{}]  # Default empty dict
            )
        )
        # Convert mapping to side input
        # mapping_list = beam.pvalue.AsList(mapping_data)
        mapping_list = beam.pvalue.AsList(mapping_with_default)

        # Step 2: Data Quality Validation
        logger.info("Applying data quality validation")
        
        validation_rules = config.get('validation_rules', [])
        if isinstance(validation_rules, str):
            try:
                validation_rules = json.loads(validation_rules)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse validation_rules: {e}")
                validation_rules = []
        
        dq_step = DataQualityStep({
            'enabled': True,
            'step_name': 'DataQuality',
            'rules': validation_rules,
            'split_output': config.get('split_output', False),
            'max_errors_percent': config.get('max_errors_percent', 0.1),
            'error_table': config.get('error_table'),
            'write_errors': config.get('write_errors', True),
            'project': config.get('project_id'),
            'dataset': config.get('staging_dataset')
        })
        
        validated_data = dq_step.execute(pipeline, source_data)
        
        # Count validated records
        # _ = (validated_data
        #     | 'Count_Validated' >> combiners.Count.Globally()
        #     | 'Log_Validated_Count' >> beam.Map(lambda c: logger.info(f"[METRIC] validated_count={c}")))

        # Step 3: Apply column mapping
        logger.info("Applying column mapping transformation")
        
        mapping_step = ColumnMappingStep({
            'enabled': True,
            'step_name': 'MappingStep',
            'mode': 'batch',
            'target_table': config.get('stg_origin_table'),
            'mapping_side_input': mapping_list,
            'min_batch_size': config.get('min_batch_size'),
            'max_batch_size': config.get('max_batch_size'),
            'enrichment_batch_size': config.get('enrichment_batch_size')
        })
        
        # mapped_data = mapping_step.execute(pipeline, validated_data)
        mapped_data = mapping_step.execute(pipeline, source_data)
        
        
        # Count mapped records
        _ = (mapped_data
            | 'Count_Mapped' >> combiners.Count.Globally()
            | 'Log_Mapped_Count' >> beam.Map(lambda c: logger.info(f"[METRIC] mapped_count={c}")))

        # Step 4: Write to staging table (stg_personas)
        logger.info(f"Writing to staging table: {stg_ongoing_source_table}")
        
        write_staging_step = WriteToBigQueryStep({
            'enabled': True,
            'step_name': 'WriteStaging',
            'project': config.get('project_id'),
            'dataset': config.get('staging_dataset'),
            'table': stg_ongoing_source_table,
            'method': config.get('write_method', 'FILE_LOADS'),
            'mode': 'WRITE_TRUNCATE',
            'create_disposition': config.get('bq_create_disposition', 'CREATE_IF_NEEDED'),
            'priority': config.get('bq_priority', 'INTERACTIVE'),
            'remove_metadata': True,
            'temp_location': config.get('temp_location')
        })
        
        write_staging_step.execute(pipeline, mapped_data)

        # Step 5: Generate and execute MERGE queries
        logger.info("Generating and executing MERGE queries")
        
        merge_queries = (
            pipeline
            | 'CreateTrigger' >> beam.Create([1])
            | 'GenerateMergeQueries' >> beam.ParDo(
                MergeQueryGenerator(),
                mapping_list,
                config.to_dict()
            )
        )
        
        merge_results = (
            merge_queries
            | 'ExecuteMergeQueries' >> beam.ParDo(
                MergeQueryExecutor(
                    project_id=config.get('project_id'),
                    dataset=config.get('staging_dataset')
                )
            )
        )
        
        # Log merge results
        _ = (
            merge_results
            | 'LogMergeResults' >> beam.Map(
                lambda x: logger.info(f"Merge execution results: {x}")
            )
        )
        
        # Step 6: Audit Logging (optional)
        if config.get('audit_enabled', True):
            logger.info("Writing audit logs")
            
            audit_step = AuditLoggingStep({
                'enabled': True,
                'step_name': 'AuditLog',
                'aggregate_windows': False,
                'pipeline_name': 'ms_member_unified',
                'mode': 'batch',
                'project': config.get('project_id'),
                'dataset': config.get('staging_dataset'),
                'audit_table': config.get('audit_table'),
                'metrics_enabled': config.get('metrics_enabled', True),
                'error_tracking_enabled': config.get('error_tracking_enabled', True),
                'temp_location': config.get('temp_location')
            })
            
            audit_step.execute(pipeline, validated_data)
            
    except Exception as e:
        logger.error(f"Error building batch pipeline: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


def build_streaming_pipeline(pipeline: beam.Pipeline, config: CommonPipelineConfig):
    """Build streaming pipeline for mid/long term"""
    logger.info(f"Building streaming pipeline for term: {config.get('term_type')}")
    
    try:
        # Create mapping side input for mid-term
        mapping_side_input = None
        if config.get('term_type') == 'mid':
            mapping_step = CreateMappingSideInput({
                'enabled': True,
                'step_name': 'MappingCache',
                'refresh_interval_seconds': config.get('mapping_refresh_interval_seconds', 600),
                'project': config.get('project_id'),
                'staging_dataset': config.get('staging_dataset'),
                'mapping_table': config.get('mapping_table')
            })
            mapping_side_input = mapping_step.execute(pipeline)
        
        # Step 1: Read from Pub/Sub
        topic_name = config.get('pubsub_topic', '')
        if '/' in topic_name:
            topic_name = topic_name.split('/')[-1]
        
        read_step = ReadFromPubSubStep({
            'enabled': True,
            'step_name': 'ReadPubSub',
            'project': config.get('project_id'),
            'topic': topic_name,
            'parse_notifications': True,
            'window_duration_seconds': config.get('window_duration_seconds'),
            'early_trigger_seconds': config.get('early_trigger_seconds'),
            'late_trigger_seconds': config.get('late_trigger_seconds'),
            'allowed_lateness_seconds': config.get('allowed_lateness_seconds'),
            'accumulation_mode': config.get('accumulation_mode')
        })
        
        # source_data = read_step.execute(pipeline)
        try:
            source_data = read_step.execute(pipeline)
            if not source_data:
                raise RuntimeError("Failed to read source data")
        except Exception as e:
            logger.error(f"Pipeline step failed: {e}")
            # Send alert or fallback logic
            raise

        # Step 2: Enrich with Bigtable (optional)
        enriched_data = source_data
        if config.get('bigtable_instance_id') and config.get('bigtable_table_id'):
            columns_to_fetch = config.get('bigtable_columns_to_fetch')
            if isinstance(columns_to_fetch, str):
                try:
                    columns_to_fetch = json.loads(columns_to_fetch)
                except json.JSONDecodeError:
                    columns_to_fetch = None
            
            enrich_step = BigtableEnrichmentStep({
                'enabled': True,
                'step_name': 'BigtableEnrich',
                'project': config.get('project_id'),
                'instance_id': config.get('bigtable_instance_id'),
                'table_id': config.get('bigtable_table_id'),
                'app_profile_id': config.get('bigtable_app_profile_id'),
                'row_key_field': config.get('bigtable_row_key_field', 'member_number'),
                'columns_to_fetch': columns_to_fetch,
                'timeout': config.get('bigtable_timeout_seconds', 10)
            })
            
            enriched_data = enrich_step.execute(pipeline, source_data)
        
        # Step 3: Data Quality Validation
        dq_step = DataQualityStep({
            'enabled': True,
            'step_name': 'DataQuality',
            'rules': [
                {'type': 'required', 'field': 'member_number'}
            ]
        })
        
        validated_data = dq_step.execute(pipeline, enriched_data)
        
        # Step 4: Process based on term type
        if config.get('term_type') == 'mid':
            # Map to stg_ms_member
            member_mapping_step = ColumnMappingStep({
                'enabled': True,
                'step_name': 'MapToMember',
                'mode': 'streaming',
                'target_table': config.get('stg_origin_table'),
                'mapping_side_input': mapping_side_input
            })
            
            member_data = member_mapping_step.execute(pipeline, validated_data)
            
            # Format for CDC upsert
            cdc_formatted_data = (
                member_data
                | 'FormatForCDC' >> beam.ParDo(CDCUpsertFormatter())
            )
            
            # Write to stg_ms_member with CDC
            write_member_step = WriteToBigQueryStep({
                'enabled': True,
                'step_name': 'WriteMemberCDC',
                'project': config.get('project_id'),
                'dataset': config.get('staging_dataset'),
                'table': config.get('stg_origin_table'),
                'mode': 'WRITE_APPEND',
                'method': config.get('write_method', 'STORAGE_WRITE_API'),
                'use_cdc': True,
                'primary_key': ['member_number'],
                'streaming_mode': config.get('streaming_mode', 'at_least_once'),
                'remove_metadata': True
            })
            
            write_member_step.execute(pipeline, cdc_formatted_data)
            
        elif config.get('term_type') == 'long':
            # Write only to refined
            refined_data = (
                validated_data
                | 'AddRefinedMetadata' >> beam.Map(
                    lambda x: {**x, 'ingested_at': datetime.utcnow().isoformat()}
                )
                | 'FormatLongForCDC' >> beam.ParDo(CDCUpsertFormatter())
            )
            
            write_refined_step = WriteToBigQueryStep({
                'enabled': True,
                'step_name': 'WriteRefinedCDC',
                'project': config.get('project_id'),
                'dataset': config.get('refined_dataset'),
                'table': config.get('refined_ongoing_table'),
                'mode': 'WRITE_APPEND',
                'method': config.get('write_method', 'STORAGE_WRITE_API'),
                'use_cdc': True,
                'primary_key': ['member_number'],
                'streaming_mode': config.get('streaming_mode', 'at_least_once'),
                'remove_metadata': True
            })
            
            write_refined_step.execute(pipeline, refined_data)
        
        # Step 5: Audit Logging (optional)
        if config.get('audit_enabled', True):
            audit_step = AuditLoggingStep({
                'enabled': True,
                'step_name': 'AuditLog',
                'aggregate_windows': True,
                'window_duration_seconds': config.get('audit_window_duration', 3600),
                'pipeline_name': 'ms_member_unified',
                'mode': 'streaming',
                'project': config.get('project_id'),
                'dataset': config.get('staging_dataset'),
                'audit_table': config.get('audit_table')
            })
            
            audit_step.execute(pipeline, validated_data)
            
    except Exception as e:
        logger.error(f"Error building streaming pipeline: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


def run_pipeline(config: CommonPipelineConfig, pipeline_options: PipelineOptions):
    """Main pipeline execution with error handling"""
    logger.info(f"Starting pipeline - Term: {config.get('term_type')}, Mode: {config.get('mode')}")
    logger.info(f"Configuration: {json.dumps(config.to_dict(), indent=2)}")
    
    try:
        with beam.Pipeline(options=pipeline_options) as pipeline:
            if config.get('mode') != 'streaming':
                build_batch_pipeline(pipeline, config)
            # else:
            #     build_streaming_pipeline(pipeline, config)
        
        logger.info("Pipeline execution completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise

class BizOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Core
        parser.add_argument('--term_type')
        parser.add_argument('--mode', default='batch')
        parser.add_argument('--env', default='dev')
        parser.add_argument('--source_project')

        # Datasets
        parser.add_argument('--source_dataset')
        parser.add_argument('--staging_dataset')
        parser.add_argument('--refined_dataset')

        # Tables
        parser.add_argument('--source_table')
        parser.add_argument('--stg_source_table')
        parser.add_argument('--stg_ongoing_source_table')
        parser.add_argument('--stg_origin_table')
        parser.add_argument('--refined_ongoing_table')
        parser.add_argument('--audit_table')
        parser.add_argument('--mapping_table')
        parser.add_argument('--error_table')

        # Batch settings
        parser.add_argument('--min_batch_size', type=int)
        parser.add_argument('--max_batch_size', type=int)
        parser.add_argument('--enrichment_batch_size', type=int)
        parser.add_argument('--batch_limit', type=int)
        parser.add_argument('--partition_filter')
        parser.add_argument('--read_method')
        parser.add_argument('--write_method')

        # Data quality
        parser.add_argument('--max_errors_percent', type=float)
        parser.add_argument('--validation_rules')      # JSON string หรือ yaml ที่แปลงแล้ว
        parser.add_argument('--split_output', type=lambda v: v.lower() == 'true')
        parser.add_argument('--write_errors', type=lambda v: v.lower() == 'true')

        # BigQuery settings
        parser.add_argument('--bq_priority')
        parser.add_argument('--bq_write_disposition')
        parser.add_argument('--bq_create_disposition')

        # Monitoring
        parser.add_argument('--metrics_enabled', type=lambda v: v.lower() == 'true')
        parser.add_argument('--audit_enabled', type=lambda v: v.lower() == 'true')
        parser.add_argument('--error_tracking_enabled', type=lambda v: v.lower() == 'true')

        # Streaming parameters
        parser.add_argument('--pubsub_topic')
        parser.add_argument('--window_duration_seconds', type=int)
        parser.add_argument('--early_trigger_seconds', type=int)
        parser.add_argument('--late_trigger_seconds', type=int)
        parser.add_argument('--allowed_lateness_seconds', type=int)
        parser.add_argument('--accumulation_mode')
        parser.add_argument('--mapping_refresh_interval_seconds', type=int)
        parser.add_argument('--streaming_mode')  # e.g. at_least_once

        # Bigtable
        parser.add_argument('--bigtable_instance_id')
        parser.add_argument('--bigtable_table_id')
        parser.add_argument('--bigtable_app_profile_id')
        parser.add_argument('--bigtable_row_key_field')
        parser.add_argument('--bigtable_columns_to_fetch')   # JSON string
        parser.add_argument('--bigtable_timeout_seconds', type=int)

        # Misc & pipeline-side
        parser.add_argument('--gcs_location')
        # เผื่อ field รวม config ทั้งก้อน
        parser.add_argument('--raw_config')  # JSON string

def main():
    """
    Main entry point for Airflow execution.
    Extracts parameters from PipelineOptions passed by BeamRunPythonPipelineOperator.
    """
    logger.info("Starting Unified Dataflow Pipeline (Airflow Version)")
    
    # Get pipeline options from Airflow
    pipeline_options = PipelineOptions()
    all_options = pipeline_options.get_all_options()
    
    logger.info(f"Received {len(all_options)} options from Airflow")
    
    # Extract configuration parameters
    # >>> NEW: อ่าน custom args ที่ Beam จะไม่ discard แล้ว
    biz = pipeline_options.view_as(BizOptions)
    # สร้าง config_dict จาก biz (และ core project)
    config_dict = {
        # Core
        'project_id': all_options.get('project') or all_options.get('project_id'),
        'source_project': biz.source_project,
        'term_type': biz.term_type,
        'mode': biz.mode,
        'env': biz.env,

        # Datasets
        'source_dataset': biz.source_dataset,
        'staging_dataset': biz.staging_dataset,
        'refined_dataset': biz.refined_dataset,

        # Tables
        'source_table': biz.source_table,
        'stg_source_table': biz.stg_source_table,
        'stg_ongoing_source_table': biz.stg_ongoing_source_table,
        'stg_origin_table': biz.stg_origin_table,
        'refined_ongoing_table': biz.refined_ongoing_table,
        'audit_table': biz.audit_table,
        'mapping_table': biz.mapping_table,
        'error_table': biz.error_table,

        # Batch settings
        'min_batch_size': biz.min_batch_size,
        'max_batch_size': biz.max_batch_size,
        'enrichment_batch_size': biz.enrichment_batch_size,
        'batch_limit': biz.batch_limit,
        'partition_filter': biz.partition_filter,
        'read_method': biz.read_method,
        'write_method': biz.write_method,

        # DQ
        'max_errors_percent': biz.max_errors_percent,
        'validation_rules': biz.validation_rules,
        'split_output': biz.split_output,
        'write_errors': biz.write_errors,

        # BQ
        'bq_priority': biz.bq_priority,
        'bq_write_disposition': biz.bq_write_disposition,
        'bq_create_disposition': biz.bq_create_disposition,

        # Monitoring
        'metrics_enabled': biz.metrics_enabled,
        'audit_enabled': biz.audit_enabled,
        'error_tracking_enabled': biz.error_tracking_enabled,

        # Streaming
        'pubsub_topic': biz.pubsub_topic,
        'window_duration_seconds': biz.window_duration_seconds,
        'early_trigger_seconds': biz.early_trigger_seconds,
        'late_trigger_seconds': biz.late_trigger_seconds,
        'allowed_lateness_seconds': biz.allowed_lateness_seconds,
        'accumulation_mode': biz.accumulation_mode,
        'mapping_refresh_interval_seconds': biz.mapping_refresh_interval_seconds,
        'streaming_mode': biz.streaming_mode,

        # Bigtable
        'bigtable_instance_id': biz.bigtable_instance_id,
        'bigtable_table_id': biz.bigtable_table_id,
        'bigtable_app_profile_id': biz.bigtable_app_profile_id,
        'bigtable_row_key_field': biz.bigtable_row_key_field,
        'bigtable_columns_to_fetch': biz.bigtable_columns_to_fetch,
        'bigtable_timeout_seconds': biz.bigtable_timeout_seconds,

        # Misc
        'gcs_location': biz.gcs_location,
        'temp_location': all_options.get('temp_location'),
        'staging_location': all_options.get('staging_location'),
    }

    
    # Map all expected parameters
    # param_mapping = {
    #     # Core parameters
    #     'project_id': 'project',  # Airflow sends as 'project'
    #     'project': 'project_id',  # Also check both names
    #     'source_project': 'source_project',
    #     'term_type': 'term_type',
    #     'mode': 'mode',
    #     'env': 'env',
        
    #     # Datasets
    #     'source_dataset': 'source_dataset',
    #     'staging_dataset': 'staging_dataset',
    #     'refined_dataset': 'refined_dataset',
        
    #     # Tables
    #     'source_table': 'source_table',
    #     'stg_source_table': 'stg_source_table',
    #     'stg_ongoing_source_table': 'stg_ongoing_source_table',
    #     'stg_origin_table': 'stg_origin_table',
    #     'refined_ongoing_table': 'refined_ongoing_table',
    #     'audit_table': 'audit_table',
    #     'mapping_table': 'mapping_table',
    #     'error_table': 'error_table',
        
    #     # Batch settings
    #     'min_batch_size': 'min_batch_size',
    #     'max_batch_size': 'max_batch_size',
    #     'enrichment_batch_size': 'enrichment_batch_size',
    #     'batch_limit': 'batch_limit',
    #     'partition_filter': 'partition_filter',
    #     'read_method': 'read_method',
    #     'write_method': 'write_method',
        
    #     # Data quality
    #     'max_errors_percent': 'max_errors_percent',
    #     'validation_rules': 'validation_rules',
    #     'split_output': 'split_output',
    #     'write_errors': 'write_errors',
        
    #     # BigQuery settings
    #     'bq_priority': 'bq_priority',
    #     'bq_write_disposition': 'bq_write_disposition',
    #     'bq_create_disposition': 'bq_create_disposition',
        
    #     # Monitoring
    #     'metrics_enabled': 'metrics_enabled',
    #     'audit_enabled': 'audit_enabled',
    #     'error_tracking_enabled': 'error_tracking_enabled',
        
    #     # Streaming parameters
    #     'pubsub_topic': 'pubsub_topic',
    #     'window_duration_seconds': 'window_duration_seconds',
    #     'early_trigger_seconds': 'early_trigger_seconds',
    #     'late_trigger_seconds': 'late_trigger_seconds',
    #     'allowed_lateness_seconds': 'allowed_lateness_seconds',
    #     'accumulation_mode': 'accumulation_mode',
    #     'mapping_refresh_interval_seconds': 'mapping_refresh_interval_seconds',
    #     'streaming_mode': 'streaming_mode',
        
    #     # Bigtable parameters
    #     'bigtable_instance_id': 'bigtable_instance_id',
    #     'bigtable_table_id': 'bigtable_table_id',
    #     'bigtable_app_profile_id': 'bigtable_app_profile_id',
    #     'bigtable_row_key_field': 'bigtable_row_key_field',
    #     'bigtable_columns_to_fetch': 'bigtable_columns_to_fetch',
    #     'bigtable_timeout_seconds': 'bigtable_timeout_seconds',
        
    #     # Pipeline settings
    #     'temp_location': 'temp_location',
    #     'staging_location': 'staging_location',
    #     'runner': 'runner',
    #     'region': 'region',
    #     'job_name': 'job_name',
    #     'service_account_email': 'service_account_email',
    # }
    
    # # Extract values from pipeline options
    # for target_key, source_key in param_mapping.items():
    #     # Check both possible keys
    #     value = all_options.get(source_key) or all_options.get(target_key)
    #     if value is not None:
    #         config_dict[target_key] = value
    
    # # Handle special case for project_id
    # if 'project_id' not in config_dict and 'project' in all_options:
    #     config_dict['project_id'] = all_options['project']
    
    # # Parse JSON fields
    # if 'validation_rules' in config_dict and isinstance(config_dict['validation_rules'], str):
    #     try:
    #         config_dict['validation_rules'] = json.loads(config_dict['validation_rules'])
    #     except json.JSONDecodeError:
    #         logger.warning("Failed to parse validation_rules as JSON")
    
    # # Parse raw_config if provided
    # if 'raw_config' in all_options and all_options['raw_config']:
    #     try:
    #         raw_config = json.loads(all_options['raw_config'])
    #         for key, value in raw_config.items():
    #             if key not in config_dict or config_dict[key] is None:
    #                 config_dict[key] = value
    #     except json.JSONDecodeError as e:
    #         logger.error(f"Failed to parse raw_config: {e}")
    # ----------------------------------------------------------------------------------------------------------------
    # parse JSON fields ถ้าส่งมาเป็น string
    if isinstance(config_dict.get('validation_rules'), str):
        try:
            config_dict['validation_rules'] = json.loads(config_dict['validation_rules'])
        except json.JSONDecodeError:
            logger.warning("Failed to parse validation_rules JSON; using as-is")

    if isinstance(config_dict.get('bigtable_columns_to_fetch'), str):
        try:
            config_dict['bigtable_columns_to_fetch'] = json.loads(config_dict['bigtable_columns_to_fetch'])
        except json.JSONDecodeError:
            pass

    # raw_config (ถ้ามี) → merge เติมค่าที่หายไป
    raw_cfg = getattr(biz, 'raw_config', None)
    if raw_cfg:
        try:
            raw_cfg_json = json.loads(raw_cfg)
            for k, v in raw_cfg_json.items():
                if config_dict.get(k) in (None, ''):
                    config_dict[k] = v
        except json.JSONDecodeError:
            logger.warning("Failed to parse raw_config JSON; ignore")

    # ----------------------------------------------------------------------------------------------------------------
    # # Create configuration object
    # config = CommonPipelineConfig.from_dict(config_dict)
    # # Validate configuration
    # issues = validate_required_params(config, config.get('mode'))
    # if issues:
    #     logger.error("Configuration validation failed:")
    #     for issue in issues:
    #         logger.error(f"  - {issue}")
    #     sys.exit(1)
    # ----------------------------------------------------------------------------------------------------------------
    config = CommonPipelineConfig.from_dict(config_dict)
    # issues = validate_required_params(config, config.get('mode'))
    # if issues:
    #     logger.error("Configuration validation failed:")
    #     for issue in issues:
    #         logger.error(f"  - {issue}")
    #     sys.exit(1)

    # ----------------------------------------------------------------------------------------------------------------
    # Additional validation
    config_issues = config.validate()
    if config_issues:
        for issue in config_issues:
            logger.error(f"Configuration error: {issue}")
        sys.exit(1)
    
    # Configure for Dataflow if runner is DataflowRunner
    if all_options.get('runner') == 'DataflowRunner':
        from apache_beam.options.pipeline_options import (
            GoogleCloudOptions, SetupOptions, WorkerOptions
        )
        
        pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
        pipeline_options.view_as(StandardOptions).streaming = (config.get('mode') == 'streaming')
        
        gcp_options = pipeline_options.view_as(GoogleCloudOptions)
        gcp_options.project = config.get('project_id')
        gcp_options.region = all_options.get('region')
        gcp_options.temp_location = all_options.get('temp_location')
        gcp_options.staging_location = all_options.get('staging_location')
        gcp_options.job_name = all_options.get('job_name')
        
        # Service account
        if all_options.get('service_account_email'):
            gcp_options.service_account_email = all_options['service_account_email']
        
        # Worker options
        worker_options = pipeline_options.view_as(WorkerOptions)
        if all_options.get('machine_type'):
            worker_options.machine_type = all_options['machine_type']
        if all_options.get('max_num_workers'):
            worker_options.max_num_workers = int(all_options['max_num_workers'])
        if all_options.get('no_use_public_ips'):
            worker_options.no_use_public_ips = True
        if all_options.get('network'):
            worker_options.network = all_options['network']
        if all_options.get('subnetwork'):
            worker_options.subnetwork = all_options['subnetwork']
        if all_options.get('worker_zone'):
            worker_options.worker_zone = all_options['worker_zone']
        
        # Setup options
        setup_options = pipeline_options.view_as(SetupOptions)
        setup_options.save_main_session = False
        # if all_options.get('save_main_session'):
        #     setup_options.save_main_session = False
        # if all_options.get('extra_packages'):
        #     setup_options.extra_packages = all_options['extra_packages']
        
        # Experiments
        # if all_options.get('experiments'):
        #     gcp_options.experiments = all_options['experiments']
    
    # Run pipeline
    try:
        run_pipeline(config, pipeline_options)
        logger.info("Pipeline execution initiated successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()