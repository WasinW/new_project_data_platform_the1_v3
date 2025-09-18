#!/usr/bin/env python
"""
Unified Dataflow Pipeline for THE1 Member Data (Refactored v3)
Supports: Short Term (Batch), Mid Term (Streaming), Long Term (Streaming)
NO HARD-CODED VALUES - All configuration from external sources
"""

import argparse
import logging
import json
import sys
import os
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from typing import Optional, Dict, Any, List

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
        WindowedAuditLogger
    )
except ImportError as e:
    logger.error(f"Failed to import dataflow_common modules: {e}")
    logger.error("Make sure dataflow_common package is installed or in PYTHONPATH")
    sys.exit(1)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def validate_required_params(config: CommonPipelineConfig, mode: str) -> List[str]:
    """Validate that all required parameters are present based on mode"""
    issues = []
    
    # Common required fields
    required_common = [
        'project_id', 'env', 'term_type', 'mode',
        'source_dataset', 'staging_dataset', 'refined_dataset',
        'source_table', 'stg_source_table', 'stg_origin_table',
        'refined_ongoing_table', 'audit_table', 'mapping_table'
    ]
    
    for field in required_common:
        if not config.get(field):
            issues.append(f"Missing required field: {field}")
    
    # Mode-specific validation
    if mode == 'batch':
        batch_fields = [
            'min_batch_size', 'max_batch_size', 'enrichment_batch_size',
            'partition_filter', 'read_method', 'write_method'
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
        # Step 1: Read from BigQuery with config values
        read_step = ReadFromBigQueryStep({
            'enabled': True,
            'step_name': 'ReadSource',
            'project': config.project_id,
            'dataset': config.get('source_dataset'),
            'table': f"{config.project_id}.{config.get('source_dataset')}.{config.get('source_table')}",
            'partition_filter': config.get('partition_filter'),
            'method': config.get('read_method', 'DIRECT_READ')
        })
        
        source_data = read_step.execute(pipeline)
        
        # Step 2: Data Quality Validation with config rules
        validation_rules = config.get('validation_rules')
        if isinstance(validation_rules, str):
            try:
                validation_rules = json.loads(validation_rules)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse validation_rules JSON: {validation_rules}")
                validation_rules = [{'type': 'required', 'field': 'member_number'}]
        
        dq_step = DataQualityStep({
            'enabled': True,
            'step_name': 'DataQuality',
            'rules': validation_rules or [{'type': 'required', 'field': 'member_number'}],
            'split_output': config.get('split_output', False),
            'max_errors_percent': config.get('max_errors_percent', 0.1),
            'error_table': config.get('error_table'),
            'write_errors': config.get('write_errors', True),
            'project': config.project_id,
            'dataset': config.get('staging_dataset')
        })
        
        validated_data = dq_step.execute(pipeline, source_data)
        
        # Step 3a: Map and enrich for stg_ms_personas
        personas_mapping_step = ColumnMappingStep({
            'enabled': True,
            'step_name': 'MapToPersonas',
            'batch_mode': True,
            'target_table': config.get('stg_source_table'),
            'mapping_type': 'source_to_ongoing',
            **config.to_dict()
        })
        
        personas_data = personas_mapping_step.execute(pipeline, validated_data)
        
        # Step 3b: Map and enrich for stg_ms_member
        member_mapping_step = ColumnMappingStep({
            'enabled': True,
            'step_name': 'MapToMember',
            'batch_mode': True,
            'target_table': config.get('stg_origin_table'),
            'mapping_type': 'source_to_origin',
            **config.to_dict()
        })
        
        member_data = member_mapping_step.execute(pipeline, validated_data)
        
        # Step 4a: Write to stg_ms_personas
        write_personas_step = WriteToBigQueryStep({
            'enabled': True,
            'step_name': 'WritePersonas',
            'project': config.project_id,
            'dataset': config.get('staging_dataset'),
            'table': config.get('stg_source_table'),
            'mode': config.get('bq_write_disposition', 'WRITE_APPEND'),
            'method': config.get('write_method', 'FILE_LOADS'),
            'create_disposition': config.get('bq_create_disposition', 'CREATE_IF_NEEDED'),
            'priority': config.get('bq_priority', 'INTERACTIVE'),
            'remove_metadata': True
        })
        
        write_personas_step.execute(pipeline, personas_data)
        
        # Step 4b: Write to stg_ms_member
        write_member_step = WriteToBigQueryStep({
            'enabled': True,
            'step_name': 'WriteMember',
            'project': config.project_id,
            'dataset': config.get('staging_dataset'),
            'table': config.get('stg_origin_table'),
            'mode': config.get('bq_write_disposition', 'WRITE_APPEND'),
            'method': config.get('write_method', 'FILE_LOADS'),
            'create_disposition': config.get('bq_create_disposition', 'CREATE_IF_NEEDED'),
            'priority': config.get('bq_priority', 'INTERACTIVE'),
            'remove_metadata': True
        })
        
        write_member_step.execute(pipeline, member_data)
        
        # Step 5: Audit Logging
        if config.get('audit_enabled', True):
            audit_step = AuditLoggingStep({
                'enabled': True,
                'step_name': 'AuditLog',
                'aggregate_windows': False,
                'pipeline_name': 'ms_member_unified',
                'mode': 'batch',
                'project': config.project_id,
                'dataset': config.get('staging_dataset'),
                'audit_table': config.get('audit_table'),
                'metrics_enabled': config.get('metrics_enabled', True),
                'error_tracking_enabled': config.get('error_tracking_enabled', True)
            })
            
            audit_step.execute(pipeline, validated_data)
            
    except Exception as e:
        logger.error(f"Error building batch pipeline: {e}")
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
                'project': config.project_id,
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
            'project': config.project_id,
            'topic': topic_name,
            'parse_notifications': True,
            'window_duration_seconds': config.get('window_duration_seconds'),
            'early_trigger_seconds': config.get('early_trigger_seconds'),
            'late_trigger_seconds': config.get('late_trigger_seconds'),
            'allowed_lateness_seconds': config.get('allowed_lateness_seconds'),
            'accumulation_mode': config.get('accumulation_mode')
        })
        
        source_data = read_step.execute(pipeline)
        
        # Step 2: Enrich with Bigtable
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
                'project': config.project_id,
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
                'streaming_mode': True,
                'target_table': config.get('stg_origin_table'),
                'mapping_type': 'source_to_origin',
                'mapping_side_input': mapping_side_input
            })
            
            member_data = member_mapping_step.execute(pipeline, validated_data)
            
            # Write to stg_ms_member
            write_member_step = WriteToBigQueryStep({
                'enabled': True,
                'step_name': 'WriteMember',
                'project': config.project_id,
                'dataset': config.get('staging_dataset'),
                'table': config.get('stg_origin_table'),
                'mode': 'WRITE_APPEND',
                'method': config.get('write_method', 'STORAGE_WRITE_API'),
                'streaming_mode': config.get('streaming_mode', 'at_least_once'),
                'remove_metadata': True
            })
            
            write_member_step.execute(pipeline, member_data)
            
            # Also write to refined
            refined_data = (
                validated_data
                | 'AddRefinedMetadata' >> beam.Map(
                    lambda x: {**x, 'ingested_at': datetime.utcnow().isoformat()}
                )
                | 'AuditRefinedWrite' >> beam.ParDo(WindowedAuditLogger('refined_write'))
            )
            
            write_refined_step = WriteToBigQueryStep({
                'enabled': True,
                'step_name': 'WriteRefined',
                'project': config.project_id,
                'dataset': config.get('refined_dataset'),
                'table': config.get('refined_ongoing_table'),
                'mode': 'WRITE_APPEND',
                'method': config.get('write_method', 'STORAGE_WRITE_API'),
                'streaming_mode': config.get('streaming_mode', 'at_least_once'),
                'remove_metadata': True
            })
            
            write_refined_step.execute(pipeline, refined_data)
            
        elif config.get('term_type') == 'long':
            # Write only to refined
            refined_data = (
                validated_data
                | 'AddRefinedMetadataLong' >> beam.Map(
                    lambda x: {**x, 'ingested_at': datetime.utcnow().isoformat()}
                )
                | 'AuditRefinedWriteLong' >> beam.ParDo(WindowedAuditLogger('refined_write_long'))
            )
            
            write_refined_step = WriteToBigQueryStep({
                'enabled': True,
                'step_name': 'WriteRefinedLong',
                'project': config.project_id,
                'dataset': config.get('refined_dataset'),
                'table': config.get('refined_ongoing_table'),
                'mode': 'WRITE_APPEND',
                'method': config.get('write_method', 'STORAGE_WRITE_API'),
                'streaming_mode': config.get('streaming_mode', 'at_least_once'),
                'remove_metadata': True
            })
            
            write_refined_step.execute(pipeline, refined_data)
        
        # Step 5: Windowed Audit Logging
        if config.get('audit_enabled', True):
            audit_step = AuditLoggingStep({
                'enabled': True,
                'step_name': 'AuditLog',
                'aggregate_windows': True,
                'window_duration_seconds': config.get('audit_window_duration', 3600),
                'pipeline_name': 'ms_member_unified',
                'mode': 'streaming',
                'project': config.project_id,
                'dataset': config.get('staging_dataset'),
                'audit_table': config.get('audit_table')
            })
            
            audit_step.execute(pipeline, validated_data)
            
    except Exception as e:
        logger.error(f"Error building streaming pipeline: {e}")
        raise


def run_pipeline(config: CommonPipelineConfig, pipeline_options: PipelineOptions):
    """Main pipeline execution with error handling"""
    logger.info(f"Starting pipeline - Term: {config.get('term_type')}, Mode: {config.get('mode')}")
    logger.info(f"Configuration: {config.to_dict()}")
    
    try:
        with beam.Pipeline(options=pipeline_options) as pipeline:
            if config.get('mode') == 'streaming':
                build_streaming_pipeline(pipeline, config)
            else:
                build_batch_pipeline(pipeline, config)
        
        logger.info("Pipeline execution completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


def main():
    """Main entry point - NO HARD CODED DEFAULTS"""
    parser = argparse.ArgumentParser(
        description='Unified MS Member Pipeline (v3 - No Defaults)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Required parameters - NO DEFAULTS
    parser.add_argument('--project_id', required=True, help='GCP Project ID')
    parser.add_argument('--term_type', required=True, choices=['short', 'mid', 'long'], help='Term type')
    parser.add_argument('--mode', required=True, choices=['batch', 'streaming'], help='Processing mode')
    parser.add_argument('--env', required=True, help='Environment (dev/staging/prod)')
    
    # Dataset parameters - NO DEFAULTS
    parser.add_argument('--source_dataset', required=True, help='Source dataset')
    parser.add_argument('--staging_dataset', required=True, help='Staging dataset')
    parser.add_argument('--refined_dataset', required=True, help='Refined dataset')
    
    # Table parameters - NO DEFAULTS
    parser.add_argument('--source_table', required=True, help='Source table')
    parser.add_argument('--stg_source_table', required=True, help='Staging source table (personas)')
    parser.add_argument('--stg_origin_table', required=True, help='Staging origin table (member)')
    parser.add_argument('--refined_ongoing_table', required=True, help='Refined ongoing table')
    parser.add_argument('--audit_table', required=True, help='Audit table')
    parser.add_argument('--mapping_table', required=True, help='Mapping table')
    parser.add_argument('--error_table', help='Error table for data quality failures')
    
    # Batch processing parameters
    parser.add_argument('--min_batch_size', type=int, help='Minimum batch size')
    parser.add_argument('--max_batch_size', type=int, help='Maximum batch size')
    parser.add_argument('--enrichment_batch_size', type=int, help='Enrichment batch size')
    parser.add_argument('--partition_filter', help='BigQuery partition filter')
    parser.add_argument('--read_method', help='BigQuery read method')
    parser.add_argument('--write_method', help='BigQuery write method')
    
    # Streaming parameters
    parser.add_argument('--pubsub_topic', help='Pub/Sub topic')
    parser.add_argument('--window_duration_seconds', type=int, help='Window duration in seconds')
    parser.add_argument('--early_trigger_seconds', type=int, help='Early trigger in seconds')
    parser.add_argument('--late_trigger_seconds', type=int, help='Late trigger in seconds')
    parser.add_argument('--allowed_lateness_seconds', type=int, help='Allowed lateness in seconds')
    parser.add_argument('--accumulation_mode', help='Window accumulation mode')
    parser.add_argument('--mapping_refresh_interval_seconds', type=int, help='Mapping refresh interval')
    
    # Bigtable parameters
    parser.add_argument('--bigtable_instance_id', help='Bigtable instance ID')
    parser.add_argument('--bigtable_table_id', help='Bigtable table ID')
    parser.add_argument('--bigtable_app_profile_id', help='Bigtable app profile ID')
    parser.add_argument('--bigtable_row_key_field', help='Bigtable row key field')
    parser.add_argument('--bigtable_columns_to_fetch', help='Columns to fetch (JSON)')
    parser.add_argument('--bigtable_timeout_seconds', type=int, help='Bigtable timeout')
    
    # Data quality parameters
    parser.add_argument('--max_errors_percent', type=float, help='Maximum error percentage')
    parser.add_argument('--validation_rules', help='Validation rules (JSON)')
    parser.add_argument('--split_output', action='store_true', help='Split good/bad records')
    parser.add_argument('--write_errors', action='store_true', help='Write errors to table')
    
    # BigQuery parameters
    parser.add_argument('--bq_priority', help='BigQuery priority')
    parser.add_argument('--bq_write_disposition', help='BigQuery write disposition')
    parser.add_argument('--bq_create_disposition', help='BigQuery create disposition')
    
    # Monitoring parameters
    parser.add_argument('--metrics_enabled', action='store_true', help='Enable metrics')
    parser.add_argument('--audit_enabled', action='store_true', help='Enable audit logging')
    parser.add_argument('--error_tracking_enabled', action='store_true', help='Enable error tracking')
    
    # Retry parameters
    parser.add_argument('--max_retries', type=int, help='Maximum retries')
    parser.add_argument('--initial_backoff', type=int, help='Initial backoff seconds')
    parser.add_argument('--max_backoff', type=int, help='Maximum backoff seconds')
    
    # Security parameters
    parser.add_argument('--specific_sa', help='Specific service account (deprecated)')
    parser.add_argument('--service_account_email', help='Service account email')
    parser.add_argument('--streaming_mode', help='Streaming mode (at_least_once/exactly_once)')
    
    # Raw config JSON (for complex nested configs)
    parser.add_argument('--raw_config', help='Raw configuration JSON')
    
    # Pipeline runner arguments
    parser.add_argument('--runner', default='DirectRunner', help='Pipeline runner')
    parser.add_argument('--region', help='GCP region')
    parser.add_argument('--temp_location', help='Temp location for Dataflow')
    parser.add_argument('--staging_location', help='Staging location for Dataflow')
    parser.add_argument('--job_name', help='Dataflow job name')
    parser.add_argument('--machine_type', help='Machine type for workers')
    parser.add_argument('--max_num_workers', type=int, help='Maximum number of workers')
    parser.add_argument('--save_main_session', action='store_true', help='Save main session')
    parser.add_argument('--setup_file', help='Setup file for dependencies')
    parser.add_argument('--requirements_file', help='Requirements file for dependencies')
    
    args, beam_args = parser.parse_known_args()
    
    # Create config from arguments
    config_dict = vars(args)
    
    # If raw_config provided, merge it
    if args.raw_config:
        try:
            raw_config = json.loads(args.raw_config)
            for key, value in raw_config.items():
                if key not in config_dict or config_dict[key] is None:
                    config_dict[key] = value
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse raw_config JSON: {e}")
            sys.exit(1)
    
    # Create CommonPipelineConfig
    config = CommonPipelineConfig.from_dict(config_dict)
    
    # Validate configuration
    issues = validate_required_params(config, config.get('mode'))
    if issues:
        logger.error("Configuration validation failed:")
        for issue in issues:
            logger.error(f"  - {issue}")
        sys.exit(1)
    
    # Additional validation
    config_issues = config.validate()
    if config_issues:
        for issue in config_issues:
            logger.error(f"Configuration error: {issue}")
        sys.exit(1)
    
    # Build pipeline options
    pipeline_options = PipelineOptions(beam_args)
    pipeline_options.view_as(StandardOptions).runner = args.runner
    
    if args.runner == 'DataflowRunner':
        from apache_beam.options.pipeline_options import (
            GoogleCloudOptions, SetupOptions, WorkerOptions
        )
        
        # Validate Dataflow-specific requirements
        if not args.temp_location or not args.staging_location or not args.region:
            logger.error("DataflowRunner requires --temp_location, --staging_location, and --region")
            sys.exit(1)
        
        pipeline_options.view_as(StandardOptions).streaming = (config.get('mode') == 'streaming')
        
        gcp_options = pipeline_options.view_as(GoogleCloudOptions)
        gcp_options.project = config.project_id
        gcp_options.region = args.region
        gcp_options.temp_location = args.temp_location
        gcp_options.staging_location = args.staging_location
        
        # Set job name
        if args.job_name:
            gcp_options.job_name = args.job_name
        else:
            gcp_options.job_name = f"ms-member-{config.get('term_type')}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        # Set service account
        if args.service_account_email:
            gcp_options.service_account_email = args.service_account_email
        elif args.specific_sa:  # Backward compatibility
            gcp_options.service_account_email = args.specific_sa
        
        # Worker configuration
        worker_options = pipeline_options.view_as(WorkerOptions)
        if args.machine_type:
            worker_options.machine_type = args.machine_type
        if args.max_num_workers:
            worker_options.max_num_workers = args.max_num_workers
        
        # Streaming engine
        if config.get('mode') == 'streaming' and config.get('enable_streaming_engine'):
            gcp_options.enable_streaming_engine = True
        
        # Setup options
        setup_options = pipeline_options.view_as(SetupOptions)
        if args.save_main_session:
            setup_options.save_main_session = True
        if args.setup_file:
            setup_options.setup_file = args.setup_file
        if args.requirements_file:
            setup_options.requirements_file = args.requirements_file
    
    # Run pipeline
    try:
        run_pipeline(config, pipeline_options)
        logger.info("Pipeline execution initiated successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()