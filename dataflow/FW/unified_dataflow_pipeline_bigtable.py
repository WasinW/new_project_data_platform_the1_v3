#!/usr/bin/env python
"""
Unified Dataflow Pipeline for THE1 Member Data (Refactored)
Supports: Short Term (Batch), Mid Term (Streaming), Long Term (Streaming)
Uses common modules for better maintainability
"""

import argparse
import logging
import json
from datetime import datetime
from pathlib import Path
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Import common modules
import sys
sys.path.append(str(Path(__file__).parent.parent))

from dataflow_common import (
    # PipelineConfig,
    CommonPipelineConfig,  # Changed from PipelineConfig
    DataflowJobConfig,
    # PipelineOrchestrator,
    # ConfigManager,
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

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_batch_pipeline(pipeline: beam.Pipeline, config: CommonPipelineConfig):
    """Build batch pipeline for short term"""
    logger.info(f"Building batch pipeline for term: {config.get('term_type')}")
    
    # Step 1: Read from BigQuery with config values
    read_step = ReadFromBigQueryStep({
        'enabled': True,
        'step_name': 'ReadSource',
        'project': config.project_id,
        'dataset': config.get('source_dataset'),
        'table': f"{config.project_id}.{config.get('source_dataset')}.{config.get('source_table')}",
        'partition_filter': config.get('partition_filter'),
        'method': config.get('read_method')
    })
    
    source_data = read_step.execute(pipeline)
    
    # Step 2: Data Quality Validation with config rules
    validation_rules = config.get('validation_rules')
    if isinstance(validation_rules, str):
        validation_rules = json.loads(validation_rules)
    
    dq_step = DataQualityStep({
        'enabled': True,
        'step_name': 'DataQuality',
        'rules': validation_rules,
        'split_output': config.get('split_output'),
        'max_errors_percent': config.get('max_errors_percent'),
        'error_table': config.get('error_table'),
        'write_errors': config.get('write_errors'),
        'project': config.project_id,
        'dataset': config.get('staging_dataset')
    })
    
    validated_data = dq_step.execute(pipeline, source_data)
    
    # Step 3a: Map and enrich for stg_ms_personas with config batch sizes
    personas_mapping_step = ColumnMappingStep({
        'enabled': True,
        'step_name': 'MapToPersonas',
        'batch_mode': True,
        'target_table': config.get('stg_source_table'),
        'mapping_type': 'source_to_ongoing',
        'min_batch_size': config.get('min_batch_size'),
        'max_batch_size': config.get('max_batch_size'),
        'enrichment_batch_size': config.get('enrichment_batch_size'),
        'project': config.project_id,
        'staging_dataset': config.get('staging_dataset'),
        'stg_origin_table': config.get('stg_origin_table'),
        'mapping_table': config.get('mapping_table')
    })
    
    personas_data = personas_mapping_step.execute(pipeline, validated_data)
    
    # Step 3b: Map and enrich for stg_ms_member with config batch sizes
    member_mapping_step = ColumnMappingStep({
        'enabled': True,
        'step_name': 'MapToMember',
        'batch_mode': True,
        'target_table': config.get('stg_origin_table'),
        'mapping_type': 'source_to_origin',
        'min_batch_size': config.get('min_batch_size'),
        'max_batch_size': config.get('max_batch_size'),
        'enrichment_batch_size': config.get('enrichment_batch_size'),
        'project': config.project_id,
        'staging_dataset': config.get('staging_dataset'),
        'stg_origin_table': config.get('stg_origin_table'),
        'mapping_table': config.get('mapping_table')
    })
    
    member_data = member_mapping_step.execute(pipeline, validated_data)
    
    # Step 4a: Write to stg_ms_personas with config method
    write_personas_step = WriteToBigQueryStep({
        'enabled': True,
        'step_name': 'WritePersonas',
        'project': config.project_id,
        'dataset': config.get('staging_dataset'),
        'table': config.get('stg_source_table'),
        'mode': config.get('bq_write_disposition'),
        'method': config.get('write_method'),
        'create_disposition': config.get('bq_create_disposition'),
        'priority': config.get('bq_priority'),
        'remove_metadata': True
    })
    
    write_personas_step.execute(pipeline, personas_data)
    
    # Step 4b: Write to stg_ms_member with config method
    write_member_step = WriteToBigQueryStep({
        'enabled': True,
        'step_name': 'WriteMember',
        'project': config.project_id,
        'dataset': config.get('staging_dataset'),
        'table': config.get('stg_origin_table'),
        'mode': config.get('bq_write_disposition'),
        'method': config.get('write_method'),
        'create_disposition': config.get('bq_create_disposition'),
        'priority': config.get('bq_priority'),
        'remove_metadata': True
    })
    
    write_member_step.execute(pipeline, member_data)
    
    # Step 5: Audit Logging with config settings
    if config.get('audit_enabled'):
        audit_step = AuditLoggingStep({
            'enabled': True,
            'step_name': 'AuditLog',
            'aggregate_windows': False,
            'pipeline_name': 'ms_member_unified',
            'mode': 'batch',
            'project': config.project_id,
            'dataset': config.get('staging_dataset'),
            'audit_table': config.get('audit_table'),
            'metrics_enabled': config.get('metrics_enabled'),
            'error_tracking_enabled': config.get('error_tracking_enabled')
        })
        
        audit_step.execute(pipeline, validated_data)


def build_streaming_pipeline(pipeline: beam.Pipeline, config: CommonPipelineConfig):
    """Build streaming pipeline for mid/long term"""
    logger.info(f"Building streaming pipeline for term: {config.get('term_type')}")
    
    # Create mapping side input for mid-term
    mapping_side_input = None
    if config.get('term_type') == 'mid':
        mapping_step = CreateMappingSideInput({
            'enabled': True,
            'step_name': 'MappingCache',
            'refresh_interval_seconds': config.get('mapping_refresh_interval_seconds'),
            'project': config.project_id,
            'staging_dataset': config.get('staging_dataset'),
            'mapping_table': config.get('mapping_table')
        })
        mapping_side_input = mapping_step.execute(pipeline)
    
    # Step 1: Read from Pub/Sub with config values
    read_step = ReadFromPubSubStep({
        'enabled': True,
        'step_name': 'ReadPubSub',
        'project': config.project_id,
        'topic': config.get('pubsub_topic', '').split('/')[-1] if '/' in config.get('pubsub_topic', '') else config.get('pubsub_topic'),
        'parse_notifications': True,
        'window_duration_seconds': config.get('window_duration_seconds'),
        'early_trigger_seconds': config.get('early_trigger_seconds'),
        'late_trigger_seconds': config.get('late_trigger_seconds'),
        'allowed_lateness_seconds': config.get('allowed_lateness_seconds'),
        'accumulation_mode': config.get('accumulation_mode')
    })
    
    source_data = read_step.execute(pipeline)
    
    # Step 2: Enrich with Bigtable using config
    if config.get('bigtable_instance_id') and config.get('bigtable_table_id'):
        # Parse columns_to_fetch if it's a JSON string
        columns_to_fetch = config.get('bigtable_columns_to_fetch')
        if isinstance(columns_to_fetch, str):
            columns_to_fetch = json.loads(columns_to_fetch)
        
        enrich_step = BigtableEnrichmentStep({
            'enabled': True,
            'step_name': 'BigtableEnrich',
            'project': config.project_id,
            'instance_id': config.get('bigtable_instance_id'),
            'table_id': config.get('bigtable_table_id'),
            'app_profile_id': config.get('bigtable_app_profile_id'),
            'row_key_field': config.get('bigtable_row_key_field'),
            'columns_to_fetch': columns_to_fetch,
            'timeout': config.get('bigtable_timeout_seconds')
        })
        
        enriched_data = enrich_step.execute(pipeline, source_data)
    else:
        enriched_data = source_data
    
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
    if config.term_type == 'mid':
        # Write to both staging and refined
        
        # Map to stg_ms_member
        member_mapping_step = ColumnMappingStep({
            'enabled': True,
            'step_name': 'MapToMember',
            'streaming_mode': True,
            'target_table': config.stg_origin_table,
            'mapping_type': 'source_to_origin',
            'mapping_side_input': mapping_side_input
        })
        
        member_data = member_mapping_step.execute(pipeline, validated_data)
        
        # Write to stg_ms_member
        write_member_step = WriteToBigQueryStep({
            'enabled': True,
            'step_name': 'WriteMember',
            'project': config.project_id,
            'dataset': config.staging_dataset,
            'table': config.stg_origin_table,
            'mode': 'WRITE_APPEND',
            'method': 'STORAGE_WRITE_API',
            'streaming_mode': config.streaming_mode,
            'remove_metadata': True
        })
        
        write_member_step.execute(pipeline, member_data)
        
        # Write to refined
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
            'dataset': config.refined_dataset,
            'table': config.refined_ongoing_table,
            'mode': 'WRITE_APPEND',
            'method': 'STORAGE_WRITE_API',
            'streaming_mode': config.streaming_mode,
            'remove_metadata': True
        })
        
        write_refined_step.execute(pipeline, refined_data)
        
    elif config.term_type == 'long':
        # Write only to refined
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
            'dataset': config.refined_dataset,
            'table': config.refined_ongoing_table,
            'mode': 'WRITE_APPEND',
            'method': 'STORAGE_WRITE_API',
            'streaming_mode': config.streaming_mode,
            'remove_metadata': True
        })
        
        write_refined_step.execute(pipeline, refined_data)
    
    # Step 5: Windowed Audit Logging
    audit_step = AuditLoggingStep({
        'enabled': True,
        'step_name': 'AuditLog',
        'aggregate_windows': True,
        'window_duration_seconds': config.audit_window_duration,
        'pipeline_name': 'ms_member_unified',
        'mode': 'streaming',
        'project': config.project_id,
        'dataset': config.staging_dataset,
        'audit_table': config.audit_table
    })
    
    audit_step.execute(pipeline, validated_data)


def run_pipeline(config: CommonPipelineConfig, pipeline_options: PipelineOptions):
    """Main pipeline execution"""
    logger.info(f"Starting pipeline - Term: {config.get('term_type')}, Mode: {config.mode}")
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        if config.mode == 'streaming':
            build_streaming_pipeline(pipeline, config)
        else:
            build_batch_pipeline(pipeline, config)
    
    logger.info("Pipeline execution completed")


def main():
    """Main entry point - NO HARD CODED DEFAULTS"""
    parser = argparse.ArgumentParser(description='Unified MS Member Pipeline (v3 - No Defaults)')
    
    # Required parameters - NO DEFAULTS
    parser.add_argument('--project_id', required=True, help='GCP Project ID')
    parser.add_argument('--term_type', required=True, help='Term type')
    parser.add_argument('--mode', required=True, help='Processing mode')
    parser.add_argument('--env', required=True, help='Environment')
    
    # Dataset parameters - NO DEFAULTS
    parser.add_argument('--source_dataset', required=True)
    parser.add_argument('--staging_dataset', required=True)
    parser.add_argument('--refined_dataset', required=True)
    
    # Table parameters - NO DEFAULTS
    parser.add_argument('--source_table', required=True)
    parser.add_argument('--stg_source_table', required=True)
    parser.add_argument('--stg_origin_table', required=True)
    parser.add_argument('--refined_ongoing_table', required=True)
    parser.add_argument('--audit_table', required=True)
    parser.add_argument('--mapping_table', required=True)
    parser.add_argument('--error_table')
    
    # Batch processing parameters
    parser.add_argument('--min_batch_size', type=int)
    parser.add_argument('--max_batch_size', type=int)
    parser.add_argument('--enrichment_batch_size', type=int)
    parser.add_argument('--partition_filter')
    parser.add_argument('--read_method')
    parser.add_argument('--write_method')
    
    # Streaming parameters
    parser.add_argument('--pubsub_topic')
    parser.add_argument('--window_duration_seconds', type=int)
    parser.add_argument('--early_trigger_seconds', type=int)
    parser.add_argument('--late_trigger_seconds', type=int)
    parser.add_argument('--allowed_lateness_seconds', type=int)
    parser.add_argument('--accumulation_mode')
    parser.add_argument('--mapping_refresh_interval_seconds', type=int)
    
    # Bigtable parameters
    parser.add_argument('--bigtable_instance_id')
    parser.add_argument('--bigtable_table_id')
    parser.add_argument('--bigtable_app_profile_id')
    parser.add_argument('--bigtable_row_key_field')
    parser.add_argument('--bigtable_columns_to_fetch')  # JSON string
    parser.add_argument('--bigtable_timeout_seconds', type=int)
    
    # Data quality parameters
    parser.add_argument('--max_errors_percent', type=float)
    parser.add_argument('--validation_rules')  # JSON string
    parser.add_argument('--split_output', type=bool)
    parser.add_argument('--write_errors', type=bool)
    
    # BigQuery parameters
    parser.add_argument('--bq_priority')
    parser.add_argument('--bq_write_disposition')
    parser.add_argument('--bq_create_disposition')
    
    # Monitoring parameters
    parser.add_argument('--metrics_enabled', type=bool)
    parser.add_argument('--audit_enabled', type=bool)
    parser.add_argument('--error_tracking_enabled', type=bool)
    
    # Retry parameters
    parser.add_argument('--max_retries', type=int)
    parser.add_argument('--initial_backoff', type=int)
    parser.add_argument('--max_backoff', type=int)
    
    # Security parameters
    parser.add_argument('--specific_sa')
    parser.add_argument('--streaming_mode')
    
    # Raw config JSON (for complex nested configs)
    parser.add_argument('--raw_config')
    
    # Pipeline runner arguments
    parser.add_argument('--runner', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    parser.add_argument('--service_account_email')
    
    args, beam_args = parser.parse_known_args()
    
    # Create config from arguments
    config_dict = vars(args)
    
    # If raw_config provided, merge it (for complex nested structures)
    if args.raw_config:
        try:
            raw_config = json.loads(args.raw_config)
            # Raw config has lower priority than explicit args
            for key, value in raw_config.items():
                if key not in config_dict or config_dict[key] is None:
                    config_dict[key] = value
        except json.JSONDecodeError:
            logger.warning("Failed to parse raw_config JSON")
    
    # Create CommonPipelineConfig
    config = CommonPipelineConfig.from_dict(config_dict)
    
    # Validate that we have all required fields
    required_fields = [
        'project_id', 'term_type', 'mode', 'env',
        'source_dataset', 'staging_dataset', 'refined_dataset',
        'source_table', 'stg_source_table', 'stg_origin_table',
        'refined_ongoing_table', 'audit_table', 'mapping_table'
    ]
    
    missing_fields = [f for f in required_fields if not config.get(f)]
    if missing_fields:
        raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
    
    # Additional validation
    issues = config.validate()
    if issues:
        for issue in issues:
            logger.error(f"Configuration error: {issue}")
        raise ValueError("Invalid configuration")
    
    # # Build pipeline options
    # if args.config_file and 'dataflow' in orchestrator.config:
    #     # Use config file for Dataflow options
    #     df_config = DataflowJobConfig.from_dict(orchestrator.config)
    #     pipeline_options = PipelineOptions(**df_config.to_pipeline_options())
    # else:
    #     # Build from command line arguments
    #     pipeline_options = PipelineOptions(beam_args)
    
    pipeline_options = PipelineOptions(beam_args)
    pipeline_options.view_as(StandardOptions).runner = args.runner
    
    if args.runner == 'DataflowRunner':
        from apache_beam.options.pipeline_options import (
            GoogleCloudOptions, SetupOptions, WorkerOptions
        )
        
        pipeline_options.view_as(StandardOptions).streaming = (config.mode == 'streaming')
        
        gcp_options = pipeline_options.view_as(GoogleCloudOptions)
        gcp_options.project = config.project_id
        gcp_options.region = args.region
        gcp_options.temp_location = args.temp_location
        gcp_options.staging_location = args.staging_location
        gcp_options.job_name = f"ms-member-{config.get('term_type')}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        if args.service_account_email:
            gcp_options.service_account_email = args.service_account_email
        
        # Use worker configuration from parameters
        worker_options = pipeline_options.view_as(WorkerOptions)
        if config.get('machine_type'):
            worker_options.machine_type = config.get('machine_type')
        if config.get('max_num_workers'):
            worker_options.max_num_workers = config.get('max_num_workers')
        
        if config.mode == 'streaming' and config.get('enable_streaming_engine'):
            gcp_options.enable_streaming_engine = True
        
        setup_options = pipeline_options.view_as(SetupOptions)
        if config.get('save_main_session'):
            setup_options.save_main_session = config.get('save_main_session')
    
    # Run pipeline
    run_pipeline(config, pipeline_options)
    logger.info("Pipeline execution initiated successfully")


def run_pipeline(config: CommonPipelineConfig, pipeline_options: PipelineOptions):
    """Main pipeline execution"""
    logger.info(f"Starting pipeline - Term: {config.get('term_type')}, Mode: {config.mode}")
    logger.info(f"Configuration: {config.to_dict()}")
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        if config.mode == 'streaming':
            build_streaming_pipeline(pipeline, config)
        else:
            build_batch_pipeline(pipeline, config)
    
    logger.info("Pipeline execution completed")


if __name__ == '__main__':
    main()
