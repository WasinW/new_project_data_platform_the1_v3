#!/usr/bin/env python
"""
Unified Dataflow Pipeline for THE1 Member Data (Refactored)
Supports: Short Term (Batch), Mid Term (Streaming), Long Term (Streaming)
Uses common modules for better maintainability
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Import common modules
import sys
sys.path.append(str(Path(__file__).parent.parent))

from dataflow_common import (
    PipelineConfig,
    DataflowJobConfig,
    PipelineOrchestrator,
    ConfigManager,
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


def build_batch_pipeline(pipeline: beam.Pipeline, config: PipelineConfig):
    """Build batch pipeline for short term"""
    logger.info(f"Building batch pipeline for term: {config.term_type}")
    
    # Step 1: Read from BigQuery
    read_step = ReadFromBigQueryStep({
        'enabled': True,
        'step_name': 'ReadSource',
        'project': config.project_id,
        'dataset': config.source_dataset,
        'table': f"{config.project_id}.{config.source_dataset}.{config.source_table}",
        'partition_filter': 'DATE(_PARTITIONTIME) = CURRENT_DATE()',
        'method': 'DIRECT_READ'
    })
    
    source_data = read_step.execute(pipeline)
    
    # Step 2: Data Quality Validation
    dq_step = DataQualityStep({
        'enabled': True,
        'step_name': 'DataQuality',
        'rules': [
            {'type': 'required', 'field': 'member_number'},
            {'type': 'not_null', 'field': 'member_number'}
        ],
        'split_output': False
    })
    
    validated_data = dq_step.execute(pipeline, source_data)
    
    # Step 3a: Map and enrich for stg_ms_personas (ongoing)
    personas_mapping_step = ColumnMappingStep({
        'enabled': True,
        'step_name': 'MapToPersonas',
        'batch_mode': True,
        'target_table': config.stg_source_table,
        'mapping_type': 'source_to_ongoing',
        'min_batch_size': 100,
        'max_batch_size': 500,
        'project': config.project_id,
        'datasets': {
            'source_dataset': config.source_dataset,
            'staging_dataset': config.staging_dataset,
            'refined_dataset': config.refined_dataset
        },
        'tables': {
            'stg_origin_table': config.stg_origin_table,
            'mapping_table': config.mapping_table
        }
    })
    
    personas_data = personas_mapping_step.execute(pipeline, validated_data)
    
    # Step 3b: Map and enrich for stg_ms_member (origin)
    member_mapping_step = ColumnMappingStep({
        'enabled': True,
        'step_name': 'MapToMember',
        'batch_mode': True,
        'target_table': config.stg_origin_table,
        'mapping_type': 'source_to_origin',
        'min_batch_size': 100,
        'max_batch_size': 500,
        'project': config.project_id,
        'datasets': {
            'source_dataset': config.source_dataset,
            'staging_dataset': config.staging_dataset,
            'refined_dataset': config.refined_dataset
        },
        'tables': {
            'stg_origin_table': config.stg_origin_table,
            'mapping_table': config.mapping_table
        }
    })
    
    member_data = member_mapping_step.execute(pipeline, validated_data)
    
    # Step 4a: Write to stg_ms_personas
    write_personas_step = WriteToBigQueryStep({
        'enabled': True,
        'step_name': 'WritePersonas',
        'project': config.project_id,
        'dataset': config.staging_dataset,
        'table': config.stg_source_table,
        'mode': 'WRITE_APPEND',
        'method': 'FILE_LOADS',
        'remove_metadata': True
    })
    
    write_personas_step.execute(pipeline, personas_data)
    
    # Step 4b: Write to stg_ms_member
    write_member_step = WriteToBigQueryStep({
        'enabled': True,
        'step_name': 'WriteMember',
        'project': config.project_id,
        'dataset': config.staging_dataset,
        'table': config.stg_origin_table,
        'mode': 'WRITE_APPEND',
        'method': 'FILE_LOADS',
        'remove_metadata': True
    })
    
    write_member_step.execute(pipeline, member_data)
    
    # Step 5: Audit Logging
    audit_step = AuditLoggingStep({
        'enabled': True,
        'step_name': 'AuditLog',
        'aggregate_windows': False,
        'pipeline_name': 'ms_member_unified',
        'mode': 'batch',
        'project': config.project_id,
        'dataset': config.staging_dataset,
        'audit_table': config.audit_table
    })
    
    audit_step.execute(pipeline, validated_data)


def build_streaming_pipeline(pipeline: beam.Pipeline, config: PipelineConfig):
    """Build streaming pipeline for mid/long term"""
    logger.info(f"Building streaming pipeline for term: {config.term_type}")
    
    # Create mapping side input for mid-term
    mapping_side_input = None
    if config.term_type == 'mid':
        mapping_step = CreateMappingSideInput({
            'enabled': True,
            'step_name': 'MappingCache',
            'refresh_interval_seconds': config.mapping_refresh_interval,
            'project': config.project_id,
            'datasets': {
                'staging_dataset': config.staging_dataset
            },
            'tables': {
                'mapping_table': config.mapping_table
            }
        })
        mapping_side_input = mapping_step.execute(pipeline)
    
    # Step 1: Read from Pub/Sub
    read_step = ReadFromPubSubStep({
        'enabled': True,
        'step_name': 'ReadPubSub',
        'project': config.project_id,
        'topic': config.pubsub_topic.split('/')[-1] if '/' in config.pubsub_topic else config.pubsub_topic,
        'parse_notifications': True,
        'window_duration_seconds': config.window_duration,
        'early_trigger_seconds': 60
    })
    
    source_data = read_step.execute(pipeline)
    
    # Step 2: Enrich with Bigtable
    if config.bigtable_instance_id and config.bigtable_table_id:
        enrich_step = BigtableEnrichmentStep({
            'enabled': True,
            'step_name': 'BigtableEnrich',
            'project': config.project_id,
            'instance_id': config.bigtable_instance_id,
            'table_id': config.bigtable_table_id,
            'app_profile_id': config.bigtable_app_profile_id,
            'row_key_field': 'member_number',
            'columns_to_fetch': ['member_number', 'first_name', 'last_name', 'email', 'status', 'updated_at'],
            'timeout': 10
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


def run_pipeline(config: PipelineConfig, pipeline_options: PipelineOptions):
    """Main pipeline execution"""
    logger.info(f"Starting pipeline - Term: {config.term_type}, Mode: {config.mode}")
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        if config.mode == 'streaming':
            build_streaming_pipeline(pipeline, config)
        else:
            build_batch_pipeline(pipeline, config)
    
    logger.info("Pipeline execution completed")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Unified MS Member Pipeline (Refactored)')
    
    # Configuration file argument
    parser.add_argument(
        '--config_file',
        help='Path to configuration YAML file (local or GCS)'
    )
    
    # Required arguments (can be overridden by config file)
    parser.add_argument('--project_id', help='GCP Project ID')
    parser.add_argument('--term_type',
                       choices=['short', 'mid', 'long'],
                       help='Term type: short (batch), mid (streaming), long (streaming)')
    
    # Optional arguments
    parser.add_argument('--env', default='staging',
                       choices=['staging', 'prod'],
                       help='Environment')
    parser.add_argument('--source_dataset', default='insight')
    parser.add_argument('--staging_dataset', default='insight_dev')
    parser.add_argument('--refined_dataset', default='insight_dev')
    
    # Bigtable arguments
    parser.add_argument('--bigtable_instance_id',
                       help='Bigtable instance ID (required for streaming)')
    parser.add_argument('--bigtable_table_id',
                       help='Bigtable table ID (required for streaming)')
    parser.add_argument('--bigtable_app_profile_id',
                       help='Bigtable app profile ID (optional)')
    
    # Streaming arguments
    parser.add_argument('--pubsub_topic',
                       help='Pub/Sub topic for streaming')
    parser.add_argument('--window_duration_hours', type=int, default=1,
                       help='Window duration in hours')
    parser.add_argument('--mapping_refresh_minutes', type=int, default=10,
                       help='Mapping cache refresh interval in minutes')
    
    # Security arguments
    parser.add_argument('--specific_sa',
                       help='Secret name for specific SA')
    parser.add_argument('--streaming_mode',
                       choices=['exactly_once', 'at_least_once'],
                       default='exactly_once',
                       help='Streaming semantics mode')
    
    # Pipeline runner arguments
    parser.add_argument('--runner', default='DirectRunner',
                       choices=['DirectRunner', 'DataflowRunner'])
    parser.add_argument('--region', default='asia-southeast1')
    parser.add_argument('--temp_location', help='GCS temp location')
    parser.add_argument('--staging_location', help='GCS staging location')
    parser.add_argument('--service_account_email',
                       help='Worker SA email (for Dataflow)')
    
    args, beam_args = parser.parse_known_args()
    
    # Load configuration
    if args.config_file:
        # Load from config file
        orchestrator = PipelineOrchestrator(config_path=args.config_file)
        config_dict = orchestrator.config
        
        # Override with command line arguments if provided
        if args.project_id:
            config_dict['gcp']['project_id'] = args.project_id
        if args.term_type:
            config_dict['pipeline']['term_type'] = args.term_type
        
        config = PipelineConfig.from_dict(config_dict)
    else:
        # Create config from command line arguments
        if not args.project_id or not args.term_type:
            parser.error('--project_id and --term_type required when not using config file')
        
        config = PipelineConfig.from_args(args)
    
    # Validate configuration
    issues = config.validate()
    if issues:
        for issue in issues:
            logger.error(f"Configuration error: {issue}")
        raise ValueError("Invalid configuration")
    
    # Build pipeline options
    if args.config_file and 'dataflow' in orchestrator.config:
        # Use config file for Dataflow options
        df_config = DataflowJobConfig.from_dict(orchestrator.config)
        pipeline_options = PipelineOptions(**df_config.to_pipeline_options())
    else:
        # Build from command line arguments
        pipeline_options = PipelineOptions(beam_args)
    
    pipeline_options.view_as(StandardOptions).runner = args.runner
    
    if args.runner == 'DataflowRunner':
        from apache_beam.options.pipeline_options import (
            GoogleCloudOptions, SetupOptions, WorkerOptions
        )
        
        pipeline_options.view_as(StandardOptions).streaming = (config.mode == 'streaming')
        
        gcp_options = pipeline_options.view_as(GoogleCloudOptions)
        gcp_options.project = config.project_id
        gcp_options.region = args.region or 'asia-southeast1'
        gcp_options.temp_location = args.temp_location
        gcp_options.staging_location = args.staging_location
        gcp_options.job_name = f"ms-member-{config.term_type}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        if args.service_account_email:
            gcp_options.service_account_email = args.service_account_email
        
        worker_options = pipeline_options.view_as(WorkerOptions)
        if config.term_type == 'short':
            worker_options.machine_type = 'n1-standard-2'
            worker_options.max_num_workers = 5
        else:
            worker_options.machine_type = 'n1-standard-4'
            worker_options.max_num_workers = 10
            
            if config.mode == 'streaming':
                gcp_options.enable_streaming_engine = True
        
        setup_options = pipeline_options.view_as(SetupOptions)
        setup_options.save_main_session = True
        
        if config.mode == 'streaming' and args.streaming_mode == 'at_least_once':
            beam_args.append('--dataflow_service_options=streaming_mode_at_least_once')
    
    # Run pipeline
    run_pipeline(config, pipeline_options)


if __name__ == '__main__':
    main()
