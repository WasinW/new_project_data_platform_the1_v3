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
from apache_beam import combiners

from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from typing import Optional, Dict, Any, List
# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add parent directory to path for imports
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# dataflow_common_path = os.path.join(project_root, 'packages', 'dataflow-common')
# sys.path.insert(0, dataflow_common_path)


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
        #  1. get mapping from mapping table >> dict
        #  2.1. ingest personas into stg_personas (3k records and map columns 250 fields like stg_ms_personas) >> sent data and dict to mapping step
        #  2.2. gen query merge (get mapping and gen query) >> sent dict to query
        #  3. merge stg_ms_personas (merge 3k records and map columns 250 fields like stg_ms_personas)
        #  4. merge stg_ms_member (merge 3k records and map columns 250 fields like stg_ms_member)

    try:
        # เพิ่ม debug logs
        logger.info("=== DEBUG: Starting batch pipeline build ===")
        logger.info(f"DEBUG: source_project = {config.get('source_project')}")
        logger.info(f"DEBUG: source_dataset = {config.get('source_dataset')}")
        logger.info(f"DEBUG: source_table = {config.get('source_table')}")
        logger.info(f"DEBUG: project_id = {config.get('project_id')}")
        logger.info(f"DEBUG: staging_dataset = {config.get('staging_dataset')}")
        logger.info(f"DEBUG: stg_source_table = {config.get('stg_source_table')}")
        
        # Step 1.1: Read from BigQuery with source values
        logger.info("DEBUG: Creating ReadFromBigQueryStep for source data...")

        logger.info("=== DEBUG: Starting batch pipeline build ===")
        logger.info(f"DEBUG: source_project = {config.get('source_project')}")
        logger.info(f"DEBUG: source_dataset = {config.get('source_dataset')}")
        logger.info(f"DEBUG: source_table = {config.get('source_table')}")
        logger.info(f"DEBUG: project_id = {config.get('project_id')}")
        logger.info(f"DEBUG: staging_dataset = {config.get('staging_dataset')}")
        logger.info(f"DEBUG: stg_source_table = {config.get('stg_source_table')}")
        
        # Step 1.1: Read from BigQuery with source values
        logger.info("DEBUG: Creating ReadFromBigQueryStep for source data...")
        
        # แยก query ออกมาเพื่อ debug
        source_project = config.get('source_project')
        source_dataset = config.get('source_dataset')
        source_table = config.get('source_table')
        project_id = config.get('project_id')
        staging_dataset = config.get('staging_dataset')
        stg_source_table = config.get('stg_source_table')
        mapping_table = config.get('mapping_table')
        read_method = config.get('read_method', 'DIRECT_READ')


        source_table_full = f"{source_project}.{source_dataset}.{source_table}"
        target_table_full = f"{project_id}.{staging_dataset}.{stg_source_table}"

        logger.info(f"DEBUG: source_table_full = {source_table_full}")
        logger.info(f"DEBUG: target_table_full = {target_table_full}")
        query = f"""
            SELECT * EXCEPT(RN_PK)
            FROM (
                SELECT *
                -- json field is sensitivity
                , ROW_NUMBER() OVER(PARTITION BY JSON_VALUE(profiles, '$.memberId') ORDER BY TIMESTAMP DESC) RN_PK
                FROM `{source_table_full}`
                -- WHERE timestamp > (SELECT COALESCE(MAX(updated_date), TIMESTAMP('1999-12-31')) FROM `{target_table_full}`)
                LIMIT 1000
            ) AS LAST_UPD
            WHERE RN_PK = 1 
        """
        logger.info(f"DEBUG: Generated query = {query[:500]}...")  # Show first 500 chars
        read_step = ReadFromBigQueryStep({
            'enabled': True,
            'step_name': 'ReadSource',
            'project': project_id,
            'src_project': source_project,
            'dataset': source_dataset,
            'src_table': source_table_full,
            'tgt_table': target_table_full,
            # 'tgt_table': f"{config.get('project_id')}.{config.get('staging_dataset')}.{config.get('stg_ongoing_source_table')}",
            'query': query,
            # 'partition_filter': config.get('partition_filter'),
            'method': read_method
        })
        logger.info("DEBUG: ReadFromBigQueryStep created successfully")
        
        # Step 1.2: Read mapping
        logger.info("DEBUG: Creating ReadFromBigQueryStep for mapping data...")
        
        mapping_query = f"""
            SELECT *
            FROM `{project_id}.{staging_dataset}.{mapping_table}`
            WHERE TRUE
                AND COALESCE(UPDATED_DATE, "1999-12-31") = (
                    SELECT COALESCE(MAX(updated_date), '1999-12-31')
                    FROM `{project_id}.{staging_dataset}.{mapping_table}`
                )
        """
        
        logger.info(f"DEBUG: Mapping query = {mapping_query[:500]}...")
        

        # Step 1.2: Read from BigQuery with config values
        read_mapping_step = ReadFromBigQueryStep({
            'enabled': True,
            'step_name': 'ReadMapping',
            'project': project_id,
            'src_project': project_id,
            'dataset': staging_dataset,
            # 'src_table': source_table_full,
            # 'tgt_table': target_table_full,
            'query': mapping_query,
            # 'query': f"""
            # SELECT *
            # FROM `{config.get('project_id')}.{config.get('staging_dataset')}.{config.get('mapping_table')}`
            # WHERE TRUE
            #     AND COALESCE(UPDATED_DATE, "1999-12-31") = (
            #         SELECT MAX(COALESCE(UPDATED_DATE, "1999-12-31"))
            #         FROM `{config.get('project_id')}.{config.get('staging_dataset')}.{config.get('mapping_table')}`
            #     )
            # """,
            'method': read_method
        })
        
        logger.info("DEBUG: About to execute source_data read...")
        source_data = read_step.execute(pipeline)
        logger.info("DEBUG: source_data read executed")
        _ = (source_data
            | 'Count_Source' >> combiners.Count.Globally()
            | 'Log_Source_Count' >> beam.Map(lambda c: logger.info(f"[METRIC] source_count={c}")))

        logger.info("DEBUG: About to execute mapping_data read...")
        mapping_data = read_mapping_step.execute(pipeline)
        logger.info("DEBUG: mapping_data read executed")
        _ = (mapping_data
            | 'Count_Mapping' >> combiners.Count.Globally()
            | 'Log_Mapping_Count' >> beam.Map(lambda c: logger.info(f"[METRIC] mapping_count={c}")))

        # Convert mapping to side input
        logger.info("DEBUG: Converting mapping to side input...")
        mapping_list = beam.pvalue.AsList(mapping_data)
        logger.info("DEBUG: Mapping side input created")

        # Step 2: Data Quality Validation with config rules
        logger.info("DEBUG: Starting Data Quality Validation...")
        validation_rules = config.get('validation_rules')
        logger.info(f"DEBUG: validation_rules = {validation_rules}")
        
        if isinstance(validation_rules, str):
            try:
                validation_rules = json.loads(validation_rules)
                logger.info(f"DEBUG: Parsed validation_rules = {validation_rules}")
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse validation_rules JSON: {validation_rules}, error: {e}")
                validation_rules = []
        
        try:
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
            logger.info("DEBUG: DataQualityStep created successfully")
        except Exception as e:
            logger.error(f"Error creating DataQualityStep: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

        try:
            validated_data = dq_step.execute(pipeline, source_data)
            logger.info("DEBUG: DataQualityStep executed successfully")
            _ = (validated_data
                | 'Count_Validated' >> combiners.Count.Globally()
                | 'Log_Validated_Count' >> beam.Map(lambda c: logger.info(f"[METRIC] validated_count={c}")))

        except Exception as e:
            logger.error(f"Error executing DataQualityStep: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
        
        # mapping field
        # Step 3: Mapping Field
        logger.info("DEBUG: Starting ColumnMappingStep...")
        try:
            mapping_step = ColumnMappingStep({
                'enabled': True,
                'step_name': 'MappingStep',
                'mode': 'batch',
                'target_table': config.get('stg_origin_table'),
                # 'mapping_type': 'source_to_origin',
                'mapping_side_input': mapping_list,
                # เพิ่ม parameters ที่อาจจำเป็น
                'min_batch_size': config.get('min_batch_size'),
                'max_batch_size': config.get('max_batch_size'),
                'enrichment_batch_size': config.get('enrichment_batch_size')
            })
            logger.info("DEBUG: ColumnMappingStep created successfully")
        except Exception as e:
            logger.error(f"Error creating ColumnMappingStep: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

        try:
            mapping_personas_data = mapping_step.execute(pipeline, validated_data)
            logger.info("DEBUG: ColumnMappingStep executed successfully")
            _ = (mapping_personas_data
                | 'Count_Before_Write' >> combiners.Count.Globally()
                | 'Log_Before_Write_Count' >> beam.Map(lambda c: logger.info(f"[METRIC] rows_before_write={c}")))
        except Exception as e:
            logger.error(f"Error executing ColumnMappingStep: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
        
        # Step 4: Write to Mapping stg_personas with WRITE_TRUNCATE for short term
        write_mapping_personas_step = WriteToBigQueryStep({
            'enabled': True,
            'step_name': 'WriteMappingPersonas',
            'project': config.get('project_id'),
            'dataset': config.get('staging_dataset'),
            # 'table': f"{config.get('project_id')}.{config.get('staging_dataset')}.{config.get('stg_ongoing_source_table')}",
            'table': f"{config.get('stg_ongoing_source_table')}",
            # 'method': config.get('write_method', 'FILE_LOADS'),
            'method': config.get('write_method', 'FILE_LOADS'),  # FILE_LOADS for batch
            # 'mode': config.get('bq_write_disposition', 'WRITE_TRUNCATE'), # WRITE_APPEND , WRITE_TRUNCATE
            'mode': 'WRITE_TRUNCATE', 
            'create_disposition': config.get('bq_create_disposition', 'CREATE_IF_NEEDED'),
            'priority': config.get('bq_priority', 'INTERACTIVE'),
            'remove_metadata': True,
            'temp_location': config.get('temp_location')  # เพิ่มบรรทัดนี้

        })
        write_mapping_personas_step.execute(pipeline, mapping_personas_data)

        # Step 5: Generate and execute MERGE queries
        # from dataflow_common.transformers import MergeQueryGenerator, MergeQueryExecutor

        merge_queries = (
                    pipeline
                    | 'CreateTrigger' >> beam.Create([1])  # Single trigger element
                    | 'GenerateMergeQueries' >> beam.ParDo(
                        MergeQueryGenerator(),
                        mapping_list,
                        config.to_dict()
                    )
                )
                
        # query_merge_ms_personas = generate_merge_query(
        #     mapping_records=list(mapping_data | "CollectMapping" >> beam.combiners.ToList()),
        #     column_condition='RECONCILE_RETRIEVED'
        # )
        # query_merge_ms_member = generate_merge_query(
        #     mapping_records=list(mapping_data | "CollectMapping" >> beam.combiners.ToList()),
        #     column_condition='RECONCILE_CONFIRMED'
        # )

        # write_member_step = WriteToBigQueryStep({
        #     'enabled': True,
        #     'step_name': 'WriteMember',
        #     'project': config.get('project_id'),
        #     'dataset': config.get('staging_dataset'),
        #     'table': config.get('stg_origin_table'),
        #     'query_bq': query_merge_ms_member,
        # })
        # write_personas_step = WriteToBigQueryStep({
        #     'enabled': True,
        #     'step_name': 'WritePersonas',
        #     'project': config.get('project_id'),
        #     'dataset': config.get('staging_dataset'),
        #     'table': config.get('stg_origin_table'),
        #     'query_bq': query_merge_ms_personas,
        # })

        # write_member_step.execute(pipeline, write_mapping_personas_step)
        # write_personas_step.execute(pipeline, write_mapping_personas_step)
        # Execute merge queries via connector
        merge_results = (
            merge_queries
            | 'ExecuteMergeQueries' >> beam.ParDo(
                MergeQueryExecutor(
                    project_id=config.get('project_id'),
                    dataset=config.get('staging_dataset')
                )
            )
        )
        
        # Optional: Log merge results
        _ = (
            merge_results
            | 'LogMergeResults' >> beam.Map(
                lambda x: logger.info(f"Merge execution results: {x}")
            )
        )
        
        # Step 6: Audit Logging
        if config.get('audit_enabled', True):
            # # สามารถใช้ merge_results ในการ audit ด้วย
            # audit_data = (
            #     merge_results
            #     | 'PrepareAuditData' >> beam.Map(
            #         lambda x: {
            #             'job_time': x['timestamp'],
            #             'pipeline': 'ms_member_unified',
            #             'step': 'merge_execution',
            #             'personas_status': x['personas_result']['status'] if x.get('personas_result') else 'skipped',
            #             'member_status': x['member_result']['status'] if x.get('member_result') else 'skipped',
            #             'overall_status': x['overall_status'],
            #             'mode': 'batch'
            #         }
            #     )
            # )
            
            # # Write audit log
            # audit_connector = BigQueryConnector(
            #     project=config.get('project_id'),
            #     dataset=config.get('staging_dataset')
            # )
            
            # audit_data | 'WriteAuditLog' >> audit_connector.write(
            #     table=config.get('audit_table'),
            #     mode='WRITE_APPEND',
            #     method='FILE_LOADS',
            #     schema='SCHEMA_AUTODETECT'
            # )

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
            
            # query get data from bigtable
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
            # mapping field for personas to origin  
            member_mapping_step = ColumnMappingStep({
                'enabled': True,
                'step_name': 'MapToMember',
                'streaming_mode': True,
                'target_table': config.get('stg_origin_table'),
                # 'mapping_type': 'source_to_origin',
                'mapping_side_input': mapping_side_input
            })
            
            member_data = member_mapping_step.execute(pipeline, validated_data)
            
            
            # Format for CDC upsert
            # from dataflow_common.transformers import CDCUpsertFormatter
            
            cdc_formatted_data = (
                member_data
                | 'FormatForCDC' >> beam.ParDo(CDCUpsertFormatter())
            )
            
            # Write to stg_ms_member with CDC for upserts
            write_member_step = WriteToBigQueryStep({
                'enabled': True,
                'step_name': 'WriteMemberCDC',
                'project': config.get('project_id'),
                'dataset': config.get('staging_dataset'),
                'table': config.get('stg_origin_table'),
                'mode': 'WRITE_APPEND',  # CDC requires WRITE_APPEND
                'method': config.get('write_method', 'STORAGE_WRITE_API'),
                'use_cdc': True,
                'primary_key': ['member_number'],  # Specify primary key
                'streaming_mode': config.get('streaming_mode', 'at_least_once'),
                'remove_metadata': True
            })
            
            write_member_step.execute(pipeline, cdc_formatted_data)
            
            # Also write to refined with CDC
            refined_data = (
                validated_data
                | 'AddRefinedMetadata' >> beam.Map(
                    lambda x: {**x, 'ingested_at': datetime.utcnow().isoformat()}
                )
                | 'FormatRefinedForCDC' >> beam.ParDo(CDCUpsertFormatter())
                | 'AuditRefinedWrite' >> beam.ParDo(WindowedAuditLogger('refined_write'))
            )
            
            write_refined_step = WriteToBigQueryStep({
                'enabled': True,
                # 'step_name': 'WriteRefined',
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
            
        elif config.get('term_type') == 'long':
            # Write only to refined
            # from dataflow_common.transformers import CDCUpsertFormatter

            refined_data = (
                validated_data
                | 'AddRefinedMetadataLong' >> beam.Map(
                    lambda x: {**x, 'ingested_at': datetime.utcnow().isoformat()}
                )
                | 'FormatLongForCDC' >> beam.ParDo(CDCUpsertFormatter())
                | 'AuditRefinedWriteLong' >> beam.ParDo(WindowedAuditLogger('refined_write_long'))
            )
            
            write_refined_step = WriteToBigQueryStep({
                'enabled': True,
                'step_name': 'WriteRefinedLongCDC',
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
        
        # Step 5: Windowed Audit Logging
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
    parser.add_argument('--source_project', required=True, help='Source project id')
    parser.add_argument('--stg_ongoing_source_table', required=True, help='Staging ongoing source table (personas) records from source_table')
    parser.add_argument('--stg_source_table', required=True, help='Staging source table (personas) all records')
    parser.add_argument('--stg_origin_table', required=True, help='Staging origin table (member) all records')
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
        gcp_options.project = config.get('project_id')
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
            gcp_options.impersonate_service_account = args.service_account_email
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