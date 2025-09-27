# dataflow_common/steps.py
"""Reusable pipeline steps for common operations"""

from typing import Dict, Any, Optional, List
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.transforms import window, trigger
from apache_beam.transforms.periodicsequence import PeriodicImpulse

from .core import PipelineStep
from .connectors import BigQueryConnector, PubSubConnector, BigtableConnector
from .transformers import (
    ColumnMapper, StreamingColumnMapper,BatchColumnMapper,
    MappingCacheLoader, DataQualityTransformer, NotificationParser,
    WindowedAggregator
)
# from .config import PipelineConfig
from .config import CommonPipelineConfig  # Changed from PipelineConfig

logger = logging.getLogger(__name__)


class ReadFromBigQueryStep(PipelineStep):
    """Step to read data from BigQuery"""
    
    def execute(self, pipeline: beam.Pipeline, 
                input_pcoll: Optional[beam.PCollection] = None) -> Optional[beam.PCollection]:
        
        logger.info(f"DEBUG ReadFromBigQueryStep: step_name={self.step_name}")
        logger.info(f"DEBUG ReadFromBigQueryStep: enabled={self.is_enabled()}")

        if not self.is_enabled():
            return None
        
        logger.info(f"DEBUG ReadFromBigQueryStep: config keys = {self.config.keys()}")
        connector = BigQueryConnector(
            project=self.config.get('project'),
            dataset=self.config.get('dataset'),
            credentials_path=self.config.get('credentials_path'),
            gcs_location=self.config.get('gcs_location','gs://t1-insight-audit-bucket/audit_log/dataflow/temp')
        )
        
        query = self.config.get('query')
        logger.info(f"DEBUG ReadFromBigQueryStep: query = {query[:200] if query else 'None'}...")
        if not query and self.config.get('src_table'):
            logger.info("DEBUG: Building query from src_table...")
            # Build query from table and partition
            src_project = self.config['src_project']  
            src_table = self.config['src_table']
            tgt_table = self.config['tgt_table']
            condition = self.config.get('partition_filter', f'timestamp > (SELECT MAX(timestamp) FROM `{tgt_table}`')
            # query = f"SELECT * FROM `{src_table}` WHERE {condition}"
            query = f"""
                SELECT * 
                EXCEPT(RN_PK)
                FROM (
                    SELECT *
                    -- json field is sensitivity
                    , ROW_NUMBER() OVER(PARTITION BY JSON_VALUE(profiles.memberId) ORDER BY TIMESTAMP DESC ) RN_PK
                    FROM `{src_table}` 
                    WHERE {condition}
                ) AS LAST_UPD
                WHERE RN_PK = 1 
                """
        logger.info("DEBUG: About to create pipeline read transform...")
        
        return (
            pipeline
            | f"Read_{self.step_name}" >> connector.read(
                query=query
                # method=self.config.get('method', 'DIRECT_READ')
            )
        )


class ReadFromPubSubStep(PipelineStep):
    """Step to read from Pub/Sub for streaming"""
    
    def execute(self, pipeline: beam.Pipeline,
                input_pcoll: Optional[beam.PCollection] = None) -> Optional[beam.PCollection]:
        
        if not self.is_enabled():
            return None
        
        connector = PubSubConnector(
            project=self.config.get('project'),
            credentials_path=self.config.get('credentials_path')
        )
        
        # Read from Pub/Sub
        messages = (
            pipeline
            | f"ReadPubSub_{self.step_name}" >> connector.read(
                topic=self.config.get('topic'),
                subscription=self.config.get('subscription'),
                with_attributes=self.config.get('with_attributes', False)
            )
        )
        
        # Parse notifications if configured
        if self.config.get('parse_notifications', True):
            messages = messages | f"ParseNotifications_{self.step_name}" >> beam.ParDo(NotificationParser())
        
        # Apply windowing if configured
        if self.config.get('window_duration_seconds'):
            messages = (
                messages
                | f"ApplyWindow_{self.step_name}" >> beam.WindowInto(
                    window.FixedWindows(self.config['window_duration_seconds']),
                    trigger=trigger.AfterWatermark(
                        early=trigger.AfterProcessingTime(
                            self.config.get('early_trigger_seconds', 60)
                        )
                    ),
                    accumulation_mode=trigger.AccumulationMode.DISCARDING
                )
            )
        
        return messages


class BigtableEnrichmentStep(PipelineStep):
    """Step to enrich data with Bigtable lookups"""
    
    def execute(self, pipeline: beam.Pipeline,
                input_pcoll: Optional[beam.PCollection] = None) -> Optional[beam.PCollection]:
        
        if not self.is_enabled() or not input_pcoll:
            return input_pcoll
        
        from apache_beam.transforms.enrichment import Enrichment
        
        connector = BigtableConnector(
            project=self.config['project'],
            instance_id=self.config['instance_id'],
            table_id=self.config['table_id'],
            app_profile_id=self.config.get('app_profile_id'),
            credentials_path=self.config.get('credentials_path')
        )
        
        handler = connector.create_enrichment_handler(
            row_key_field=self.config.get('row_key_field', 'member_id'),
            columns_to_fetch=self.config.get('columns_to_fetch')
        )
        
        def join_fn(original, enrichment):
            """Join function to merge original with enrichment data"""
            result = {**original}
            if enrichment:
                for key, value in enrichment.items():
                    if isinstance(value, dict):
                        result.update(value)
                    else:
                        result[key] = value
            result['_enrichment_timestamp'] = datetime.utcnow().isoformat()
            return result
        
        return (
            input_pcoll
            | f"Enrich_{self.step_name}" >> Enrichment(
                enrichment_handler=handler,
                join_fn=join_fn,
                timeout=self.config.get('timeout', 10)
            )
        )


# Also update the ColumnMappingStep to pass batch_size
class ColumnMappingStep(PipelineStep):
    """Step to apply column mapping transformation - NO HARD CODED VALUES"""
    
    def execute(self, pipeline: beam.Pipeline,
                input_pcoll: Optional[beam.PCollection] = None) -> Optional[beam.PCollection]:
        
        if not self.is_enabled() or not input_pcoll:
            return input_pcoll
        
        # Get all values from config - NO DEFAULTS
        min_batch_size = self.config.get('min_batch_size')
        max_batch_size = self.config.get('max_batch_size')
        enrichment_batch_size = self.config.get('enrichment_batch_size')
        
        # if self.config.get('batch_mode'):
        if self.config['mode'] == 'batch':
            mapping_side_input = self.config.get('mapping_side_input')
            if not mapping_side_input:
                raise ValueError("mapping_side_input required for batch mode")

            # Batch mode with enrichment
            return (
                input_pcoll
                | f"Batch_{self.step_name}" >> beam.BatchElements(
                    min_batch_size=min_batch_size,
                    max_batch_size=max_batch_size
                )
                # | f"EnrichAndMap_{self.step_name}" >> beam.FlatMap(
                #     EnrichAndMapColumns(
                #         config=CommonPipelineConfig.from_dict(self.config),
                #         target_table=self.config['target_table'],
                #         mapping_type=self.config['mapping_type'],
                #         batch_size=enrichment_batch_size
                #     ).process
                # )
                | f"BatchMap_{self.step_name}" >> beam.ParDo(
                    BatchColumnMapper(
                        self.config['target_table'],
                    ),
                    mapping_dict=mapping_side_input
                )
            )
        # elif self.config.get('streaming_mode'):
        elif self.config['mode'] == 'streaming':
            # Streaming mode with side input
            mapping_side_input = self.config.get('mapping_side_input')
            # ต้องไปอ่าน Mapping ตรงนี้ mapping_side_input
            if not mapping_side_input:
                raise ValueError("mapping_side_input required for streaming mode")
            
            return (
                input_pcoll
                | f"StreamMap_{self.step_name}" >> beam.ParDo(
                    StreamingColumnMapper(
                        self.config['target_table'],
                    ),
                    mapping_dict=mapping_side_input
                )
            )
        else:
            # Standard mode
            return (
                input_pcoll
                | f"Map_{self.step_name}" >> beam.ParDo(
                    ColumnMapper(
                        config=CommonPipelineConfig.from_dict(self.config),
                        target_table=self.config['target_table'],
                        mapping_type=self.config['mapping_type']
                    )
                )
            )

class DataQualityStep(PipelineStep):
    """Step for data quality validation"""
    
    def execute(self, pipeline: beam.Pipeline,
                input_pcoll: Optional[beam.PCollection] = None) -> Optional[beam.PCollection]:
        
        if not self.is_enabled() or not input_pcoll:
            return input_pcoll
        
        rules = self.config.get('rules', [])
        
        validated = (
            input_pcoll
            | f"Validate_{self.step_name}" >> beam.ParDo(
                DataQualityTransformer(rules=rules)
            )
        )
        
        # Split good and bad records if configured
        if self.config.get('split_output', False):
            good_records = (
                validated
                | f"FilterGood_{self.step_name}" >> beam.Filter(
                    lambda x: x.get('_dq_status') == 'PASSED'
                )
            )
            
            bad_records = (
                validated
                | f"FilterBad_{self.step_name}" >> beam.Filter(
                    lambda x: x.get('_dq_status') == 'FAILED'
                )
            )
            
            # Write bad records to error table if configured
            if self.config.get('error_table'):
                error_connector = BigQueryConnector(
                    project=self.config.get('project'),
                    dataset=self.config.get('dataset')
                )
                
                bad_records | f"WriteErrors_{self.step_name}" >> error_connector.write(
                    table=self.config['error_table'],
                    mode='WRITE_APPEND',
                    method=self.config.get('write_method', 'STORAGE_WRITE_API')
                )
            
            return good_records
        
        return validated


class WriteToBigQueryStep(PipelineStep):
    """Step to write data to BigQuery"""
    
    def execute(self, pipeline: beam.Pipeline,
                input_pcoll: Optional[beam.PCollection] = None) -> Optional[beam.PCollection]:
        
        if not self.is_enabled() or not input_pcoll:
            return input_pcoll
        
        connector = BigQueryConnector(
            project=self.config.get('project'),
            dataset=self.config.get('dataset'),
            credentials_path=self.config.get('credentials_path')
        )

        # input_pcoll | f"Write_{self.step_name}" >> connector.write(
        #     table=self.config['table'],
        #     mode=self.config.get('mode', 'WRITE_APPEND'),
        #     method=self.config.get('method', 'STORAGE_WRITE_API'),
        #     schema=self.config.get('schema', 'SCHEMA_AUTODETECT'),
        #     streaming_mode=self.config.get('streaming_mode')
        # )

        # Clean metadata fields if configured (but not for CDC)
        if self.config.get('remove_metadata', True) and not self.config.get('use_cdc'):
            input_pcoll = (
                input_pcoll
                | f"RemoveMetadata_{self.step_name}" >> beam.Map(
                    lambda x: {k: v for k, v in x.items() 
                              if not k.startswith('_')}
                )
            )
        # TESTCASE SCENARIO 1 : SHORT TERM WRITE_TRUNCATE  : mode WRITE_TRUNCATE , method = STORAGE_WRITE_API 
        #                       WRITE_TRUNCATE TO STG_PERSONAS  > MERGE TO STG_MS_PERSONAS
        #                                                       > MERGE TO STG_MS_MEMBER
        # TESTCASE SCENARIO 2 : MID/LONG TERM CDC : USING CDC : mode WRITE_APPEND , method = STORAGE_WRITE_API , use_cdc = True
        #                       UPSERT WITH WRITE_APPEND USE_CDC TO STG_MS_MEMBER AND MS_PERSONAS
        # TESTCASE SCENARIO 3 : MID/LONG TERM NO CDC : mode WRITE_APPEND , method = STORAGE_WRITE_API , use_cdc = False
        #                       FOR THIS CASE NOT USE IN MEMBER/PERSONAS TABLE BECAUSE NEED CDC
        # Check if using CDC
        # if self.config.get('query_bq') :
        #     # (
        #     #     input_pcoll
        #     #     | f"Read_{self.step_name}" >> connector.query(query=self.config.get('query_bq'))
                
        #     # )
        #     # from apache_beam.io.gcp.bigquery import BigQueryInsertJobOperator

        #     _ = (
        #         pipeline
        #         | beam.Create([1])
        #         | "ExecuteMergeQuery" >> beam.Map(
        #             lambda _: connector.client.query(self.config['query_bq']).result()
        #         )
        #     )


        if self.config.get('use_cdc'):
            input_pcoll | f"WriteCDC_{self.step_name}" >> connector.write_cdc(
                table=self.config['table'],
                primary_key=self.config.get('primary_key', ['member_id']),
                schema=self.config.get('schema', 'SCHEMA_AUTODETECT')
            )
        else:
            input_pcoll | f"Write_{self.step_name}" >> connector.write(
                table=self.config['table'],
                mode=self.config.get('mode', 'WRITE_APPEND'),
                method=self.config.get('method', 'STORAGE_WRITE_API'),
                schema=self.config.get('schema', 'SCHEMA_AUTODETECT'),
                streaming_mode=self.config.get('streaming_mode'),
                use_cdc=False,
                primary_key=None  # ไม่ใช้ primary_key สำหรับ normal write
            )

        return input_pcoll


class AuditLoggingStep(PipelineStep):
    """Step for audit logging"""
    
    def execute(self, pipeline: beam.Pipeline,
                input_pcoll: Optional[beam.PCollection] = None) -> Optional[beam.PCollection]:
        
        if not self.is_enabled() or not input_pcoll:
            return input_pcoll
        
        if self.config.get('aggregate_windows'):
            # Aggregate audit data within windows
            audit_data = (
                input_pcoll
                | f"PrepareAudit_{self.step_name}" >> beam.Map(
                    lambda x: {
                        'member_number': x.get('member_number'),
                        'timestamp': datetime.utcnow().isoformat(),
                        '_dq_status': x.get('_dq_status', 'UNKNOWN')
                    }
                )
                | f"ApplyAuditWindow_{self.step_name}" >> beam.WindowInto(
                    window.FixedWindows(self.config.get('window_duration_seconds', 3600)),
                    trigger=trigger.AfterWatermark(),
                    accumulation_mode=trigger.AccumulationMode.DISCARDING
                )
                | f"AggregateAudit_{self.step_name}" >> beam.CombineGlobally(
                    WindowedAggregator()
                ).without_defaults()
                | f"FormatAuditLog_{self.step_name}" >> beam.Map(
                    lambda stats: {
                        'job_time': datetime.utcnow().isoformat(),
                        'pipeline': self.config.get('pipeline_name', 'unknown'),
                        'step': self.step_name,
                        'mode': self.config.get('mode', 'batch'),
                        'records_processed': stats['record_count'],
                        'unique_count': stats['unique_count'],
                        'error_count': stats['error_count'],
                        'success_rate': stats['success_rate'],
                        'window_start': stats['window_start'],
                        'window_end': stats['window_end'],
                        'duration_seconds': stats['duration_seconds'],
                        'status': 'COMPLETED'
                    }
                )
            )
        else:
            # Simple audit logging
            audit_data = (
                input_pcoll
                | f"CreateAudit_{self.step_name}" >> beam.Map(
                    lambda x: {
                        'job_time': datetime.utcnow().isoformat(),
                        'pipeline': self.config.get('pipeline_name', 'unknown'),
                        'step': self.step_name,
                        'record_id': x.get('member_number'),
                        'status': x.get('_dq_status', 'PROCESSED')
                    }
                )
            )
        
        # Write audit logs
        if self.config.get('audit_table'):
            audit_connector = BigQueryConnector(
                project=self.config.get('project'),
                dataset=self.config.get('dataset')
            )
            if self.config.get('runner') == 'DirectRunner':
                audit_data | f"WriteAudit_{self.step_name}" >> audit_connector.write(
                    table=self.config['audit_table'],
                    mode='WRITE_APPEND',
                    method='STREAMING_INSERTS',  # Use batch for audit logs
                    schema={
                        'fields': [
                            {"name": "job_time","mode": "NULLABLE","type": "STRING"},
                            {"name": "pipeline","mode": "NULLABLE","type": "STRING"},
                            {"name": "term_type","mode": "NULLABLE","type": "STRING"},
                            {"name": "mode","mode": "NULLABLE","type": "STRING"},
                            {"name": "environment","mode": "NULLABLE","type": "STRING"},
                            {"name": "records_processed","mode": "NULLABLE","type": "STRING"},
                            {"name": "unique_members","mode": "NULLABLE","type": "STRING"},
                            {"name": "window_start","mode": "NULLABLE","type": "STRING"},
                            {"name": "window_end","mode": "NULLABLE","type": "STRING"},
                            {"name": "status","mode": "NULLABLE","type": "STRING"}
                            ]
                    },
                    custom_gcs_temp_location=self.config.get('temp_location')  # เพิ่มบรรทัดนี้
                )
            else:
                audit_data | f"WriteAudit_{self.step_name}" >> audit_connector.write(
                    table=self.config['audit_table'],
                    mode='WRITE_APPEND',
                    method='FILE_LOADS',  # Use batch for audit logs
                    schema='SCHEMA_AUTODETECT',
                    custom_gcs_temp_location=self.config.get('temp_location')  # เพิ่มบรรทัดนี้

                )
        
        return input_pcoll


class CreateMappingSideInput(PipelineStep):
    """Step to create periodically refreshing mapping side input"""
    
    def execute(self, pipeline: beam.Pipeline,
                input_pcoll: Optional[beam.PCollection] = None) -> Optional[beam.PCollection]:
        
        if not self.is_enabled():
            return None
        
        config = CommonPipelineConfig.from_dict(self.config)  # Changed from PipelineConfig
        
        # Generate periodic impulses for refreshing mapping
        mapping_refresh = (
            pipeline
            | f"RefreshTrigger_{self.step_name}" >> PeriodicImpulse(
                start_timestamp=0,
                stop_timestamp=float('inf'),
                fire_interval=self.config.get('refresh_interval_seconds', 600)
            )
            | f"LoadMapping_{self.step_name}" >> beam.ParDo(
                MappingCacheLoader(config)
            )
        )
        
        return beam.pvalue.AsSingleton(mapping_refresh)


class BranchingStep(PipelineStep):
    """Step to branch pipeline based on conditions"""
    
    def execute(self, pipeline: beam.Pipeline,
                input_pcoll: Optional[beam.PCollection] = None) -> Optional[beam.PCollection]:
        
        if not self.is_enabled() or not input_pcoll:
            return input_pcoll
        
        branches = {}
        
        for branch_config in self.config.get('branches', []):
            name = branch_config['name']
            condition = branch_config.get('condition')
            
            if condition:
                # Apply filter condition
                branches[name] = (
                    input_pcoll
                    | f"Filter_{name}" >> beam.Filter(
                        lambda x, cond=condition: eval(cond, {'x': x})
                    )
                )
            else:
                # No condition means this is the default branch
                branches[name] = input_pcoll
        
        # Store branches in config for later use
        self.config['output_branches'] = branches
        
        # Return main branch or input if no main branch specified
        main_branch = self.config.get('main_branch', 'main')
        return branches.get(main_branch, input_pcoll)

class DataValidator(beam.DoFn):
    """Validate incoming data and split into valid/invalid streams"""
    
    def __init__(self, validation_rules: List[Dict[str, Any]] = None):
        self.validation_rules = validation_rules or []
        self.valid_counter = beam.metrics.Metrics.counter('data_validator', 'valid')
        self.invalid_counter = beam.metrics.Metrics.counter('data_validator', 'invalid')
        
    def process(self, element):
        """Validate element and route to appropriate output"""
        errors = []
        
        # Check for required fields
        for rule in self.validation_rules:
            if rule['type'] == 'required':
                field = rule['field']
                if not element.get(field):
                    errors.append(f"Missing required field: {field}")
                    
            elif rule['type'] == 'not_null':
                field = rule['field']
                if element.get(field) is None:
                    errors.append(f"Null value in field: {field}")
                    
            elif rule['type'] == 'regex':
                field = rule['field']
                pattern = rule['pattern']
                import re
                if element.get(field) and not re.match(pattern, str(element[field])):
                    errors.append(f"Invalid format in field {field}")
        
        if errors:
            self.invalid_counter.inc()
            element['_validation_errors'] = errors
            element['_validation_timestamp'] = datetime.utcnow().isoformat()
            yield beam.pvalue.TaggedOutput('invalid', element)
        else:
            self.valid_counter.inc()
            element['_validation_status'] = 'valid'
            yield beam.pvalue.TaggedOutput('valid', element)