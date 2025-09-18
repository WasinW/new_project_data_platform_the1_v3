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
    ColumnMapper, StreamingColumnMapper, EnrichAndMapColumns,
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
        
        if not self.is_enabled():
            return None
        
        connector = BigQueryConnector(
            project=self.config.get('project'),
            dataset=self.config.get('dataset'),
            credentials_path=self.config.get('credentials_path')
        )
        
        query = self.config.get('query')
        if not query and self.config.get('src_table'):
            # Build query from table and partition
            src_table = self.config['src_table']
            tgt_table = self.config['tgt_table']
            condition = self.config.get('partition_filter', f'timestamp > (SELECT MAX(timestamp) FROM `{tgt_table}`')
            # query = f"SELECT * FROM `{src_table}` WHERE {condition}"
            query = f"SELECT * FROM `{src_table}` WHERE {condition} )"
        
        return (
            pipeline
            | f"Read_{self.step_name}" >> connector.read(
                query=query,
                method=self.config.get('method', 'DIRECT_READ')
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
            row_key_field=self.config.get('row_key_field', 'member_number'),
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
        
        if self.config.get('batch_mode'):
            if not min_batch_size or not max_batch_size or not enrichment_batch_size:
                raise ValueError("min_batch_size, max_batch_size, and enrichment_batch_size required for batch mode")
            
            # Batch mode with enrichment
            return (
                input_pcoll
                | f"Batch_{self.step_name}" >> beam.BatchElements(
                    min_batch_size=min_batch_size,
                    max_batch_size=max_batch_size
                )
                | f"EnrichAndMap_{self.step_name}" >> beam.FlatMap(
                    EnrichAndMapColumns(
                        config=CommonPipelineConfig.from_dict(self.config),
                        target_table=self.config['target_table'],
                        mapping_type=self.config['mapping_type'],
                        batch_size=enrichment_batch_size
                    ).process
                )
            )
        elif self.config.get('streaming_mode'):
            # Streaming mode with side input
            mapping_side_input = self.config.get('mapping_side_input')
            if not mapping_side_input:
                raise ValueError("mapping_side_input required for streaming mode")
            
            return (
                input_pcoll
                | f"StreamMap_{self.step_name}" >> beam.ParDo(
                    StreamingColumnMapper(
                        self.config['target_table'],
                        self.config['mapping_type']
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
        
        rules = self.config.get('rules', [
            {'type': 'required', 'field': 'member_number'},
            {'type': 'not_null', 'field': 'member_number'}
        ])
        
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
        
        # Clean metadata fields if configured
        if self.config.get('remove_metadata', True):
            input_pcoll = (
                input_pcoll
                | f"RemoveMetadata_{self.step_name}" >> beam.Map(
                    lambda x: {k: v for k, v in x.items() 
                              if not k.startswith('_')}
                )
            )
        
        input_pcoll | f"Write_{self.step_name}" >> connector.write(
            table=self.config['table'],
            mode=self.config.get('mode', 'WRITE_APPEND'),
            method=self.config.get('method', 'STORAGE_WRITE_API'),
            schema=self.config.get('schema', 'SCHEMA_AUTODETECT'),
            streaming_mode=self.config.get('streaming_mode')
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
            
            audit_data | f"WriteAudit_{self.step_name}" >> audit_connector.write(
                table=self.config['audit_table'],
                mode='WRITE_APPEND',
                method='FILE_LOADS',  # Use batch for audit logs
                schema='SCHEMA_AUTODETECT'
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
