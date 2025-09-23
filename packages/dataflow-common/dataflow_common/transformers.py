# dataflow_common/transformers.py
"""Enhanced transformation utilities for pipeline processing"""

from typing import Dict, Any, List, Optional
import logging
import hashlib
import json
from datetime import datetime
import apache_beam as beam
from google.cloud import bigquery
from .core import Transformer
from .config import CommonPipelineConfig  # Changed from PipelineConfig

logger = logging.getLogger(__name__)


class MappingLoader:
    """Load and manage column mappings from BigQuery"""
    
    @staticmethod
    def load_mapping(config: CommonPipelineConfig,  # Changed from PipelineConfig
                    client: Optional[bigquery.Client] = None) -> Dict[str, Dict[str, str]]:
        """Load column mapping from BigQuery mapping_reconcile table"""
        try:
            if not client:
                client = bigquery.Client(project=config.project_id)
            
            # Get values from config - NO DEFAULTS
            staging_dataset = config.get('staging_dataset')
            mapping_table = config.get('mapping_table')
            
            if not staging_dataset or not mapping_table:
                raise ValueError("staging_dataset and mapping_table are required in config")
            
            query = f"""
            SELECT 
                RECONCILE_COLUMN_NAME,
                PERSONAS_MAPPING_COLUMN_NAME,
                RECONCILE_RETRIEVED,
                RECONCILE_CONFIRMED,
                UPDATED_DATE
            FROM `{config.get_full_table_id(staging_dataset, mapping_table)}`
            WHERE TRUE
                AND COALESCE(UPDATED_DATE, "1999-12-31") = (
                    SELECT MAX(COALESCE(UPDATED_DATE, "1999-12-31"))
                    FROM `{config.get_full_table_id(staging_dataset, mapping_table)}`
                )
            """
            
            mapping = {
                # 'source_to_ongoing': {},
                # 'source_to_origin': {}
            }
            
            query_job = client.query(query)
            
            # for row in query_job:
            #     tech_col = row['PERSONAS_MAPPING_COLUMN_NAME'] if row['PERSONAS_MAPPING_COLUMN_NAME'] else row['RECONCILE_COLUMN_NAME']
            #     data_col = row['RECONCILE_COLUMN_NAME']
                
            #     # FOR NOW THIS CASE FOR MID TERM ONLY : USING source_to_origin ONLY
            #     # For ongoing table (reconciliation)
            #     # if row['RECONCILE_RETRIEVED'] == 'Y':
            #     #     mapping['source_to_ongoing'][data_col] = tech_col
            #     # else:
            #     #     mapping['source_to_ongoing'][data_col] = data_col
                
            #     # For origin table (confirmed columns)
            #     if row['RECONCILE_CONFIRMED'] == 'Y':
            #         mapping['source_to_origin'][data_col] = tech_col
            #     else:
            #         mapping['source_to_origin'][data_col] = data_col
            
            # Add essential columns
            # for m in mapping.values():
            #     if 'member_number' not in m:
            #         m['member_id'] = 'member_number'
            #         # m['member_number'] = 'member_number'
            
            logger.info(f"Loaded mapping - Ongoing: {len(mapping['source_to_ongoing'])}, Origin: {len(mapping['source_to_origin'])}")
            return query_job.result()
            
        except Exception as e:
            logger.error(f"Error loading mapping: {str(e)}")
            # Return default mapping if error
            return {}


class ColumnMapper(beam.DoFn):
    """Maps columns based on configuration"""
    
    def __init__(self, config: CommonPipelineConfig, target_table: str, mapping_type: str):  # Changed
        """
        Args:
            config: Pipeline configuration
            target_table: Target table name
            mapping_type: 'source_to_ongoing' or 'source_to_origin'
        """
        self.config = config
        self.target_table = target_table
        self.mapping_type = mapping_type
        self.column_mapping = None
        
    def setup(self):
        """Load mapping once when DoFn starts"""
        all_mappings = MappingLoader.load_mapping(self.config)
        self.column_mapping = all_mappings.get(self.mapping_type, {})
        
    def process(self, element):
        """Map source columns to target format"""
        mapped_record = {}
        
        # Apply column mapping
        for source_col, target_col in self.column_mapping.items():
            if target_col in element and element[target_col] is not None:
                mapped_record[source_col] = element[target_col]
            else:
                mapped_record[source_col] = None
        
        # Add metadata
        mapped_record['_metadata'] = {
            'ingested_at': datetime.utcnow().isoformat(),
            'processed_at': datetime.utcnow().isoformat(),
            'source_table': self.config.get('source_table'),
            'target_table': self.target_table,
            'pipeline_version': '1.0.0'
        }
        
        yield mapped_record

class BatchColumnMapper(beam.DoFn):
    """Column mapper for batch processing with cached mapping"""

    def __init__(self, target_table: str):
        self.target_table = target_table
        
    def process(self, element, mapping_dict):
        """Map columns using cached mapping from side input"""
        column_mapping = mapping_dict
        
        mapped_record = {}
        
        for rec_col_nm, psn_map_col_nm, reconcile_sts, confirmed_sts, updated_date in column_mapping.items():
            psn_map_col_nm = psn_map_col_nm.split('.')[1] if '.' in psn_map_col_nm else psn_map_col_nm

            if psn_map_col_nm in element and reconcile_sts :
                mapped_record[rec_col_nm] = element[psn_map_col_nm]
            else:
                mapped_record[rec_col_nm] = None
        
        # Add metadata
        mapped_record['_metadata'] = {
            'ingested_at': datetime.utcnow().isoformat(),
            'processed_at': datetime.utcnow().isoformat(),
            'target_table': self.target_table,
            'stream_timestamp': element.get('_timestamp', datetime.utcnow().isoformat())
        }
        
        yield mapped_record

class StreamingColumnMapper(beam.DoFn):
    """Column mapper for streaming with cached mapping"""

    def __init__(self, target_table: str):
        self.target_table = target_table
        
    def process(self, element, mapping_dict):
        """Map columns using cached mapping from side input"""
        column_mapping = mapping_dict
        
        mapped_record = {}
        
        for source_col, target_col in column_mapping.items():
            target_col = target_col.split('.')[1] if '.' in target_col else target_col
            if target_col in element:
                mapped_record[source_col] = element[target_col]
            else:
                mapped_record[source_col] = None
        
        # Add metadata
        mapped_record['_metadata'] = {
            'ingested_at': datetime.utcnow().isoformat(),
            'processed_at': datetime.utcnow().isoformat(),
            'target_table': self.target_table,
            'stream_timestamp': element.get('_timestamp', datetime.utcnow().isoformat())
        }
        
        yield mapped_record

class MappingCacheLoader(beam.DoFn):
    """Load mapping from BigQuery periodically for side input"""
    
    def __init__(self, config: CommonPipelineConfig):  # Changed from PipelineConfig
        self.config = config
        
    def process(self, impulse):
        """Load mapping data from BigQuery"""
        try:
            client = bigquery.Client(project=self.config.project_id)
            mapping = MappingLoader.load_mapping(self.config, client)
            
            logger.info(f"Loaded mapping cache at {datetime.utcnow()}")
            yield mapping
            
        except Exception as e:
            logger.error(f"Error loading mapping cache: {e}")
            # Return default mapping on error
            yield {}


class DataQualityTransformer(beam.DoFn):
    """Transform for data quality validation"""
    
    def __init__(self, rules: List[Dict[str, Any]] = None):
        self.rules = rules or []
        self.error_counter = beam.metrics.Metrics.counter('data_quality', 'errors')
        self.success_counter = beam.metrics.Metrics.counter('data_quality', 'success')
        self.null_counter = beam.metrics.Metrics.counter('data_quality', 'nulls')
        
    def process(self, element):
        """Validate data quality rules"""
        errors = []
        
        # Check for required fields
        for rule in self.rules:
            if rule['type'] == 'required':
                field = rule['field']
                if not element.get(field):
                    errors.append(f"Missing required field: {field}")
                    
            elif rule['type'] == 'not_null':
                field = rule['field']
                if element.get(field) is None:
                    errors.append(f"Null value in field: {field}")
                    self.null_counter.inc()
                    
            elif rule['type'] == 'regex':
                field = rule['field']
                pattern = rule['pattern']
                import re
                if element.get(field) and not re.match(pattern, str(element[field])):
                    errors.append(f"Invalid format in field {field}")
                    
            elif rule['type'] == 'range':
                field = rule['field']
                min_val = rule.get('min')
                max_val = rule.get('max')
                value = element.get(field)
                if value is not None:
                    if min_val is not None and value < min_val:
                        errors.append(f"Value {value} below minimum {min_val} in field {field}")
                    if max_val is not None and value > max_val:
                        errors.append(f"Value {value} above maximum {max_val} in field {field}")
        
        if errors:
            self.error_counter.inc(len(errors))
            element['_dq_errors'] = errors
            element['_dq_status'] = 'FAILED'
            logger.warning(f"Data quality issues: {errors}")
        else:
            self.success_counter.inc()
            element['_dq_status'] = 'PASSED'
        
        yield element


class RecordHasher(Transformer):
    """Generate hash for record comparison"""
    
    def __init__(self, columns: List[str], hash_algorithm: str = 'sha256'):
        self.columns = columns
        self.hash_algorithm = hash_algorithm
        
    def transform(self, element: Dict[str, Any]) -> Dict[str, Any]:
        """Add hash to element"""
        hasher = hashlib.new(self.hash_algorithm)
        
        for col in sorted(self.columns):
            value = element.get(col, '')
            hasher.update(str(value).encode('utf-8'))
            hasher.update(b'|')
        
        element['_record_hash'] = hasher.hexdigest()
        return element


class WindowedAggregator(beam.CombineFn):
    """Aggregate data within windows"""
    
    def create_accumulator(self):
        return {
            'count': 0,
            'unique_ids': set(),
            'errors': 0,
            'start_time': None,
            'end_time': None,
            'metrics': {}
        }
    
    def add_input(self, accumulator, element):
        accumulator['count'] += 1
        
        if not accumulator['start_time']:
            accumulator['start_time'] = datetime.utcnow()
        accumulator['end_time'] = datetime.utcnow()
        
        # Track unique IDs
        if 'member_number' in element:
            accumulator['unique_ids'].add(element['member_number'])
        
        # Track errors
        if element.get('_dq_status') == 'FAILED':
            accumulator['errors'] += 1
        
        return accumulator
    
    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        
        for acc in accumulators:
            merged['count'] += acc['count']
            merged['errors'] += acc['errors']
            merged['unique_ids'].update(acc['unique_ids'])
            
            if acc['start_time']:
                if not merged['start_time'] or acc['start_time'] < merged['start_time']:
                    merged['start_time'] = acc['start_time']
            if acc['end_time']:
                if not merged['end_time'] or acc['end_time'] > merged['end_time']:
                    merged['end_time'] = acc['end_time']
        
        return merged
    
    def extract_output(self, accumulator):
        return {
            'record_count': accumulator['count'],
            'unique_count': len(accumulator['unique_ids']),
            'error_count': accumulator['errors'],
            'success_rate': (accumulator['count'] - accumulator['errors']) / max(accumulator['count'], 1),
            'window_start': accumulator['start_time'].isoformat() if accumulator['start_time'] else None,
            'window_end': accumulator['end_time'].isoformat() if accumulator['end_time'] else None,
            'duration_seconds': (accumulator['end_time'] - accumulator['start_time']).total_seconds() 
                               if accumulator['start_time'] and accumulator['end_time'] else 0
        }


class NotificationParser(beam.DoFn):
    """Parse Pub/Sub notifications for streaming"""
    
    def process(self, element):
        """Parse notification and extract key fields"""
        try:
            # Parse JSON message
            if isinstance(element, bytes):
                message = json.loads(element.decode('utf-8'))
            else:
                message = json.loads(element) if isinstance(element, str) else element
            
            # Extract member identifier
            member_number = (message.get('member_number') or 
                           message.get('profileId') or 
                           message.get('member_id'))
            
            if not member_number:
                logger.warning(f"No member identifier in notification: {message}")
                return
            
            # Ensure member_number is in the message
            message['member_number'] = member_number
            message['_timestamp'] = datetime.utcnow().isoformat()
            message['_notification_type'] = message.get('type', 'update')
            
            yield beam.Row(**message)
            
        except Exception as e:
            logger.error(f"Error parsing notification: {e}")


class CDCFormatter(beam.DoFn):
    """Format records for BigQuery CDC writes"""
    
    def __init__(self, mutation_type: str = 'UPSERT'):
        """
        Args:
            mutation_type: Type of mutation (UPSERT or DELETE)
        """
        self.mutation_type = mutation_type
        self.counter = beam.metrics.Metrics.counter('cdc', f'{mutation_type.lower()}_count')
        
    def process(self, element):
        """Format element for CDC write"""
        import time
        
        # Extract the actual record data
        record_data = element.copy()
        
        # Remove internal metadata fields
        record_data = {k: v for k, v in record_data.items() if not k.startswith('_')}
        
        # Create CDC formatted row
        cdc_row = {
            'record': record_data,
            'row_mutation_info': {
                'mutation_type': self.mutation_type,
                'change_sequence_number': str(int(time.time() * 1000000))  # Microsecond timestamp
            }
        }
        
        self.counter.inc()
        logger.debug(f"Formatted CDC {self.mutation_type} for record")
        
        yield beam.Row(**cdc_row)


class CDCUpsertFormatter(CDCFormatter):
    """Convenience class for UPSERT mutations"""
    def __init__(self):
        super().__init__(mutation_type='UPSERT')


class CDCDeleteFormatter(CDCFormatter):
    """Convenience class for DELETE mutations"""
    def __init__(self):
        super().__init__(mutation_type='DELETE')