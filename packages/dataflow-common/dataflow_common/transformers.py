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
import re
from typing import Dict, Any, List, Tuple
import time
from functools import lru_cache

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
            mapping_list = []
            for row in query_job.result():
                mapping_list.append({
                    'RECONCILE_COLUMN_NAME': row['RECONCILE_COLUMN_NAME'],
                    'PERSONAS_MAPPING_COLUMN_NAME': row['PERSONAS_MAPPING_COLUMN_NAME'],
                    'RECONCILE_RETRIEVED': row['RECONCILE_RETRIEVED'],
                    'RECONCILE_CONFIRMED': row['RECONCILE_CONFIRMED'],
                    'UPDATED_DATE': row['UPDATED_DATE']
                })

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
            
            logger.info(f"Loaded {len(mapping_list)} mapping records")
            return mapping_list
            
        except Exception as e:
            logger.error(f"Error loading mapping: {str(e)}")
            # Return default mapping if error
            return []


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
        self._column_mapping = None
        self._cache_timestamp = None
        self.cache_ttl = 600  # 10 minutes

        
    # def setup(self):
    #     """Load mapping once when DoFn starts"""
    #     all_mappings = MappingLoader.load_mapping(self.config)
    #     self.column_mapping = all_mappings.get(self.mapping_type, {})

    @property
    def column_mapping(self) -> Dict[str, str]:
        """Get column mapping with caching"""
        now = time.time()
        if (self._column_mapping is None or 
            self._cache_timestamp is None or
            now - self._cache_timestamp > self.cache_ttl):
            
            # Reload mapping
            logger.info(f"Loading mapping for {self.mapping_type}")
            all_mappings = MappingLoader.load_mapping(self.config)
            self._column_mapping = all_mappings.get(self.mapping_type, {})
            self._cache_timestamp = now
            logger.info(f"Loaded {len(self._column_mapping)} mappings")
        return self._column_mapping

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
        self.cache_size_limit = 100000

        
    def process(self, element, mapping_dict):
        """Map columns using cached mapping from side input"""
        if len(mapping_dict) > self.cache_size_limit:
            logger.warning("Mapping cache too large, consider pagination")

        column_mapping = mapping_dict  # This is the list of mappings
        
        mapped_record = {}
        
        # for rec_col_nm, psn_map_col_nm, reconcile_sts, confirmed_sts, updated_date in column_mapping.items():
        #     psn_map_col_nm = psn_map_col_nm.split('.')[1] if '.' in psn_map_col_nm else psn_map_col_nm

        #     if psn_map_col_nm in element and reconcile_sts :
        #         mapped_record[rec_col_nm] = element[psn_map_col_nm]
        #     else:
        #         mapped_record[rec_col_nm] = None
        for mapping in column_mapping:
            rec_col_nm = mapping.get('RECONCILE_COLUMN_NAME')
            psn_map_col_nm = mapping.get('PERSONAS_MAPPING_COLUMN_NAME', rec_col_nm)
            reconcile_sts = mapping.get('RECONCILE_RETRIEVED')
            
            # Clean field name
            if '.' in psn_map_col_nm:
                psn_map_col_nm = psn_map_col_nm.split('.')[-1]
            
            # Map field
            if reconcile_sts and psn_map_col_nm in element:
                mapped_record[rec_col_nm] = element[psn_map_col_nm]
            else:
                mapped_record[rec_col_nm] = None
        
        # Add metadata
        # mapped_record['_metadata'] = {
        #     'ingested_at': datetime.utcnow().isoformat(),
        #     'processed_at': datetime.utcnow().isoformat(),
        #     'target_table': self.target_table,
        #     'stream_timestamp': element.get('_timestamp', datetime.utcnow().isoformat())
        # }
        mapped_record['_metadata'] = {
                'ingested_at': datetime.utcnow().isoformat(),
                'processed_at': datetime.utcnow().isoformat(),
                'target_table': self.target_table
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



class MergeQueryGenerator(beam.DoFn):
    """Generate MERGE queries for BigQuery reconciliation"""
    
    @staticmethod
    def validate_field_name(field: str) -> str:
        """Validate and return safe field name for SQL
        
        Args:
            field: Field name to validate
            
        Returns:
            Validated field name
            
        Raises:
            ValueError: If field name is invalid
        """
        if not field:
            raise ValueError("Field name cannot be empty")
            
        # Remove any potential schema prefix
        if '.' in field:
            field = field.split('.')[-1]
            
        # Check for valid SQL identifier
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', field):
            raise ValueError(f"Invalid field name: {field}")
            
        return field
    
    def generate_merge_query(
        self,
        mapping_records: List[Dict[str, Any]], 
        column_condition: str,
        config: Dict[str, Any]
    ) -> str:
        """Generate MERGE SQL query for BigQuery
        
        Args:
            mapping_records: List of mapping configurations
            column_condition: Field name to check for merge condition (RECONCILE_RETRIEVED or RECONCILE_CONFIRMED)
            config: Pipeline configuration with project_id, datasets, tables
            
        Returns:
            SQL MERGE query string
            
        Raises:
            ValueError: If no valid mappings found
        """
        if not mapping_records:
            raise ValueError("No mapping records found to generate merge query")
        
        list_columns = []
        set_clauses = []
        insert_columns = []
        insert_values = []
        
        for record in mapping_records:
            try:
                # Get and validate field names
                source_field = self.validate_field_name(record.get('RECONCILE_COLUMN_NAME', ''))
                target_field = self.validate_field_name(
                    record.get('PERSONAS_MAPPING_COLUMN_NAME') or source_field
                )
                
                # Check if this column should be included based on condition
                should_reconcile = record.get(column_condition) 
                
                if should_reconcile:
                    # Use source data for reconciliation
                    set_clauses.append(f"tgt.`{source_field}` = src.`{target_field}`")
                    insert_columns.append(f"`{source_field}`")
                    insert_values.append(f"src.`{target_field}`")
                else:
                    # Keep target data
                    set_clauses.append(f"tgt.`{source_field}` = tgt.`{source_field}`")
                    insert_columns.append(f"`{source_field}`")
                    insert_values.append(f"COALESCE(src.`{target_field}`, tgt.`{source_field}`)")
                    
                list_columns.append(source_field)
                
            except ValueError as e:
                logger.warning(f"Skipping invalid field mapping: {e}")
                continue
        
        if not set_clauses:
            raise ValueError("No valid field mappings found in mapping records")
        
        # Determine target table based on condition
        if column_condition == 'RECONCILE_RETRIEVED':
            target_table = config.get('stg_source_table')  # stg_ms_personas
        else:  # RECONCILE_CONFIRMED
            target_table = config.get('stg_origin_table')  # stg_ms_member
        
        source_table = config.get('stg_ongoing_source_table')  # stg_personas (temp table)
        
        # Build MERGE query
        merge_query = f"""
        MERGE `{config['project_id']}.{config['staging_dataset']}.{target_table}` AS tgt
        USING `{config['project_id']}.{config['staging_dataset']}.{source_table}` AS src
        ON tgt.member_number = src.member_number
        WHEN MATCHED THEN
            UPDATE SET
                {',\n                '.join(set_clauses)},
                tgt.updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(insert_columns)})
            VALUES ({', '.join(insert_values)})
        """
        
        logger.info(f"Generated MERGE query for {target_table} with {len(set_clauses)} fields")
        return merge_query
    
    def process(self, element, mapping_records, config):
        """Process element and generate merge queries
        
        Yields:
            Dict with both merge queries
        """
        try:
            # Generate query for stg_ms_personas
            personas_query = self.generate_merge_query(
                mapping_records, 
                'RECONCILE_RETRIEVED',
                config
            )
            
            # Generate query for stg_ms_member  
            member_query = self.generate_merge_query(
                mapping_records,
                'RECONCILE_CONFIRMED', 
                config
            )
            
            yield {
                'personas_query': personas_query,
                'member_query': member_query,
                'timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error generating merge queries: {e}")
            yield {
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }

# dataflow_common/transformers.py (เพิ่มส่วนนี้)

class MergeQueryExecutor(beam.DoFn):
    """Execute generated MERGE queries via BigQuery connector"""
    
    def __init__(self, project_id: str, dataset: str):
        """
        Args:
            project_id: GCP project ID
            dataset: BigQuery dataset
        """
        self.project_id = project_id
        self.dataset = dataset
        self._connector = None
        
    def setup(self):
        """Initialize BigQuery connector"""
        from .connectors import BigQueryConnector
        self._connector = BigQueryConnector(
            project=self.project_id,
            dataset=self.dataset
        )

    def execute_with_retry(self, query: str, max_retries: int = 3):
        """Execute query with exponential backoff retry"""
        import time
        
        for attempt in range(max_retries):
            try:
                return self._connector.execute_query(query)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"Query failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
                
    def process(self, queries_dict):
        """Execute the merge queries
        
        Args:
            queries_dict: Dictionary containing personas_query and member_query
            
        Yields:
            Execution results
        """
        results = {
            'timestamp': datetime.utcnow().isoformat(),
            'personas_result': None,
            'member_result': None
        }
        
        try:
            # Execute personas merge
            if 'personas_query' in queries_dict and queries_dict['personas_query']:
                logger.info("Executing personas merge query via connector")
                personas_result = self._connector.execute_merge_query(
                    queries_dict['personas_query'],
                    table_name='stg_ms_personas'
                )
                results['personas_result'] = personas_result
                
                if personas_result.get('status') != 'success':
                    logger.error(f"Personas merge failed: {personas_result.get('error')}")
            
            # Execute member merge  
            if 'member_query' in queries_dict and queries_dict['member_query']:
                logger.info("Executing member merge query via connector")
                member_result = self._connector.execute_merge_query(
                    queries_dict['member_query'],
                    table_name='stg_ms_member'
                )
                results['member_result'] = member_result
                
                if member_result.get('status') != 'success':
                    logger.error(f"Member merge failed: {member_result.get('error')}")
            
            # Determine overall status
            personas_success = results['personas_result'] and results['personas_result'].get('status') == 'success'
            member_success = results['member_result'] and results['member_result'].get('status') == 'success'
            
            if personas_success and member_success:
                results['overall_status'] = 'success'
                logger.info("All merge queries completed successfully")
            elif personas_success or member_success:
                results['overall_status'] = 'partial_success'
                logger.warning("Some merge queries failed")
            else:
                results['overall_status'] = 'failed'
                logger.error("All merge queries failed")
                
        except Exception as e:
            logger.error(f"Error executing merge queries: {e}")
            results['overall_status'] = 'error'
            results['error'] = str(e)
            
        yield results