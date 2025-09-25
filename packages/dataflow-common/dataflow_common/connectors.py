# dataflow_common/connectors.py
"""Enhanced data connectors for various sources"""

from typing import Dict, Any, Optional, List, Union
import logging
import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, BigQueryDisposition
from google.cloud import bigquery, bigtable
from google.cloud.bigtable import row_filters
from .core import DataConnector

logger = logging.getLogger(__name__)


class BigQueryConnector(DataConnector):
    """Enhanced BigQuery connector with CDC and WRITE_TRUNCATE support"""
    
    def __init__(self, project: str, dataset: str = None, 
                 credentials_path: Optional[str] = None):
        self.project = project
        self.dataset = dataset
        self.credentials_path = credentials_path
        self._client = None
        
    @property
    def client(self):
        """Lazy load BigQuery client"""
        if not self._client:
            if self.credentials_path:
                self._client = bigquery.Client.from_service_account_json(
                    self.credentials_path, project=self.project
                )
            else:
                self._client = bigquery.Client(project=self.project)
        return self._client
        
    def read(self, table: str = None, query: str = None, 
             method: str = 'DIRECT_READ', **kwargs):
        """Read from BigQuery with various methods"""
        read_options = {
            'use_standard_sql': True,
            'project': self.project,
        }
        
        if method:
            read_method = getattr(beam.io.ReadFromBigQuery.Method, method, None)
            if read_method:
                read_options['method'] = read_method
        
        if self.credentials_path:
            read_options['service_account_json'] = self.credentials_path
        
        read_options.update(kwargs)
        
        if query:
            read_options['query'] = query
        elif table:
            full_table = f"{self.project}.{self.dataset}.{table}" if self.dataset else table
            read_options['table'] = full_table
        else:
            raise ValueError("Either table or query must be provided")
            
        return ReadFromBigQuery(**read_options)
    
    def write(self, table: str, mode: str = "WRITE_APPEND", 
              method: str = "STORAGE_WRITE_API", schema: Union[str, dict] = 'SCHEMA_AUTODETECT',
              streaming_mode: Optional[str] = None, 
              use_cdc: bool = False,  # เพิ่ม parameter
              primary_key: Optional[List[str]] = None,  # เพิ่ม parameter
              **kwargs):
        """Write to BigQuery with various methods and modes"""
        full_table = f"{self.project}.{self.dataset}.{table}" if self.dataset else table
        
        write_options = {
            'table': full_table,
            'schema': schema,
            'write_disposition': getattr(BigQueryDisposition, mode),
            'create_disposition': BigQueryDisposition.CREATE_IF_NEEDED,
        }
        
        # Set write method
        write_method = getattr(WriteToBigQuery.Method, method, None)
        if write_method:
            write_options['method'] = write_method
        
        # Handle CDC writes for upserts
        if use_cdc:  # ตอนนี้ use_cdc เป็น parameter แล้ว
            if method != 'STORAGE_WRITE_API':
                raise ValueError("CDC writes require STORAGE_WRITE_API method")
            
            if not primary_key:  # ตอนนี้ primary_key เป็น parameter แล้ว
                raise ValueError("CDC writes require primary_key to be specified")
            
            # Enable CDC writes
            write_options['use_cdc_writes'] = True
            write_options['primary_key'] = primary_key
            
            # For CDC, we need at_least_once semantics
            if streaming_mode != 'exactly_once':
                write_options['use_at_least_once'] = True
            
            logger.info(f"Configured CDC writes for table {full_table} with primary key: {primary_key}")
            
        # Handle streaming specific options (non-CDC)
        elif streaming_mode == 'at_least_once' and method == 'STORAGE_WRITE_API':
            write_options['use_at_least_once'] = True

        # Handle WRITE_TRUNCATE for batch mode
        if mode == 'WRITE_TRUNCATE':
            # WRITE_TRUNCATE is handled by BigQueryDisposition
            if method == 'FILE_LOADS':
                logger.info(f"Using WRITE_TRUNCATE with FILE_LOADS for table {full_table}")
                if 'custom_gcs_temp_location' not in write_options:
                    write_options['custom_gcs_temp_location'] = (
                        kwargs.get('custom_gcs_temp_location') or
                        kwargs.get('temp_location') or
                        'gs://t1-insight-audit-bucket/audit_log/dataflow/temp'
                    )
                write_options['triggering_frequency']=1
                write_options['file_loads_buffer_size']=1
            else:
                logger.info(f"Using WRITE_TRUNCATE with {method} for table {full_table}")

            # write_options['use_cdc_writes'] = True
            # write_options['primary_key'] = ['member_id']
        
        if self.credentials_path:
            write_options['service_account_json'] = self.credentials_path
        
        # Apply any additional options
        write_options.update(kwargs)
        
        return WriteToBigQuery(**write_options)
    
    def write_cdc(self, table: str, primary_key: List[str],
                  schema: Union[str, dict] = 'SCHEMA_AUTODETECT',
                  **kwargs):
        """Convenience method for CDC writes (upserts)
        
        Args:
            table: Target table name
            primary_key: List of primary key columns
            schema: Table schema
            **kwargs: Additional options
        """
        return self.write(
            table=table,
            mode='WRITE_APPEND',  # CDC always uses WRITE_APPEND
            method='STORAGE_WRITE_API',
            use_cdc=True,
            primary_key=primary_key,
            schema=schema,
            **kwargs
        )
    
    def write_truncate(self, table: str, 
                      schema: Union[str, dict] = 'SCHEMA_AUTODETECT',
                      method: str = 'FILE_LOADS',
                      **kwargs):
        """Convenience method for WRITE_TRUNCATE
        
        Args:
            table: Target table name
            schema: Table schema
            method: Write method (default FILE_LOADS for batch)
            **kwargs: Additional options
        """
        return self.write(
            table=table,
            mode='WRITE_TRUNCATE',
            method=method,
            schema=schema,
            **kwargs
        )
    
    def query(self, query: str) -> List[Dict[str, Any]]:
        """Execute query and return results"""
        query_job = self.client.query(query)
        return [dict(row) for row in query_job.result()]

    def execute_query(self, query: str, timeout: int = 300) -> Dict[str, Any]:
        """Execute a BigQuery SQL query and wait for completion
        
        Args:
            query: SQL query to execute
            timeout: Maximum time to wait in seconds
            
        Returns:
            Dictionary with execution results
        """
        try:
            logger.info(f"Executing BigQuery query: {query[:100]}...")
            
            # Configure query job
            job_config = bigquery.QueryJobConfig(
                use_legacy_sql=False,
                priority=bigquery.QueryPriority.INTERACTIVE,
            )
            
            # Execute query
            query_job = self.client.query(query, job_config=job_config)
            
            # Wait for completion
            result = query_job.result(timeout=timeout)
            
            # Get job statistics
            stats = {
                'job_id': query_job.job_id,
                'state': query_job.state,
                'created': query_job.created.isoformat() if query_job.created else None,
                'started': query_job.started.isoformat() if query_job.started else None,
                'ended': query_job.ended.isoformat() if query_job.ended else None,
                'total_bytes_processed': query_job.total_bytes_processed,
                'total_bytes_billed': query_job.total_bytes_billed,
                'slot_millis': query_job.slot_millis,
                'status': 'success'
            }
            
            logger.info(f"Query completed successfully. Job ID: {query_job.job_id}")
            return stats
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'query_preview': query[:500]
            }
    
    def execute_merge_query(self, merge_query: str, table_name: str = None) -> Dict[str, Any]:
        """Execute a MERGE query specifically
        
        Args:
            merge_query: MERGE SQL query
            table_name: Optional table name for logging
            
        Returns:
            Dictionary with merge results
        """
        logger.info(f"Executing MERGE query for table: {table_name or 'unknown'}")
        
        result = self.execute_query(merge_query)
        
        if result.get('status') == 'success':
            logger.info(f"MERGE completed for {table_name}. Bytes processed: {result.get('total_bytes_processed')}")
        else:
            logger.error(f"MERGE failed for {table_name}: {result.get('error')}")
            
        return result

class PubSubConnector(DataConnector):
    """Pub/Sub connector for streaming pipelines"""
    
    def __init__(self, project: str, credentials_path: Optional[str] = None):
        self.project = project
        self.credentials_path = credentials_path
        
    def read(self, topic: str = None, subscription: str = None, 
             id_label: Optional[str] = None, with_attributes: bool = False):
        """Read from Pub/Sub topic or subscription"""
        if subscription:
            return ReadFromPubSub(
                subscription=f"projects/{self.project}/subscriptions/{subscription}",
                id_label=id_label,
                with_attributes=with_attributes
            )
        elif topic:
            return ReadFromPubSub(
                topic=f"projects/{self.project}/topics/{topic}",
                id_label=id_label,
                with_attributes=with_attributes
            )
        else:
            raise ValueError("Either topic or subscription must be provided")
    
    def write(self, topic: str, with_attributes: bool = False):
        """Write to Pub/Sub topic"""
        from apache_beam.io import WriteToPubSub
        return WriteToPubSub(
            topic=f"projects/{self.project}/topics/{topic}",
            with_attributes=with_attributes
        )


class BigtableConnector(DataConnector):
    """Bigtable connector for real-time lookups"""
    
    def __init__(self, project: str, instance_id: str, table_id: str,
                 app_profile_id: Optional[str] = None,
                 credentials_path: Optional[str] = None):
        self.project = project
        self.instance_id = instance_id
        self.table_id = table_id
        self.app_profile_id = app_profile_id
        self.credentials_path = credentials_path
        self._client = None
        self._table = None
        
    @property
    def client(self):
        """Lazy load Bigtable client"""
        if not self._client:
            if self.credentials_path:
                from google.oauth2 import service_account
                credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_path
                )
                self._client = bigtable.Client(
                    project=self.project,
                    credentials=credentials
                )
            else:
                self._client = bigtable.Client(project=self.project)
        return self._client
    
    @property
    def table(self):
        """Get Bigtable table reference"""
        if not self._table:
            instance = self.client.instance(self.instance_id)
            self._table = instance.table(self.table_id)
        return self._table
    
    def read(self, row_keys: List[str] = None, row_filter=None):
        """Read from Bigtable"""
        from apache_beam.io.gcp.bigtableio import ReadFromBigtable
        
        read_options = {
            'project_id': self.project,
            'instance_id': self.instance_id,
            'table_id': self.table_id,
        }
        
        if self.app_profile_id:
            read_options['app_profile_id'] = self.app_profile_id
        
        if row_filter:
            read_options['row_filter'] = row_filter
        
        return ReadFromBigtable(**read_options)
    
    def write(self):
        """Write to Bigtable"""
        from apache_beam.io.gcp.bigtableio import WriteToBigtable
        
        write_options = {
            'project_id': self.project,
            'instance_id': self.instance_id,
            'table_id': self.table_id,
        }
        
        if self.app_profile_id:
            write_options['app_profile_id'] = self.app_profile_id
        
        return WriteToBigtable(**write_options)
    
    def create_enrichment_handler(self, row_key_field: str,
                                 columns_to_fetch: Optional[List[str]] = None):
        """Create Bigtable enrichment handler for pipeline"""
        from apache_beam.transforms.enrichment_handlers.bigtable import BigTableEnrichmentHandler
        
        # Create row filter for specific columns if provided
        row_filter = None
        if columns_to_fetch:
            regex_pattern = '|'.join(columns_to_fetch)
            row_filter = row_filters.ColumnQualifierRegexFilter(
                regex_pattern.encode('utf-8')
            )
        
        handler = BigTableEnrichmentHandler(
            project_id=self.project,
            instance_id=self.instance_id,
            table_id=self.table_id,
            row_key=row_key_field,
            row_filter=row_filter,
            app_profile_id=self.app_profile_id
        )
        
        return handler


class CloudStorageConnector(DataConnector):
    """Enhanced Cloud Storage connector"""
    
    def __init__(self, bucket: str, format: str = "parquet"):
        self.bucket = bucket if not bucket.startswith('gs://') else bucket[5:]
        self.format = format
        
    def read(self, path: str, **kwargs):
        """Read from Cloud Storage"""
        full_path = f"gs://{self.bucket}/{path}"
        
        if self.format == "parquet":
            from apache_beam.io.parquetio import ReadFromParquet
            return ReadFromParquet(full_path, **kwargs)
        elif self.format == "avro":
            from apache_beam.io.avroio import ReadFromAvro
            return ReadFromAvro(full_path, **kwargs)
        elif self.format == "json":
            return beam.io.ReadFromText(full_path) | beam.Map(lambda x: json.loads(x))
        elif self.format == "text":
            return beam.io.ReadFromText(full_path, **kwargs)
        else:
            raise ValueError(f"Unsupported format: {self.format}")
    
    def write(self, path: str, **kwargs):
        """Write to Cloud Storage"""
        full_path = f"gs://{self.bucket}/{path}"
        
        if self.format == "parquet":
            from apache_beam.io.parquetio import WriteToParquet
            return WriteToParquet(full_path, **kwargs)
        elif self.format == "avro":
            from apache_beam.io.avroio import WriteToAvro
            return WriteToAvro(full_path, **kwargs)
        elif self.format == "json":
            return beam.Map(lambda x: json.dumps(x)) | beam.io.WriteToText(full_path, **kwargs)
        elif self.format == "text":
            return beam.io.WriteToText(full_path, **kwargs)
        else:
            raise ValueError(f"Unsupported format: {self.format}")


class ConnectorFactory:
    """Factory for creating appropriate connectors"""
    
    @staticmethod
    def create_connector(connector_type: str, config: Dict[str, Any]) -> DataConnector:
        """Create connector based on type and configuration"""
        if connector_type == 'bigquery':
            return BigQueryConnector(
                project=config.get('project'),
                dataset=config.get('dataset'),
                credentials_path=config.get('credentials_path')
            )
        elif connector_type == 'pubsub':
            return PubSubConnector(
                project=config.get('project'),
                credentials_path=config.get('credentials_path')
            )
        elif connector_type == 'bigtable':
            return BigtableConnector(
                project=config.get('project'),
                instance_id=config.get('instance_id'),
                table_id=config.get('table_id'),
                app_profile_id=config.get('app_profile_id'),
                credentials_path=config.get('credentials_path')
            )
        elif connector_type == 'gcs':
            return CloudStorageConnector(
                bucket=config.get('bucket'),
                format=config.get('format', 'parquet')
            )
        else:
            raise ValueError(f"Unsupported connector type: {connector_type}")
