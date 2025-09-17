# dataflow_common/connectors.py
"""Enhanced data connectors for various sources"""

from typing import Dict, Any, Optional, List
import logging
import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, BigQueryDisposition
from google.cloud import bigquery, bigtable
from google.cloud.bigtable import row_filters
from .core import DataConnector

logger = logging.getLogger(__name__)


class BigQueryConnector(DataConnector):
    """Enhanced BigQuery connector with credentials support"""
    
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
              method: str = "STORAGE_WRITE_API", schema: str = 'SCHEMA_AUTODETECT',
              streaming_mode: Optional[str] = None, **kwargs):
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
        
        # Add streaming specific options
        if streaming_mode == 'at_least_once' and method == 'STORAGE_WRITE_API':
            write_options['use_at_least_once'] = True
        
        if self.credentials_path:
            write_options['service_account_json'] = self.credentials_path
        
        write_options.update(kwargs)
        
        return WriteToBigQuery(**write_options)
    
    def query(self, query: str) -> List[Dict[str, Any]]:
        """Execute query and return results"""
        query_job = self.client.query(query)
        return [dict(row) for row in query_job.result()]


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
