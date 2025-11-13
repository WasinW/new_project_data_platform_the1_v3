import apache_beam as beam
from apache_beam import DoFn, ParDo, Map, FlatMap
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import window
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.transforms.window import IntervalWindow
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.io.fileio import FileNaming, WindowedFilenamePolicy
from apache_beam.utils.windowed_value import PaneInfo
from datetime import datetime, timedelta
from functools import reduce
import operator
import json
import logging
from typing import Dict, Any, Optional, List
from google.cloud import bigtable
from google.cloud.bigtable import row_filters
import pyarrow as pa # ต้องใช้ pyarrow ในการกำหนด Parquet Schema

# Configuration
PROJECT_ID = "the1-insight-dev"
SUBSCRIPTION_NAME = "ms-personas-datapipeline-dataflow-subscription"
MAPPING_TABLE = f"{PROJECT_ID}.insight_dev.stg_mapping_reconcile"
BIGLAKE_TABLE = f"{PROJECT_ID}.insight_dev.ms_personas"
BT_INSTANCE = "personas-instance"
BT_TABLE = "personas"

# GCS Staging Bucket สำหรับ Parquet (จะถูก Sync ไป S3 ในภายหลัง)
# ⚠️ เปลี่ยนเป็น GCS path ของคุณ
S3_PARQUET_BUCKET = f"s3://t1-analytics/refined/insights/ms_personas_realtime_dev" 
# --- Parquet Schema Definition ---
# ⚠️ คุณต้องกำหนด Schema นี้ให้ตรงกับ output ของ TransformSchemasDoFn
PARQUET_SCHEMA = pa.schema([
    pa.field('personas_id', pa.string()),
    pa.field('member_number', pa.string()),
    pa.field('gender', pa.string()),
    pa.field('birth_year', pa.int64()),
    # Column Family ที่เป็น JSON/String จาก Bigtable
    pa.field('profiles_json', pa.string()), 
    # Field สำหรับการ Backward Compatibility (S3 AWS)
    pa.field('s3_aws_compatible_schema', pa.string()), 
    # Timestamp สำหรับ Tracking
    pa.field('_insert_timestamp', pa.timestamp('us', tz='utc')) 
])

# --- Custom Naming Policy สำหรับ Hourly Partitioning (ใช้ได้ทั้ง S3 และ GCS) ---

class HourlyPartitioningPolicy(WindowedFilenamePolicy):
    """
    สร้าง Path ตาม Partition hourly:
    .../par_month=MM/par_day=DD/par_hour=HH/run_dt=YYYYMMDDHH/
    """
    def __init__(self, base_path, prefix='data'):
        # base_path คือ S3_PARQUET_BUCKET
        self.base_path = base_path
        self.prefix = prefix
        # ใช้ ShardNameTemplate มาตรฐานสำหรับ Parquet (.parquet)
        self.shard_template = FileNaming.default(
            prefix='shard', shard_template='-SSSSS-of-NNNNN', extension='.parquet'
        )

    def get_filename(self, window, shard_id, num_shards, pane_info):
        # 1. ใช้ End Time ของ Window ในการกำหนด Partition
        # window.end เป็น Beam timestamp (Microseconds)
        window_end_micros = window.end.micros
        window_end = datetime.fromtimestamp(window_end_micros / 10**6) 

        # 2. สร้าง Partition Folders: par_month=MM/par_day=DD/par_hour=HH
        par_month = window_end.strftime('%m')
        par_day = window_end.strftime('%d')
        par_hour = window_end.strftime('%H')
        
        # 3. สร้าง Sub-folder: run_dt=YYYYMMDDHH
        run_dt = window_end.strftime('%Y%m%d%H')
        
        partition_path = (
            f"par_month={par_month}/"
            f"par_day={par_day}/"
            f"par_hour={par_hour}/"
            f"run_dt={run_dt}"
        )
        
        # 4. สร้าง Filename 
        file_name = self.shard_template.get_filename(shard_id, num_shards, pane_info)
        
        # คืนค่าเป็น Full Path: base_path/par_month=.../run_dt=.../cdc-data-shard-00000-of-00001.parquet
        return f"{self.base_path}/{partition_path}/{self.prefix}-{file_name}"

class MappingRefreshDoFn(DoFn):
    """Refresh mapping table every hour"""
    
    def __init__(self, mapping_table):
        self.mapping_table = mapping_table
        
    def process(self, element):
        from google.cloud import bigquery
        client = bigquery.Client()
        
        # Query mapping table
        query = f"""
        SELECT 
            reconcile_column_name,
            personas_mapping_column_name,
            reconcile_retrieved,
            reconcile_confirmed
        FROM `{self.mapping_table}`
        WHERE reconcile_retrieved = 'Y'
        """
        
        results = client.query(query).result()
        
        # Transform to mapping structure
        mapping_dict = {}
        schemas_dict = []
        
        for row in results:
            old_name = row['reconcile_column_name']
            new_name = row['personas_mapping_column_name']
            
            # mapping_dict[old_name] = new_name.split('.')[-1]  # Use last part for flat mapping
            mapping_dict[old_name] = new_name
            # mapping_dict : {'is_mobile': {'profile':{'consent':'has_mobile'}} }
            # message structure : {'profile':{'consent':{'has_mobile':'Y'}}}
            
            # mapping_dict[old_name][]
            schemas_dict.append(old_name)
            # Parse nested column names (e.g., 'profile.memberId')
            # parts = new_name.split('.')
            # if len(parts) == 2:
            #     parent, field = parts
            #     if parent not in mapping_dict:
            #         mapping_dict[parent] = {}
            #         schemas_dict[parent] = []
            #     mapping_dict[parent][field] = old_name
            #     schemas_dict[parent].append(field)
            # else:
            #     # Handle non-nested columns if any
            #     mapping_dict[new_name] = old_name
        
        logging.info(f"Refreshed mapping: {mapping_dict}")
        yield beam.pvalue.AsSingleton({
            'mapping_dict': mapping_dict,
            'schemas_dict': schemas_dict
        })

class ExtractPersonasDoFn(DoFn):
    """Extract personasId from PubSub message"""
    
    def process(self, element):
        try:
            message = json.loads(element.decode('utf-8'))
            personas_id = message.get('personasId')
            
            if personas_id:
                yield {
                    'personas_id': personas_id,
                    'message': message
                }
        except Exception as e:
            logging.error(f"Error parsing message: {e}")

class FetchFromBigtableDoFn(DoFn):
    """Fetch data from BigTable using personasId"""
    
    def __init__(self, instance_id, table_id, parent_field='profiles'):
        self.instance_id = instance_id
        self.table_id = table_id
        self.parent_field = parent_field
        self._client = None
        self._table = None
        
    def setup(self):
        """Initializes Bigtable Client once per worker."""
        try:

            self._client = bigtable.Client(project=PROJECT_ID)
            self._instance = self._client.instance(self.instance_id)
            self._table = self._instance.table(self.table_id)
            logging.info("Bigtable client and table initialized.")
        except Exception as e:
            logging.error(f"Failed to initialize Bigtable client: {e}")
            self._client = None
            self._table = None


    def process(self, element):
        if not self._table:
            logging.error("Bigtable table not available. Skipping record.")
            return

        try:
            personas_id = element.get('personas_id')
            if not personas_id:
                logging.warning("Missing personas_id in element.")
                return
            
            row_key = personas_id.encode()
            row = self._table.read_row(row_key)
            
            if row:
                # profile_data = self.transform_message(row.cells, mapping_dict)
                # Extract profile data from BigTable
                family_data = {}
                # family_data = row.cells.get(self.parent_field, {})

                for column_family_id, columns in row.cells.items():
                    if column_family_id == self.parent_field:
                        for column, cells in columns.items():
                            # Get the latest value
                            value = cells[0].value.decode('utf-8')
                            try:
                                # Try to parse as JSON if applicable
                                family_data[column.decode('utf-8')] = json.loads(value)
                            except:
                                family_data[column.decode('utf-8')] = value
                
                yield {
                    'personas_id': personas_id,
                    self.parent_field: family_data,
                    'original_message': element['message']
                }
            else:
                logging.warning(f"Row not found for personas_id: {personas_id}")

        except Exception as e:
            # Log error แต่ไม่ให้ pipeline fail
            logging.error(f"Error processing personas_id {element.get('personas_id')}: {str(e)}")
            # อาจจะ yield error record สำหรับ dead letter queue
            yield {
                'personas_id': element.get('personas_id'),
                'error': str(e),
                'error_type': 'processing_error'
            }

class TransformSchemasDoFn(DoFn):
    """Transform data according to mapping dictionary"""
    def get_nested_value(self , data, path):
        """ดึงค่าจาก nested dict ด้วย dot notation path"""
        try:
            return reduce(operator.getitem, path.split('.'), data)
        except (KeyError, TypeError):
            return None

    def transform_message(self , message_dict, mapping_dict):
        """แปลง message ตาม mapping"""
        result = {}
        for new_key, path in mapping_dict.items():
            value = self.get_nested_value(message_dict, path)
            if value is not None:
                result[new_key] = value
            else:
                result[new_key] = None
        return result


    def process(self, element, mapping_info, family_field='profile',pk_key='personas_id'):
        mapping_dict = mapping_info.get('mapping_dict', {})
        # schemas_dict = mapping_info.get('schemas_dict', {})
        
        personas_id = element[pk_key]
        family_data = element.get(family_field, {})
        
        # Prepare outputs
        # aws_output = {}  # For old schema
        # gcp_output = {'personasId': personas_id}  # For new schema
        # aws_output = self.transform_message(element, mapping_dict)

        # Process family data
        # if family_field in mapping_dict and family_data:
        if family_data:
        #     family_mapping = mapping_dict[family_field]
        #     family_schemas = schemas_dict.get(family_field, [])
            full_data = {
                'personas_id': personas_id,
                family_field: family_data,
                # เพิ่ม fields อื่นๆ จาก original_message ถ้าต้องการ
                'original_message': element.get('original_message', {}) 
            }
            
            aws_output = self.transform_message(full_data, mapping_dict)

        #     # For AWS (old schema)
        #     for new_field, old_field in family_mapping.items():
        #         value = family_data.get(new_field)
        #         aws_output[old_field] = value if value is not None else None
            
        #     # Add missing columns as None
        #     for field in family_schemas:
        #         if field in family_mapping:
        #             old_name = family_mapping[field]
        #             if old_name not in aws_output:
        #                 aws_output[old_name] = None
            
            # For GCP (new schema)
            gcp_output = {
                'personasId': personas_id,
                family_field: family_data
                # BigQuery Auto-detect Schema จะทำงานได้ดีกับ nested dict
            }
        
        yield beam.pvalue.TaggedOutput('aws', aws_output)
        yield beam.pvalue.TaggedOutput('gcp', gcp_output)

class WriteToBigLakeDoFn(DoFn):
    """Custom write to BigLake with partitioning"""
    
    def __init__(self, table_name):
        self.table_name = table_name
        
    def process(self, element):
        # Prepare for BigLake write with proper data types
        output = {}
        for key, value in element.items():
            # Convert None to appropriate BigQuery NULL
            if value is None:
                output[key] = None
            elif isinstance(value, dict):
                output[key] = json.dumps(value)
            else:
                output[key] = value
        
        # Add timestamp for partitioning
        # output['_insert_timestamp'] = datetime.utcnow().isoformat()
        
        yield output

# class WriteToS3DoFn(DoFn):
#     """Write to S3 with CDC pattern"""
    
#     def __init__(self, s3_bucket, s3_prefix):
#         self.s3_bucket = s3_bucket
#         self.s3_prefix = s3_prefix
        
#     def setup(self):
#         import boto3
#         self.s3_client = boto3.client('s3')
        
#     def process(self, element):
#         # Prepare CDC format
#         cdc_record = {
#             'operation': 'INSERT',  # or UPDATE based on logic
#             'timestamp': datetime.utcnow().isoformat(),
#             'data': element
#         }
        
#         # Generate partition path
#         now = datetime.utcnow()
#         partition_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
        
#         # Write to S3 (batch for better performance in production)
#         key = f"{self.s3_prefix}/{partition_path}/{element.get('member_number', 'unknown')}_{now.timestamp()}.json"
        
#         self.s3_client.put_object(
#             Bucket=self.s3_bucket,
#             Key=key,
#             Body=json.dumps(cdc_record)
#         )
        
#         yield element
# --- Custom Naming Policy สำหรับ Hourly Partitioning ---

class HourlyPartitioningPolicy(WindowedFilenamePolicy):
    """
    สร้าง Path ตาม Partition hourly:
    .../par_month=MM/par_day=DD/par_hour=HH/run_dt=YYYYMMDDHH/
    """
    def __init__(self, base_path, prefix='data'):
        # base_path คือ S3_PARQUET_BUCKET
        self.base_path = base_path
        self.prefix = prefix
        # ใช้ ShardNameTemplate มาตรฐานสำหรับ Parquet (.parquet)
        self.shard_template = FileNaming.default(
            prefix='shard', shard_template='-SSSSS-of-NNNNN', extension='.parquet'
        )

    def get_filename(self, window, shard_id, num_shards, pane_info):
        # 1. ใช้ End Time ของ Window ในการกำหนด Partition
        # window.end เป็น Beam timestamp (Microseconds)
        window_end_micros = window.end.micros
        window_end = datetime.fromtimestamp(window_end_micros / 10**6) 

        # 2. สร้าง Partition Folders: par_month=MM/par_day=DD/par_hour=HH
        par_month = window_end.strftime('%m')
        par_day = window_end.strftime('%d')
        par_hour = window_end.strftime('%H')
        
        # 3. สร้าง Sub-folder: run_dt=YYYYMMDDHH
        run_dt = window_end.strftime('%Y%m%d%H')
        
        partition_path = (
            f"par_month={par_month}/"
            f"par_day={par_day}/"
            f"par_hour={par_hour}/"
            f"run_dt={run_dt}"
        )
        
        # 4. สร้าง Filename 
        file_name = self.shard_template.get_filename(shard_id, num_shards, pane_info)
        
        # คืนค่าเป็น Full Path: base_path/par_month=.../run_dt=.../cdc-data-shard-00000-of-00001.parquet
        return f"{self.base_path}/{partition_path}/{self.prefix}-{file_name}"

def create_pipeline():
    """Create the main pipeline"""
    
    pipeline_options = PipelineOptions(
        streaming=True,
        runner='DataflowRunner',
        project=PROJECT_ID,
        job_name='ms-member-realtime-pipeline',
        temp_location='t1-insight-audit-bucket/audit_log/dataflow/temp',
        region='asia-southeast1',
        autoscaling_algorithm='THROUGHPUT_BASED',
        max_num_workers=10,
        experiments=['use_runner_v2']
    )
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Step 0: Cache mapping table (refresh every hour)
        mapping_refresh = (
            pipeline
            | 'PeriodicTrigger' >> PeriodicImpulse(
                start_timestamp=datetime.utcnow(),
                stop_timestamp=datetime.max,
                fire_interval=3600  # 1 hour in seconds
            )
            | 'RefreshMapping' >> ParDo(MappingRefreshDoFn(MAPPING_TABLE))
        )
        
        # Step 1-2: Consume from PubSub and extract personasId
        messages = (
            pipeline
            | 'ReadFromPubSub' >> ReadFromPubSub(subscription=SUBSCRIPTION_NAME)
            | 'ExtractPersonasId' >> ParDo(ExtractPersonasDoFn())
        )
        
        # Step 3: Fetch from BigTable
        bigtable_data = (
            messages
            | 'FetchFromBigTable' >> ParDo(
                FetchFromBigtableDoFn(BT_INSTANCE, BT_TABLE,parent_field='profiles')
            )
        )
        
        # Step 5: Transform schemas
        transformed = (
            bigtable_data
            | 'TransformSchemas' >> ParDo(
                TransformSchemasDoFn(),
                mapping_info=beam.pvalue.AsSingleton(mapping_refresh),
                family_field='profiles',
                pk_key='personas_id'
            ).with_outputs('aws', 'gcp')
        )
        
        # Step 6.1: Write to BigLake (GCP)
        gcp_data = transformed.gcp
        (
            gcp_data
            | 'PrepareForBigLake' >> ParDo(WriteToBigLakeDoFn(BIGLAKE_TABLE))
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=BIGLAKE_TABLE,
                schema='SCHEMA_AUTODETECT',
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
        
        # Step 6.2: Write to S3 (AWS)
        # aws_data = transformed.aws
        # (
        #     aws_data
        #     | 'WriteToS3' >> ParDo(
        #         WriteToS3DoFn(
        #             s3_bucket='your-s3-bucket',
        #             s3_prefix='ms-personas-cdc'
        #         )
        #     )
        # )

        aws_data = transformed.aws
        (
            aws_data
            # 5.2.1. Windowing: Batch data into 5-minute fixed windows (Near-Real-time batching)
            | 'ApplyFixedWindow' >> beam.WindowInto(
                window.FixedWindows(300) # 5 minutes = 300 seconds
            )
            # 5.2.2. Write to Parquet using Custom Filename Policy
            | 'WriteToParquetS3' >> WriteToParquet(
                file_path_prefix=S3_PARQUET_BUCKET, # ⚠️ S3 URI
                schema=PARQUET_SCHEMA,
                file_naming=HourlyPartitioningPolicy(
                    base_path=S3_PARQUET_BUCKET,
                    prefix='cdc-data'
                ),
                # ตั้งเป็น 1 เพื่อให้ได้ไฟล์ต่อ partition น้อยที่สุด
                num_shards=1 
            )
        )

    return pipeline

# Infrastructure setup script
# def setup_infrastructure():
#     """Setup required infrastructure"""
    
#     from google.cloud import bigquery
#     from google.cloud import pubsub_v1
    
#     # Create BigLake table
#     bq_client = bigquery.Client()
    
#     # BigLake external table DDL
#     biglake_ddl = """
#     CREATE OR REPLACE EXTERNAL TABLE `{}`
#     OPTIONS (
#         format = 'PARQUET',
#         uris = ['gs://your-bucket/biglake-data/*'],
#         max_staleness = INTERVAL 1 HOUR
#     )
#     """.format(BIGLAKE_TABLE)
    
#     # Create Pub/Sub subscription
#     publisher = pubsub_v1.PublisherClient()
#     subscriber = pubsub_v1.SubscriberClient()
    
#     topic_path = publisher.topic_path(PROJECT_ID, 'ms-member-realtime-topic')
#     subscription_path = subscriber.subscription_path(PROJECT_ID, 'ms-member-realtime-sub')
    
#     try:
#         subscriber.create_subscription(
#             request={
#                 "name": subscription_path,
#                 "topic": topic_path,
#                 "ack_deadline_seconds": 60,
#                 "message_retention_duration": {"seconds": 86400}  # 1 day
#             }
#         )
#         print(f"Subscription created: {subscription_path}")
#     except Exception as e:
#         print(f"Subscription might already exist: {e}")

if __name__ == "__main__":
    # Setup infrastructure first (run once)
    # setup_infrastructure()
    
    # Create and run pipeline
    pipeline = create_pipeline()
    pipeline.run()