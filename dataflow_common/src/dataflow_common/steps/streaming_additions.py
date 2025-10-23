# dataflow_common/src/dataflow_common/steps/streaming_additions.py

from __future__ import annotations
import os
import json
import logging
import apache_beam as beam
from typing import Dict, Any
from ..core import BaseStep
from ..transforms.mapping import extract_by_path

LOGGER = logging.getLogger(__name__)

# 1. WindowingAuditStep - สำหรับ audit
class WindowingAuditStep(BaseStep):
    """Create audit window every hour"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        window_size = self.spec.get("size", 3600)
        
        return (
            pipeline
            | f"{self.step_id}_Impulse" >> beam.transforms.PeriodicImpulse(
                fire_interval=window_size
            )
            | f"{self.step_id}_CreateAudit" >> beam.Map(
                lambda x: {
                    "timestamp": beam.utils.timestamp.Timestamp.now(),
                    "pipeline": self.config.name
                }
            )
        )

# 2. WindowingOpenHourlyPartitionStep - สร้าง S3 directory + add metadata
class WindowingOpenHourlyPartitionStep(BaseStep):
    """Create S3 hourly partitions and add path metadata"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing input '{input_key}'")
            
        pcoll = self.state[input_key]
        window_size = self.spec.get("size", 3600)
        s3_prefix = self.config.io.s3.get("refined_prefix")
        s3_region = self.config.io.s3.get("region", "ap-southeast-1")
        aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        
        class CreatePartitionDoFn(beam.DoFn):
            def __init__(self, s3_prefix, region, access_key, secret_key):
                self.s3_prefix = s3_prefix
                self.region = region
                self.access_key = access_key
                self.secret_key = secret_key
                self._s3_client = None
                self._created_partitions = set()
                
            def setup(self):
                import boto3
                self._s3_client = boto3.client(
                    's3',
                    region_name=self.region,
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key
                )
                
            def process(self, element, window=beam.DoFn.WindowParam):
                from datetime import datetime
                
                window_start = window.start.to_utc_datetime()
                year = window_start.strftime("%Y")
                month = window_start.strftime("%m")
                day = window_start.strftime("%d")
                hour = window_start.strftime("%H")
                
                partition_path = f"par_year={year}/par_month={month}/par_day={day}/par_hour={hour}"
                full_path = f"{self.s3_prefix}/ms_personas_streaming/{partition_path}"
                
                # Create S3 directory once per partition
                if full_path not in self._created_partitions:
                    self._create_s3_directory(full_path)
                    self._created_partitions.add(full_path)
                    LOGGER.info(f"Created S3 partition: {full_path}")
                
                # Add metadata
                element_with_path = dict(element)
                element_with_path["_partition_path"] = full_path
                yield element_with_path
                
            def _create_s3_directory(self, s3_path):
                if s3_path.startswith("s3://"):
                    s3_path = s3_path[5:]
                parts = s3_path.split("/", 1)
                bucket = parts[0]
                prefix = parts[1] if len(parts) > 1 else ""
                
                marker_key = f"{prefix}/_SUCCESS"
                self._s3_client.put_object(
                    Bucket=bucket,
                    Key=marker_key,
                    Body=json.dumps({
                        "created_at": beam.utils.timestamp.Timestamp.now().to_rfc3339(),
                        "pipeline": "ms_member_streaming"
                    }).encode('utf-8')
                )
        
        return (
            pcoll
            | f"{self.step_id}_Window" >> beam.WindowInto(
                beam.window.FixedWindows(window_size),
                trigger=beam.transforms.trigger.AfterWatermark(
                    early_firings=beam.transforms.trigger.AfterProcessingTime(60)
                ),
                accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING
            )
            | f"{self.step_id}_CreatePartition" >> beam.ParDo(
                CreatePartitionDoFn(s3_prefix, s3_region, aws_access_key, aws_secret_key)
            )
        )

# 3. MapRecordFixedStep - ใช้ fixed mapping
# class MapRecordFixedStep(BaseStep):
#     """Map with fixed mapping from config"""
    
#     def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
#         input_key = self.spec.get("in")
#         if not input_key or input_key not in self.state:
#             raise KeyError(f"Step {self.step_id}: missing input '{input_key}'")
            
#         pcoll = self.state[input_key]
#         mapping = self.config.streaming.fixed_mapping
        
#         def apply_fixed_mapping(record):
#             result = {}
#             for dest_field, config in mapping.items():
#                 src_path = config.get("src_path", [])
#                 if src_path:
#                     value = extract_by_path(record, src_path)
#                     result[dest_field] = value
#             return result
        
#         return pcoll | f"{self.step_id}_Map" >> beam.Map(apply_fixed_mapping)

# # 4. WriteParquetDynamicStep - เขียน Parquet ด้วย dynamic path
# class WriteParquetDynamicStep(BaseStep):
#     """Write Parquet using dynamic partition paths"""
    
#     def execute(self, pipeline: beam.Pipeline) -> None:
#         input_key = self.spec.get("in")
#         if not input_key or input_key not in self.state:
#             raise KeyError(f"Step {self.step_id}: missing input '{input_key}'")
            
#         pcoll = self.state[input_key]
        
#         class WriteDynamicParquet(beam.DoFn):
#             def __init__(self, schema_spec):
#                 self.schema_spec = schema_spec
#                 self._schema = None
                
#             def setup(self):
#                 from ..transforms.schema import load_schema_from_spec
#                 self._schema = load_schema_from_spec(self.schema_spec)
                
#             def process(self, element):
#                 import pyarrow as pa
#                 import pyarrow.parquet as pq
#                 from apache_beam.io.filesystems import FileSystems
#                 import uuid
                
#                 # Get partition path
#                 partition_path = element.get("_partition_path")
#                 if not partition_path:
#                     LOGGER.warning("No partition path found in element")
#                     return
                    
#                 # Clean element (remove metadata)
#                 clean_element = {
#                     k: v for k, v in element.items() 
#                     if not k.startswith("_")
#                 }
                
#                 # Write file
#                 filename = f"data_{uuid.uuid4().hex}.snappy.parquet"
#                 full_path = f"{partition_path}/{filename}"
                
#                 # Create PyArrow table
#                 table = pa.Table.from_pylist([clean_element], schema=self._schema)
                
#                 with FileSystems.create(full_path) as f:
#                     pq.write_table(table, f, compression='snappy')
                    
#                 LOGGER.debug(f"Wrote record to {full_path}")
#                 yield {"path": full_path, "success": True}
        
#         return pcoll | f"{self.step_id}_Write" >> beam.ParDo(
#             WriteDynamicParquet(self.config.schema)
#         )