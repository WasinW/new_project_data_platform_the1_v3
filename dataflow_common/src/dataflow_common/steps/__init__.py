"""
Generic Beam pipeline steps for dataflow_common.
"""
# dataflow_common/src/dataflow_common/steps/__init__.py
from __future__ import annotations  # ต้องมาก่อน

import logging
import json
from typing import Any, Dict, Iterable, List, Optional
import traceback

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery

from dataflow_common.config import PipelineConfig
from dataflow_common.core import BaseStep
from dataflow_common.connectors import BigQueryConnector, ParquetConnector, GCSFilesStorage
from dataflow_common.transforms import (
    create_mapping_dict,
    map_record,
    coalesce_by_mapping,
    normalize_row_to_schema,
    load_schema_from_spec,
)
# from dataflow_common.utils.logging import logger
from dataflow_common.utils import get_dataflow_logger
# import logging
logger = get_dataflow_logger(__name__)

# LOGGER = logging.getLogger(__name__)

# Import streaming steps
from dataflow_common.steps.streaming import (
    ProcessWithDLQStep,
    WindowStep,
    WriteToBigQueryStep as StreamingWriteToBigQueryStep,
    CreateFixedMappingStep,
    CreateEmptyStep,
)

# Import จาก pubsub_bigtable_steps.py
from dataflow_common.steps.pubsub_bigtable_steps import (
    ConsumePubSubSubscriptionStep,
    ExtractIdStep,
    ReadBigTableByIdStep,
)

# Import จาก streaming_additions.py (ที่ไม่ถูก comment)
from dataflow_common.steps.streaming_additions import (
    WindowingAuditStep,
    WindowingOpenHourlyPartitionStep,
    WriteParquetDynamicStep,  # ต้อง uncomment ใน streaming_additions.py ก่อน
    # MapRecordFixedStep,
)

# Import จาก streaming_midterm.py  
from dataflow_common.steps.streaming_midterm import (
    ConsumeMessagesWithDLQStep,
    ParseNestedJsonStep,
    WindowedMappingQueryStep,
    EnhancedWriteToBigQueryStep,
)

# BaseStep is now defined in dataflow_common.core and imported above.

class ReadBQQueryStep(BaseStep):
    """Read a BigQuery SQL query into a PCollection of dictionaries."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_dataflow_logger(f"{__name__}.{self.step_id}")

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            query: str = self.spec.get("query", "")
            if not query:
                raise ValueError(f"Step {self.step_id}: 'query' must be provided for ReadBQQuery")
            
            # Log query for debugging
            logger.info(self.step_id,f"Executing BigQuery query (first 500 chars): {query[:500]}...")
            
            if "{" in query:
                raise RuntimeError(f"Unresolved template in query for {self.step_id}: {query}")
            
            result = BigQueryConnector.read_query(pipeline, query, self.config, self.step_id)
            logger.info(self.step_id,f"Query executed successfully")
            return result
            
        except Exception as e:
            logger.error(self.step_id,f"Failed to execute ReadBQQuery")
            logger.error(self.step_id,f"Error: {str(e)}")
            logger.error(self.step_id,f"Full query: {query if 'query' in locals() else 'Query not available'}")
            logger.error(self.step_id,f"Stack trace: {traceback.format_exc()}")
            raise
        # return BigQueryConnector.read_query(pipeline, query, self.config, self.step_id)

class BuildMappingDictStep(BaseStep):
    """Build a mapping dictionary from mapping rows.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_dataflow_logger(f"{__name__}.{self.step_id}")

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            input_key = self.spec.get("in")
            logger.info(self.step_id,f"Building mapping dict from input: {input_key}")
            
            if not input_key or input_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
            
            pcoll = self.state[input_key]
            fields = self.spec.get("mapping_fields", {})
            
            src_field = fields.get("src_field", "src_column_name")
            dest_field = fields.get("dest_field", "dest_column_name")
            retrieved_flag = fields.get("retrieved_flag_field", "retrieved_flag")
            confirmed_flag = fields.get("confirmed_flag_field", "confirmed_flag")
            
            logger.info(self.step_id,f"Mapping fields - src: {src_field}, dest: {dest_field}")
            
            result = (
                pcoll
                | f"{self.step_id}_ToList" >> beam.combiners.ToList()
                | f"{self.step_id}_BuildDict" >> beam.Map(
                    create_mapping_dict,
                    src_field=src_field,
                    dest_field=dest_field,
                    retrieved_flag_field=retrieved_flag,
                    confirmed_flag_field=confirmed_flag,
                )
            )
            logger.info(self.step_id,f"Mapping dict built successfully")
            return result
            
        except Exception as e:
            logger.error(self.step_id,f"Failed to build mapping dict")
            logger.error(self.step_id,f"Error: {str(e)}")
            logger.error(self.step_id,f"Stack trace: {traceback.format_exc()}")
            raise

class ParseJsonStep(BaseStep):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_dataflow_logger(f"{__name__}.{self.step_id}")

    def execute(self, pipeline):
        try:
            input_key = self.spec.get("in")
            json_fields = self.spec.get("json_fields", ["profiles"])
            
            logger.info(self.step_id,f"Parsing JSON fields: {json_fields} from input: {input_key}")
            
            pcoll = self.state[input_key]
            
            def parse_json_fields(record):
                try:
                    rec = dict(record)
                    for field in json_fields:
                        if field in rec and isinstance(rec[field], str):
                            try:
                                rec[field] = json.loads(rec[field])
                                logger.debug(self.step_id,f"Successfully parsed JSON field: {field}")
                            except json.JSONDecodeError as je:
                                logger.warning(self.step_id,f"Failed to parse JSON field '{field}': {je}")
                    return rec
                except Exception as e:
                    logger.error(self.step_id,f"Error in parse_json_fields: {e}")
                    raise
            
            result = pcoll | f"{self.step_id}_Parse" >> beam.Map(parse_json_fields)
            logger.info(self.step_id,f"JSON parsing completed")
            return result
            
        except Exception as e:
            logger.error(self.step_id,f"Failed in ParseJsonStep")
            logger.error(self.step_id,f"Error: {str(e)}")
            logger.error(self.step_id,f"Stack trace: {traceback.format_exc()}")
            raise
    
class MapRecordStep(BaseStep):
    """Apply a mapping dictionary to each record in the input PCollection."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_dataflow_logger(f"{__name__}.{self.step_id}")

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            input_key = self.spec.get("in")
            side_key = self.spec.get("side")
            mode: str = self.spec.get("mode", "reconcile")
            
            logger.info(self.step_id,f"Mapping records - mode: {mode}, input: {input_key}, side: {side_key}")
            
            if not input_key or input_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
            if not side_key or side_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown side input '{side_key}'")
            
            pcoll = self.state[input_key]
            mapping_pcoll = self.state[side_key]
            mapping_side = beam.pvalue.AsSingleton(mapping_pcoll)
            
            result = pcoll | f"{self.step_id}_MapRecord" >> beam.Map(
                lambda rec, m: map_record(rec, m, mode), mapping_side
            )
            logger.info(self.step_id,f"Record mapping completed")
            return result
            
        except Exception as e:
            logger.error(self.step_id,f"Failed in MapRecordStep")
            logger.error(self.step_id,f"Error: {str(e)}")
            logger.error(self.step_id,f"Stack trace: {traceback.format_exc()}")
            raise

class KVPairsStep(BaseStep):
    # map key with id , map value with record
    """Convert records into key/value pairs keyed by the specified field."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_dataflow_logger(f"{__name__}.{self.step_id}")

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            input_key = self.spec.get("in")
            key_field = self.spec.get("key_field") or self.config.params.pk
            
            logger.info(self.step_id,f"Creating KV pairs - key_field: {key_field}, input: {input_key}")
            
            if not input_key or input_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
            
            pcoll = self.state[input_key]
            
            def safe_get_key(d):
                try:
                    return (d.get(key_field), d)
                except Exception as e:
                    logger.warning(self.step_id,f"Failed to get key '{key_field}' from record: {e}")
                    return (None, d)
            
            result = (
                pcoll
                | f"{self.step_id}_KV" >> beam.Map(safe_get_key)
                | f"{self.step_id}_DropNoneKey" >> beam.Filter(lambda kv: kv[0] is not None)
            )
            logger.info(self.step_id,f"KV pairs created successfully")
            return result
            
        except Exception as e:
            logger.error(self.step_id,f"Failed in KVPairsStep")
            logger.error(self.step_id,f"Error: {str(e)}")
            logger.error(self.step_id,f"Stack trace: {traceback.format_exc()}")
            raise

class CoGroupByKeyStep(BaseStep):
    """Group multiple keyed PCollections by key."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_dataflow_logger(f"{__name__}.{self.step_id}")

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            alias_mapping: Dict[str, str] = self.spec.get("as") or {}
            
            logger.info(self.step_id,f"CoGroupByKey with aliases: {list(alias_mapping.keys())}")
            
            if not alias_mapping:
                raise ValueError(f"Step {self.step_id}: 'as' mapping must be provided for CoGroupByKey")
            
            inputs: Dict[str, beam.PCollection] = {}
            for alias, state_key in alias_mapping.items():
                if state_key not in self.state:
                    raise KeyError(f"Step {self.step_id}: unknown input '{state_key}' for alias '{alias}'")
                inputs[alias] = self.state[state_key]
                logger.info(self.step_id,f"Added input '{state_key}' as alias '{alias}'")
            
            result = inputs | f"{self.step_id}_CoGroupByKey" >> beam.CoGroupByKey()
            logger.info(self.step_id,f"CoGroupByKey completed")
            return result
            
        except Exception as e:
            logger.error(self.step_id,f"Failed in CoGroupByKeyStep")
            logger.error(self.step_id,f"Error: {str(e)}")
            logger.error(self.step_id,f"Stack trace: {traceback.format_exc()}")
            raise

class CoalesceByMappingStep(BaseStep):
    """Coalesce new and old records using mapping flags."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_dataflow_logger(f"{__name__}.{self.step_id}")

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            input_key = self.spec.get("in")
            side_key = self.spec.get("side")
            flag_field = self.spec.get("flag_field")
            
            logger.info(self.step_id,f"Coalescing - input: {input_key}, side: {side_key}, flag: {flag_field}")
            
            if not input_key or input_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
            if not side_key or side_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown side input '{side_key}'")
            if not flag_field:
                raise ValueError(f"Step {self.step_id}: 'flag_field' must be provided for CoalesceByMapping")
            
            pcoll = self.state[input_key]
            mapping_rows_pcoll = self.state[side_key]
            columns_side = beam.pvalue.AsList(mapping_rows_pcoll)
            pk_field = self.config.params.pk
            dest_field = self.spec.get("dest_field") or "dest_column_name"

            result = (pcoll 
                | f"{self.step_id}_Coalesce" >> beam.Map(
                    coalesce_by_mapping,
                    columns=columns_side,
                    flag_field=flag_field,
                    pk_field=pk_field,
                    dest_field=dest_field,
                )
                | f"{self.step_id}_FilterNone" >> beam.Filter(lambda x: x is not None)
            )
            logger.info(self.step_id,f"Coalescing completed")
            return result
            
        except Exception as e:
            logger.error(self.step_id,f"Failed in CoalesceByMappingStep")
            logger.error(self.step_id,f"Error: {str(e)}")
            logger.error(self.step_id,f"Stack trace: {traceback.format_exc()}")
            raise

class NormalizeToSchemaStep(BaseStep):
    """Normalise rows to the loaded schema using the configured formats."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_dataflow_logger(f"{__name__}.{self.step_id}")

    _schema_cache: Optional[Any] = None

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            input_key = self.spec.get("in")
            
            logger.info(self.step_id,f"Normalizing to schema - input: {input_key}")
            
            if not input_key or input_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
            
            pcoll = self.state[input_key]
            
            if NormalizeToSchemaStep._schema_cache is None:
                NormalizeToSchemaStep._schema_cache = load_schema_from_spec(self.config.schema)
                logger.info(self.step_id,f"Schema loaded with {len(NormalizeToSchemaStep._schema_cache.names)} fields")
            
            schema = NormalizeToSchemaStep._schema_cache
            formats = self.config.formats
            
            def safe_normalize(row):
                try:
                    return normalize_row_to_schema(row, schema, formats)
                except Exception as e:
                    logger.error(self.step_id,f"Failed to normalize row: {e}")
                    logger.debug(self.step_id,f"Problematic row: {row}")
                    raise
            
            result = pcoll | f"{self.step_id}_Normalize" >> beam.Map(safe_normalize)
            logger.info(self.step_id,f"Normalization completed")
            return result
            
        except Exception as e:
            logger.error(self.step_id,f"Failed in NormalizeToSchemaStep")
            logger.error(self.step_id,f"Error: {str(e)}")
            logger.error(self.step_id,f"Stack trace: {traceback.format_exc()}")
            raise

class WriteParquetStep(BaseStep):
    """Write a PCollection of dictionaries to Parquet files."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_dataflow_logger(f"{__name__}.{self.step_id}")

    def execute(self, pipeline: beam.Pipeline) -> None:
        try:
            input_key = self.spec.get("in")
            prefix_template: str = self.spec.get("prefix") or ""
            
            logger.info(self.step_id,f"Writing Parquet - input: {input_key}, prefix: {prefix_template[:100]}...")
            
            if not input_key or input_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
            if not prefix_template:
                raise ValueError(f"Step {self.step_id}: 'prefix' must be provided for WriteParquet")
            
            pcoll = self.state[input_key]
            refined_prefix = self.config.io.s3.get("refined_prefix")
            run_dt = self.config.params.run_dt

            format_dict = {}
            format_dict.update(self.config.io.s3)
            format_dict.update(self.config.params.__dict__)
            
            if run_dt:
                format_dict['run_dt'] = run_dt

            try:
                prefix = prefix_template.format(**format_dict)
                logger.info(self.step_id,f"Final Parquet path: {prefix}")
            except Exception as exc:
                raise RuntimeError(f"Failed to format prefix '{prefix_template}': {exc}")
            
            output_key = self.spec.get("out") or self.spec.get("in")
            label = f"WriteParquet_{output_key}"
            ParquetConnector.write(pcoll, prefix, self.config, label)
            
            logger.info(self.step_id,f"Parquet write initiated")
            return None
            
        except Exception as e:
            logger.error(self.step_id,f"Failed in WriteParquetStep")
            logger.error(self.step_id,f"Error: {str(e)}")
            logger.error(self.step_id,f"Stack trace: {traceback.format_exc()}")
            raise

class WriteToBigQueryStep(BaseStep):
    """Write to BigQuery table"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_dataflow_logger(f"{__name__}.{self.step_id}")

    def execute(self, pipeline: beam.Pipeline) -> None:
        try:
            input_key = self.spec.get("in")
            table = self.spec.get("table")
            
            logger.info(self.step_id,f"Writing to BigQuery - input: {input_key}, table: {table}")
            
            if not input_key or input_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing input '{input_key}'")
            if not table:
                raise ValueError(f"Step {self.step_id}: 'table' must be provided")
            
            pcoll = self.state[input_key]
            
            write_disposition = self.spec.get("write_disposition", "WRITE_APPEND")
            create_disposition = self.spec.get("create_disposition", "CREATE_IF_NEEDED")
            schema = self.spec.get("schema", "SCHEMA_AUTODETECT")
            
            pcoll | f"{self.step_id}_WriteBQ" >> WriteToBigQuery(
                table=table,
                write_disposition=write_disposition,
                create_disposition=create_disposition,
                schema=schema
            )
            
            logger.info(self.step_id,f"BigQuery write initiated")
            return None
            
        except Exception as e:
            logger.error(self.step_id,f"Failed in WriteToBigQueryStep")
            logger.error(self.step_id,f"Error: {str(e)}")
            logger.error(self.step_id,f"Stack trace: {traceback.format_exc()}")
            raise

# class ReadGCSStep(BaseStep):
#    def __init__(self, *args, **kwargs):
#        super().__init__(*args, **kwargs)
#        # Logger เฉพาะสำหรับ step นี้
#        self.logger = get_dataflow_logger(f"{__name__}.{self.step_id}")
#     def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
#         # Determine the path and format from the step specification.
#         path = self.spec.get("path") or self.spec.get("gcs_path")
#         if not path:
#             raise ValueError(f"ReadGCS step '{self.step_id}' requires a 'path' parameter")
#         fmt = (self.spec.get("format") or "text").lower()
#         if fmt not in {"text", "json"}:
#             raise ValueError(f"Unsupported format '{fmt}' in ReadGCS step '{self.step_id}'")

#         # Read the file as text lines.
#         pcoll = pipeline | self.step_id >> beam.io.ReadFromText(path)

#         # Optionally parse JSON lines.
#         if fmt == "json":
#             pcoll = pcoll | f"{self.step_id}_ParseJson" >> beam.Map(json.loads)
#         return pcoll


class WriteGCSStep(BaseStep):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_dataflow_logger(f"{__name__}.{self.step_id}")

    def execute(self, pipeline: beam.Pipeline) -> None:
        try:
            input_key: Optional[str] = self.spec.get("in") or self.spec.get("id")
            path = self.spec.get("path") or self.spec.get("gcs_path")
            fmt = (self.spec.get("format") or "text").lower()
            
            logger.info(self.step_id,f"Writing to GCS - input: {input_key}, path: {path}, format: {fmt}")
            
            if not input_key:
                raise ValueError(f"WriteGCS step '{self.step_id}' requires an 'in' parameter")
            if not path:
                raise ValueError(f"WriteGCS step '{self.step_id}' requires a 'path' parameter")
            if fmt not in {"text", "json"}:
                raise ValueError(f"Unsupported format '{fmt}' in WriteGCS step '{self.step_id}'")

            if input_key not in self.state:
                raise KeyError(f"WriteGCS step '{self.step_id}' could not find input key '{input_key}' in state")
            
            pcoll = self.state[input_key]
            if pcoll is None:
                logger.warning(self.step_id,f"No data to write")
                return None
            
            if fmt == "json":
                pcoll = pcoll | f"{self.step_id}_SerializeJson" >> beam.Map(json.dumps)
            else:
                pcoll = pcoll | f"{self.step_id}_ToString" >> beam.Map(lambda x: str(x))
            
            pcoll | self.step_id >> beam.io.WriteToText(path, shard_name_template="")
            
            logger.info(self.step_id,f"GCS write initiated")
            return None
            
        except Exception as e:
            logger.error(self.step_id,f"Failed in WriteGCSStep")
            logger.error(self.step_id,f"Error: {str(e)}")
            logger.error(self.step_id,f"Stack trace: {traceback.format_exc()}")
            raise

# class GetNewMaxDateStep(BaseStep):
#     def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
#         try:
#             input_key: Optional[str] = self.spec.get("in")
#             field_name = self.spec.get("field", "UPDATED_DATE")
#             max_date_step = self.spec.get("max_date_step", "")
            
#             logger.info(self.step_id,f"Getting max date - input: {input_key}, field: {field_name}")
            
#             if not input_key:
#                 raise ValueError(f"GetNewMaxDate step '{self.step_id}' requires an 'in' parameter")
#             if input_key not in self.state:
#                 raise KeyError(f"GetNewMaxDate step '{self.step_id}' could not find input key '{input_key}' in state")
            
#             pcoll = self.state[input_key]
            
#             if max_date_step:
#                 extract_label = f"{self.step_id}_{max_date_step}_ExtractField"
#                 filter_label = f"{self.step_id}_{max_date_step}_FilterNone"
#                 max_label = f"{self.step_id}_{max_date_step}_Max"
#             else:
#                 extract_label = f"{self.step_id}_ExtractField"
#                 filter_label = f"{self.step_id}_FilterNone"
#                 max_label = f"{self.step_id}_Max"
            
#             def extract_with_debug(rec):
#                 try:
#                     if rec:
#                         logger.debug(self.step_id,f"Available fields: {list(rec.keys())}")
#                         value = rec.get(field_name)
#                         logger.debug(self.step_id,f"{field_name} value: {value}")
#                         return value
#                 except Exception as e:
#                     logger.error(self.step_id,f"Error extracting field '{field_name}': {e}")
#                 return None

#             dates = pcoll | extract_label >> beam.Map(extract_with_debug)
#             dates_filtered = dates | filter_label >> beam.Filter(lambda x: x is not None)

#             def safe_max(vals):
#                 filtered = [v for v in vals if v is not None]
#                 result = max(filtered) if filtered else None
#                 logger.info(self.step_id,f"Max date found: {result}")
#                 return result
            
#             max_date = dates_filtered | max_label >> beam.CombineGlobally(safe_max)
            
#             logger.info(self.step_id,f"Max date extraction completed")
#             return max_date
            
#         except Exception as e:
#             logger.error(self.step_id,f"Failed in GetNewMaxDateStep")
#             logger.error(self.step_id,f"Error: {str(e)}")
#             logger.error(self.step_id,f"Stack trace: {traceback.format_exc()}")
#             raise
        
# class SetMaxDateParamStep(BaseStep):
#     """Set max_date from PCollection to params"""
    
#     def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
#         input_key = self.spec.get("in")
#         if not input_key or input_key not in self.state:
#             raise KeyError(f"Step {self.step_id}: missing input '{input_key}'")
        
#         pcoll = self.state[input_key]
        
#         # Extract single value and set to params
#         def set_param(value):
#             if value:
#                 self.config.params.max_date = str(value).strip()
#             return value
        
#         return pcoll | f"{self.step_id}_SetParam" >> beam.Map(set_param)

__all__ = [
    "BaseStep",
    "ReadBQQueryStep",
    "BuildMappingDictStep",
    "ParseJsonStep",
    "MapRecordStep",
    "KVPairsStep",
    "CoGroupByKeyStep",
    "CoalesceByMappingStep",
    "NormalizeToSchemaStep",
    "WriteParquetStep",
    "WriteToBigQueryStep",
    "WriteGCSStep",
    # "GetNewMaxDateStep",
    # Streaming steps
    "ProcessWithDLQStep",
    "WindowStep",
    "CreateFixedMappingStep",
    "CreateEmptyStep",
    # "ReadGCSStep",
    # "SetMaxDateParamStep",
    # Pub/Sub & BigTable steps
    "ConsumePubSubSubscriptionStep",
    "ExtractIdStep",
    "ReadBigTableByIdStep",
    # Streaming additions
    "WindowingAuditStep",
    "WindowingOpenHourlyPartitionStep",
    "WriteParquetDynamicStep",
    # "MapRecordFixedStep",
    # Mid-term streaming steps
    "ConsumeMessagesWithDLQStep",
    "ParseNestedJsonStep",
    "WindowedMappingQueryStep",
    "EnhancedWriteToBigQueryStep",

]