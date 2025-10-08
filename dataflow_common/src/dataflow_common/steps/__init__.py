"""
Generic Beam pipeline steps for dataflow_common.

Each step class in this module implements the :class:`BaseStep`
interface.  Instances are created by the orchestrator with the
corresponding plan specification, the global pipeline configuration
and the current state dictionary.  Steps should not directly
reference table‑specific information; such details must be supplied
via the configuration.
"""
from apache_beam.io.gcp.bigquery import WriteToBigQuery

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional

import apache_beam as beam
# Note: BigQuery and Parquet I/O are accessed via connectors rather
# than imported directly here.  This avoids duplicating project
# configuration logic in each step.

from ..config import PipelineConfig
from ..core import BaseStep
from ..connectors import BigQueryConnector, ParquetConnector
from ..transforms import (
    create_mapping_dict,
    map_record,
    coalesce_by_mapping,
    normalize_row_to_schema,
    load_schema_from_spec,
)
from .streaming import (
    ConsumePubSubStep,
    ExtractKeysStep,
    ReadBigTableStep, 
    ProcessWithDLQStep,
    WindowStep
)

LOGGER = logging.getLogger(__name__)


# BaseStep is now defined in dataflow_common.core and imported above.


class ReadBQQueryStep(BaseStep):
    """Read a BigQuery SQL query into a PCollection of dictionaries."""

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        query: str = self.spec.get("query", "")
        if not query:
            raise ValueError(f"Step {self.step_id}: 'query' must be provided for ReadBQQuery")
        # Validate that the query has been fully formatted by the
        # orchestrator; a stray "{" implies a missing placeholder.
        if "{" in query:
            raise RuntimeError(f"Unresolved template in query for {self.step_id}: {query}")
        # Delegate to the BigQuery connector to perform the read.  The
        # connector handles project and temp GCS options from the
        # config.
        return BigQueryConnector.read_query(pipeline, query, self.config, self.step_id)

class BuildMappingDictStep(BaseStep):
    """Build a mapping dictionary from mapping rows.

    The step collects all rows from its input PCollection into a list
    and then invokes :func:`create_mapping_dict` with field names
    taken from the ``mapping_fields`` dictionary in the spec.  The
    result is emitted as a PCollection containing a single dictionary
    element; later steps can use this PCollection as a side input.
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
        pcoll = self.state[input_key]
        fields = self.spec.get("mapping_fields", {})
        # Provide generic default field names; these can be overridden
        # via the mapping_fields section in the YAML plan.  No
        # table‑specific names remain here.
        src_field = fields.get("src_field", "src_column_name")
        dest_field = fields.get("dest_field", "dest_column_name")
        retrieved_flag = fields.get("retrieved_flag_field", "retrieved_flag")
        confirmed_flag = fields.get("confirmed_flag_field", "confirmed_flag")
        # Gather all mapping rows into a list then build the dictionary
        return (
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


class MapRecordStep(BaseStep):
    """Apply a mapping dictionary to each record in the input PCollection."""

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("in")
        side_key = self.spec.get("side")
        mode: str = self.spec.get("mode", "reconcile")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
        if not side_key or side_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown side input '{side_key}'")
        pcoll = self.state[input_key]
        mapping_pcoll = self.state[side_key]
        mapping_side = beam.pvalue.AsSingleton(mapping_pcoll)
        LOGGER.info(
            "[%s] Mapping records with mode '%s' using mapping dict from '%s'", self.step_id, mode, side_key
        )
        return pcoll | f"{self.step_id}_MapRecord" >> beam.Map(
            lambda rec, m: map_record(rec, m, mode), mapping_side
        )


class KVPairsStep(BaseStep):
    # check key ที่ join เจอ 
    """Convert records into key/value pairs keyed by the specified field."""

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("in")
        key_field = self.spec.get("key_field") or self.config.params.pk
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
        pcoll = self.state[input_key]
        return (
            pcoll
            | f"{self.step_id}_KV" >> beam.Map(lambda d: (d.get(key_field), d))
            | f"{self.step_id}_DropNoneKey" >> beam.Filter(lambda kv: kv[0] is not None)
        )


class CoGroupByKeyStep(BaseStep):
    """Group multiple keyed PCollections by key.

    The step specification must provide an ``as`` dictionary mapping
    alias names (e.g. ``new`` and ``old``) to the keys of the
    PCollections in the state.  The output PCollection is the result
    of ``beam.CoGroupByKey`` on the inputs.
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        alias_mapping: Dict[str, str] = self.spec.get("as") or {}
        if not alias_mapping:
            raise ValueError(f"Step {self.step_id}: 'as' mapping must be provided for CoGroupByKey")
        inputs: Dict[str, beam.PCollection] = {}
        for alias, state_key in alias_mapping.items():
            if state_key not in self.state:
                raise KeyError(f"Step {self.step_id}: unknown input '{state_key}' for alias '{alias}'")
            inputs[alias] = self.state[state_key]
        LOGGER.info("[%s] Grouping keys for aliases: %s", self.step_id, list(alias_mapping.keys()))
        return inputs | f"{self.step_id}_CoGroupByKey" >> beam.CoGroupByKey()


class CoalesceByMappingStep(BaseStep):
    """Coalesce new and old records using mapping flags."""

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("in")
        side_key = self.spec.get("side")
        flag_field = self.spec.get("flag_field")
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
        return pcoll | f"{self.step_id}_Coalesce" >> beam.Map(
            coalesce_by_mapping,
            columns=columns_side,
            flag_field=flag_field,
            pk_field=pk_field,
            dest_field=dest_field,
        )


class NormalizeToSchemaStep(BaseStep):
    """Normalise rows to the loaded schema using the configured formats."""

    _schema_cache: Optional[Any] = None

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
        pcoll = self.state[input_key]
        # Load or reuse schema
        if NormalizeToSchemaStep._schema_cache is None:
            NormalizeToSchemaStep._schema_cache = load_schema_from_spec(self.config.schema)
        schema = NormalizeToSchemaStep._schema_cache
        formats = self.config.formats
        LOGGER.info("[%s] Normalising rows to schema with %d fields", self.step_id, len(schema.names))
        return pcoll | f"{self.step_id}_Normalize" >> beam.Map(
            lambda row: normalize_row_to_schema(row, schema, formats)
        )


class WriteParquetStep(BaseStep):
    """Write a PCollection of dictionaries to Parquet files."""

    def execute(self, pipeline: beam.Pipeline) -> None:
        input_key = self.spec.get("in")
        prefix_template: str = self.spec.get("prefix") or ""
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
        if not prefix_template:
            raise ValueError(f"Step {self.step_id}: 'prefix' must be provided for WriteParquet")
        pcoll = self.state[input_key]
        # Build prefix from template and global config
        refined_prefix = self.config.io.s3.get("refined_prefix")
        run_dt = self.config.params.run_dt

        # Create format dict without duplicates
        format_dict = {}
        format_dict.update(self.config.io.s3)
        format_dict.update(self.config.params.__dict__)
        # Override run_dt if provided
        if run_dt:
            format_dict['run_dt'] = run_dt

        try:
            # prefix = prefix_template.format(
            #     refined_prefix=refined_prefix,
            #     run_dt=run_dt,
            #     **self.config.io.s3,
            #     **self.config.params.__dict__,
            # )
            prefix = prefix_template.format(**format_dict)
        except Exception as exc:
            raise RuntimeError(f"Failed to format prefix '{prefix_template}': {exc}")
        # Use the ParquetConnector to write files.  This delegates
        # schema loading and other options to a single place.
        output_key = self.spec.get("out") or self.spec.get("in")
        label = f"WriteParquet_{output_key}"
        ParquetConnector.write(pcoll, prefix, self.config, label)
        return None

class WriteToBigQueryStep(BaseStep):
    """Write to BigQuery table"""
    
    def execute(self, pipeline: beam.Pipeline) -> None:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing input '{input_key}'")
        
        pcoll = self.state[input_key]
        
        # Get table reference
        table = self.spec.get("table")
        if not table:
            raise ValueError(f"Step {self.step_id}: 'table' must be provided")
        
        # BQ write options
        write_disposition = self.spec.get("write_disposition", "WRITE_APPEND")
        create_disposition = self.spec.get("create_disposition", "CREATE_IF_NEEDED")
        
        # Schema can be auto-detected or provided
        schema = self.spec.get("schema", "SCHEMA_AUTODETECT")
        
        pcoll | f"{self.step_id}_WriteBQ" >> WriteToBigQuery(
            table=table,
            write_disposition=write_disposition,
            create_disposition=create_disposition,
            schema=schema
        )
        
        return None
    
__all__ = [
    "BaseStep",
    "ReadBQQueryStep",
    "BuildMappingDictStep",
    "MapRecordStep",
    "KVPairsStep",
    "CoGroupByKeyStep",
    "CoalesceByMappingStep",
    "NormalizeToSchemaStep",
    "WriteParquetStep",
    "ConsumePubSubStep",
    "ExtractKeysStep", 
    "ReadBigTableStep",
    "ProcessWithDLQStep",
    "WindowStep",
    "WriteToBigQueryStep",
]