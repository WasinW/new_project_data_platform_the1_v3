"""
Generic connector classes for external systems.

The connectors defined in this module encapsulate interactions with
external systems such as BigQuery or cloud storage.  They are kept
light‑weight and generic so that Beam steps can use them without
embedding any table‑specific details.  Additional connectors can
easily be added by defining a new class with static or class
methods to perform reads/writes.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.parquetio import WriteToParquet

from ..config import PipelineConfig
from ..transforms.schema import load_schema_from_spec

from .bigtable import BigTableConnector
from .pubsub import PubSubConnector

LOGGER = logging.getLogger(__name__)


class BigQueryConnector:
    """Connector for reading data from BigQuery.

    This connector exposes static methods that wrap Beam's built‑in
    BigQuery I/O transforms.  It accepts a SQL query or table
    reference and passes through configuration parameters from the
    pipeline config (e.g. project, temp GCS location).  Additional
    arguments may be added in the future to support more advanced
    options.
    """

    @staticmethod
    def read_query(pipeline: beam.Pipeline, query: str, cfg: PipelineConfig, label: str = "ReadBQQuery") -> beam.PCollection:
        """Read a BigQuery SQL query and return a :class:`PCollection`.

        Parameters
        ----------
        pipeline: :class:`~apache_beam.Pipeline`
            The pipeline into which the read transform will be inserted.
        query: str
            The SQL query to execute.  Must be a valid Standard SQL
            query (``use_standard_sql=True`` is set internally).
        cfg: :class:`PipelineConfig`
            The global pipeline configuration from which BigQuery
            options (project, temp GCS location) will be extracted.

        Returns
        -------
        beam.PCollection
            A collection of dictionaries representing the rows
            returned by the query.
        """
        bq_cfg = cfg.io.bq or {}
        project = bq_cfg.get("project")
        temp_gcs = bq_cfg.get("temp_gcs")
        LOGGER.info("Reading BigQuery query with label %s: %s", label, query)
        # LOGGER.info("Reading BigQuery query: %s", query)
        return pipeline | label >> ReadFromBigQuery(
            query=query,
            use_standard_sql=True,
            project=project,
            gcs_location=temp_gcs,
        )


class ParquetConnector:
    """Connector for writing data to Parquet files on cloud storage.

    This connector wraps Beam's :class:`WriteToParquet` transform.
    It uses the schema specified in the pipeline configuration to
    ensure output files are written with the correct data types.
    """

    @staticmethod
    # def write(pcoll: beam.PCollection, prefix: str, cfg: PipelineConfig) -> None:
    def write(pcoll: beam.PCollection, prefix: str, cfg: PipelineConfig, label: str = "WriteParquet") -> None:
        """Write the given PCollection of dictionaries to Parquet.

        Parameters
        ----------
        pcoll: beam.PCollection
            The collection of dictionaries to write.
        prefix: str
            The file path prefix (without extension) for the
            resulting Parquet files.  Beam will append shard
            identifiers and a file extension (``.snappy.parquet``).
        cfg: :class:`PipelineConfig`
            The pipeline configuration used to load the schema.
        """
        schema = load_schema_from_spec(cfg.schema)
        LOGGER.info("Writing Parquet files with label %s to prefix: %s", label, prefix)
        # LOGGER.info("Writing Parquet files to prefix: %s", prefix)
        pcoll | label >> WriteToParquet(
            file_path_prefix=prefix,
            schema=schema,
            file_name_suffix=".snappy.parquet",
            num_shards=2,
        )


__all__ = ["BigQueryConnector", "ParquetConnector" , "PubSubConnector", "BigTableConnector"]