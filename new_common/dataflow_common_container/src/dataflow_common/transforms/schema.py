"""
Schema loading and normalisation utilities for the dataflow_common package.

This module contains functions to load a schema from either a JSON
file on GCS or a BigQuery table/query definition and convert it into
a :class:`pyarrow.Schema`.  It also provides helper functions to
normalise individual rows to the target schema using configurable
date and timestamp parsing formats.

The schema JSON format should be a list of objects with at least
``name`` and ``type`` fields, compatible with the format returned by
the BigQuery API.  Nested records are not supported in this example
implementation; such use cases would require additional logic.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import pyarrow as pa

from apache_beam.io.filesystems import FileSystems

from ..config import SchemaSpec, BigQuerySchemaSpec, FormatSpec

LOGGER = logging.getLogger(__name__)


def load_schema_from_spec(spec: Optional[SchemaSpec]) -> pa.Schema:
    """Load a PyArrow schema from a :class:`SchemaSpec`.

    If ``spec`` is ``None`` a default schema consisting of a single
    ``pk`` string column will be returned.  If ``gcs_uri`` is
    provided the JSON file will be loaded from GCS.  Otherwise if
    ``bq`` is provided the schema will be retrieved from BigQuery via
    the Python client library.  Errors during loading will be
    propagated to the caller.
    """
    if spec is None:
        # Fallback schema: a single string column named "pk"
        return pa.schema([pa.field("pk", pa.string())])
    if spec.gcs_uri:
        return _load_schema_from_gcs(spec.gcs_uri)
    if spec.bq:
        return _load_schema_from_bq(spec.bq)
    # No schema specified; return fallback
    return pa.schema([pa.field("pk", pa.string())])


def _load_schema_from_gcs(uri: str) -> pa.Schema:
    """Load a schema from a JSON file on GCS using Beam's FileSystems API."""
    with FileSystems.open(uri) as f:
        content = f.read().decode("utf-8")
    schema_def = json.loads(content)
    return build_pyarrow_schema(schema_def)


def _load_schema_from_bq(spec: BigQuerySchemaSpec) -> pa.Schema:
    """Load a schema from a BigQuery table or query.

    Uses the google.cloud.bigquery client library if available.  For
    queries, a dry run is executed to extract the schema without
    reading any rows.
    """
    try:
        from google.cloud import bigquery  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "google-cloud-bigquery is required to load schema from BigQuery"
        ) from exc
    client = bigquery.Client(project=spec.project)
    if spec.query:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = client.query(spec.query, job_config=job_config)
        bq_schema = query_job.schema
    elif spec.table:
        table_ref = f"{spec.project}.{spec.dataset}.{spec.table}"
        table = client.get_table(table_ref)
        bq_schema = table.schema
    else:
        raise ValueError("BigQuery schema spec must provide either a table or a query")
    # Convert BigQuery schema into JSON-like dict format
    json_schema = [
        {"name": field.name, "type": field.field_type.upper()} for field in bq_schema
    ]
    return build_pyarrow_schema(json_schema)


def build_pyarrow_schema(schema_def: List[Dict[str, Any]]) -> pa.Schema:
    """Construct a PyArrow schema from a JSON-like definition.

    The input should be a list of dictionaries each containing
    ``name`` and ``type`` keys.  Supported types are BigQuery
    primitive types (``STRING``, ``INT64``, ``FLOAT64``, ``NUMERIC``,
    ``BOOLEAN``, ``DATE``, ``TIMESTAMP``).  Unrecognised types are
    mapped to strings.
    """
    fields = []
    for col in schema_def:
        name = col.get("name")
        typ = (col.get("type") or "STRING").upper()
        if typ in ("INT64", "INTEGER", "INT32"):
            pa_type = pa.int64()
        elif typ in ("FLOAT64", "FLOAT", "DOUBLE"):
            pa_type = pa.float64()
        elif typ in ("NUMERIC",):
            pa_type = pa.decimal128(38, 9)
        elif typ in ("BOOLEAN", "BOOL"):
            pa_type = pa.bool_()
        elif typ in ("DATE",):
            pa_type = pa.date32()
        elif typ in ("TIMESTAMP",):
            pa_type = pa.timestamp("us")
        else:
            # default to string
            pa_type = pa.string()
        fields.append(pa.field(name, pa_type))
    return pa.schema(fields)


def _parse_date(value: Any, formats: List[str]) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None


def _parse_timestamp(value: Any, formats: List[str]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    # Attempt epoch seconds or milliseconds
    try:
        iv = int(s)
    except Exception:
        return None
    if len(s) > 10:
        return datetime.fromtimestamp(iv / 1000.0)
    return datetime.fromtimestamp(iv)


def normalize_row_to_schema(
    row: Dict[str, Any],
    schema: pa.Schema,
    formats: FormatSpec,
) -> Dict[str, Any]:
    """Normalise a single row to match the provided PyArrow schema.

    Missing columns are filled with appropriate defaults (empty string
    for strings, ``None`` for other types).  Dates and timestamps are
    parsed using the format lists provided in ``formats``.
    """
    out: Dict[str, Any] = {}
    for field in schema:
        name = field.name
        val = row.get(name)
        if pa.types.is_string(field.type):
            out[name] = "" if val is None else str(val)
        elif pa.types.is_date(field.type):
            out[name] = None if val in (None, "") else _parse_date(val, formats.date)
        elif pa.types.is_timestamp(field.type):
            out[name] = None if val in (None, "") else _parse_timestamp(val, formats.timestamp)
        elif pa.types.is_boolean(field.type):
            if val in (None, ""):
                out[name] = None
            elif isinstance(val, bool):
                out[name] = val
            else:
                # convert truthy strings/numbers
                out[name] = str(val).lower() in ("true", "1", "yes")
        elif pa.types.is_integer(field.type):
            try:
                out[name] = None if val in (None, "") else int(val)
            except Exception:
                out[name] = None
        elif pa.types.is_floating(field.type):
            try:
                out[name] = None if val in (None, "") else float(val)
            except Exception:
                out[name] = None
        else:
            out[name] = val
    return out


__all__ = [
    "load_schema_from_spec",
    "build_pyarrow_schema",
    "normalize_row_to_schema",
]