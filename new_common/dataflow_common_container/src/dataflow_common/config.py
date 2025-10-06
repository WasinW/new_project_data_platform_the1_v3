"""
Typed configuration models and loader for the dataflow_common package.

The primary entrypoint in this module is :func:`load_config`, which
reads a YAML file, optionally merges a defaults file, performs
environment variable expansion, and returns a :class:`PipelineConfig`
instance.  A small hierarchy of dataclasses defines the supported
structure of the configuration.  These classes perform basic
validation and provide defaults where appropriate.
"""

from __future__ import annotations

import os
import re
import yaml
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def _expand_env(value: Any) -> Any:
    """Recursively expand environment variables in a config value.

    Strings containing ``${VAR}`` will be replaced with the value of
    the corresponding environment variable.  If the variable is
    undefined it is replaced with an empty string.  For lists and
    dicts the expansion is applied elementwise.
    """
    if isinstance(value, str):
        result = ""
        i = 0
        while i < len(value):
            if value[i:i + 2] == "${":
                j = value.find("}", i + 2)
                if j != -1:
                    env = value[i + 2 : j]
                    result += os.environ.get(env, "")
                    i = j + 1
                else:
                    result += value[i:]
                    break
            else:
                result += value[i]
                i += 1
        return result
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    return value


def _merge_dicts(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Merge two dictionaries recursively.

    Values from *b* override values in *a*; nested dicts are merged
    recursively.  This helper is used to merge a ``defaults_file``
    into a pipeline config.
    """
    out = dict(a) if a else {}
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge_dicts(out[k], v)
        else:
            out[k] = v
    return out


@dataclass
class BigQuerySchemaSpec:
    """Specification of a BigQuery schema source.

    Either ``table`` or ``query`` should be provided.  If both are
    provided the query takes precedence.
    """

    project: str
    dataset: str
    table: Optional[str] = None
    query: Optional[str] = None


@dataclass
class SchemaSpec:
    """Specification of the data schema used in normalisation.

    A schema can be loaded from either a JSON file stored on GCS (via
    ``gcs_uri``) or from a BigQuery table/query (via the ``bq``
    attribute).  If both are provided the JSON file takes precedence.
    """

    gcs_uri: Optional[str] = None
    bq: Optional[BigQuerySchemaSpec] = None


@dataclass
class FormatSpec:
    """Specification of date and timestamp parsing formats.

    The order of formats matters: each format string will be tried
    sequentially when parsing a value.  You can override these lists
    in your YAML configuration.
    """

    date: List[str] = field(
        default_factory=lambda: ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"]
    )
    timestamp: List[str] = field(
        default_factory=lambda: [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
        ]
    )


@dataclass
class PipelineParams:
    """Parameters specific to the pipeline instance.

    ``pk`` identifies the field used to join new and old records in
    the pipeline.  ``run_dt`` should be filled in at runtime to
    construct output paths; it can be left ``None`` in the YAML and
    supplied programmatically by the orchestrator or DAG.
    """

    pk: str = "member_number"
    run_dt: Optional[str] = None


@dataclass
class IOConfig:
    """I/O configuration shared across steps.

    ``s3`` and ``bq`` hold arbitrary key/value pairs used by
    individual steps.  For example, ``s3`` may contain the
    ``refined_prefix`` used when writing Parquet files, while ``bq``
    may contain a ``project``, ``dataset`` and optional
    ``temp_gcs`` for BigQuery reads.  Additional keys can be added
    depending on your needs.
    """

    s3: Dict[str, Any] = field(default_factory=dict)
    bq: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineConfig:
    """Top‑level configuration for a single pipeline run.

    The ``plan`` is a list of dictionaries describing each step in
    order.  Fields that reference other values in the config can use
    Python ``str.format`` syntax; placeholders will be filled in by
    the orchestrator before the pipeline runs.
    """

    name: str
    mode: str
    term: str
    schema: Optional[SchemaSpec] = None
    formats: FormatSpec = field(default_factory=FormatSpec)
    params: PipelineParams = field(default_factory=PipelineParams)
    io: IOConfig = field(default_factory=IOConfig)
    plan: List[Dict[str, Any]] = field(default_factory=list)
    defaults_file: Optional[str] = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "PipelineConfig":
        """Create a :class:`PipelineConfig` from a plain dictionary.

        Missing optional sections will be filled with their defaults.
        """
        # Build schema spec
        schema_spec = None
        schema_dict = data.get("schema")
        if schema_dict:
            gcs_uri = schema_dict.get("gcs_uri")
            bq_spec = None
            bq_dict = schema_dict.get("bq")
            if bq_dict:
                bq_spec = BigQuerySchemaSpec(
                    project=bq_dict["project"],
                    dataset=bq_dict["dataset"],
                    table=bq_dict.get("table"),
                    query=bq_dict.get("query"),
                )
            schema_spec = SchemaSpec(gcs_uri=gcs_uri, bq=bq_spec)
        # Build format spec
        format_spec = FormatSpec(**data.get("formats", {}))
        # Build params
        params_spec = PipelineParams(**data.get("params", {}))
        # Build io config
        io_spec = IOConfig(**data.get("io", {}))
        plan = data.get("plan", [])
        return PipelineConfig(
            name=data["pipeline"].get("name"),
            mode=data["pipeline"].get("mode"),
            term=data["pipeline"].get("term"),
            schema=schema_spec,
            formats=format_spec,
            params=params_spec,
            io=io_spec,
            plan=plan,
            defaults_file=data.get("defaults_file"),
        )


def load_config(path: str, overrides: Optional[Dict[str, Any]] = None) -> PipelineConfig:
    """Load a pipeline configuration from YAML and return a dataclass.

    If the YAML document defines a ``defaults_file`` key the file at
    that path (relative to the YAML's directory if not absolute) will
    be loaded first and merged with the main document.  Overrides
    supplied via the ``overrides`` argument take precedence over both
    the defaults file and the primary file.  Environment variable
    references of the form ``${VAR_NAME}`` are expanded in both
    documents.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    defaults_path = data.get("defaults_file")
    base_data: Dict[str, Any] = {}
    if defaults_path:
        # Resolve relative defaults file
        if not os.path.isabs(defaults_path):
            defaults_path = os.path.join(os.path.dirname(path), defaults_path)
        with open(defaults_path, "r", encoding="utf-8") as f:
            base_data = yaml.safe_load(f) or {}
    merged = _merge_dicts(base_data, data)
    if overrides:
        merged = _merge_dicts(merged, overrides)
    expanded = _expand_env(merged)
    return PipelineConfig.from_dict(expanded)