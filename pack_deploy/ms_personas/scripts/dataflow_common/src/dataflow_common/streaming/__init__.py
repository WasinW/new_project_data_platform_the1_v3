"""Streaming helpers for :mod:`dataflow_common`.

The modules in this package provide reusable building blocks for
real-time Dataflow pipelines.  They deliberately avoid any knowledge
of specific datasets so that streaming jobs can share the same
infrastructure pieces as batch jobs.
"""

from .mapping import (
    create_mapping_query,
    MappingCacheLoader,
    mapping_side_input,
)
from .message import ParsePubSubMessage, ApplyMappingDoFn
from .enrichment import apply_bigtable_enrichment
from .sideinputs import CachedQuerySideInput, cached_side_input
from .sinks import (
    BatchedS3ParquetWriter,
    apply_sink_config,
    default_cleanup,
)

__all__ = [
    "create_mapping_query",
    "MappingCacheLoader",
    "mapping_side_input",
    "ParsePubSubMessage",
    "ApplyMappingDoFn",
    "apply_bigtable_enrichment",
    "CachedQuerySideInput",
    "cached_side_input",
    "BatchedS3ParquetWriter",
    "apply_sink_config",
    "default_cleanup",
]
