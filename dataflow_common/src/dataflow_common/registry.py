"""
Step registry for the dataflow_common package.

This module contains a single dictionary, :data:`STEP_REGISTRY`,
mapping the string names used in a pipeline plan to the concrete
classes that implement each step.  When adding a new step class you
should also add an entry here so that the orchestrator can discover
it at runtime.
"""

from __future__ import annotations

from typing import Dict, Type

from .steps import (
    ReadBQQueryStep,
    BuildMappingDictStep,
    MapRecordStep,
    KVPairsStep,
    CoGroupByKeyStep,
    CoalesceByMappingStep,
    NormalizeToSchemaStep,
    WriteParquetStep,
    ConsumePubSubStep,
    ExtractKeysStep,
    ReadBigTableStep,
    ProcessWithDLQStep,
    WindowStep,
    WriteToBigQueryStep,  # เพิ่มนี้
    CreateFixedMappingStep,  # เพิ่ม
    CreateEmptyStep,  # เพิ่ม
)

# Import BigTable steps
from .steps.bigtable_batch_steps import (
    ReadBigTableBatchStep,
    WriteBigTableBatchStep
)
from .steps.bigtable_realtime_steps import (
    ReadBigTableRealtimeStep,
    WriteBigTableRealtimeStep
)

# Mapping from step type string in a plan to the corresponding class
STEP_REGISTRY: Dict[str, Type] = {
    "ReadBQQuery": ReadBQQueryStep,
    "BuildMappingDict": BuildMappingDictStep,
    "MapRecord": MapRecordStep,
    "KVPairs": KVPairsStep,
    "CoGroupByKey": CoGroupByKeyStep,
    "CoalesceByMapping": CoalesceByMappingStep,
    "NormalizeToSchema": NormalizeToSchemaStep,
    "WriteParquet": WriteParquetStep,
    "ConsumePubSub": ConsumePubSubStep,
    "ReadBigTable": ReadBigTableStep,
    "ProcessWithDLQ": ProcessWithDLQStep,
    "ExtractKeys": ExtractKeysStep,
    "Window": WindowStep,
    # BigTable Batch Steps (Option C)
    "ReadBigTableBatch": ReadBigTableBatchStep,
    "WriteBigTableBatch": WriteBigTableBatchStep,
    
    # BigTable Real-time Steps (Option D)
    "ReadBigTableRealtime": ReadBigTableRealtimeStep,
    "WriteBigTableRealtime": WriteBigTableRealtimeStep,
    "WriteToBigQuery": WriteToBigQueryStep,  # เพิ่มนี้
    "CreateFixedMapping": CreateFixedMappingStep,  # เพิ่ม
    "CreateEmpty": CreateEmptyStep,  # เพิ่ม
}

__all__ = ["STEP_REGISTRY"]