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
    ProcessWithDLQStep,
    WindowStep,
    WriteToBigQueryStep,  # เพิ่มนี้
    CreateFixedMappingStep,  # เพิ่ม
    CreateEmptyStep,  # เพิ่ม
    # ReadGCSStep,  # ✅ เพิ่ม import นี้
    WriteGCSStep,  # ✅ เพิ่ม import นี้  
    GetNewMaxDateStep,  # ✅ เพิ่ม import นี้
    SetMaxDateParamStep,  # ✅ เพิ่ม import นี้
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

# Import streaming steps only if available (safe import)
try:
    from .steps.streaming import (
        ConsumePubSubStep,
        ExtractKeysStep,
        ReadBigTableRealtimeStep,
        ProcessWithDLQStep,
        WindowStep,
        WriteToBigQueryStep,
        CreateFixedMappingStep,
        CreateEmptyStep,
        # ReadGCSStep,  # ✅ เพิ่ม import
        WriteGCSStep,  # ✅ เพิ่ม import  
        GetNewMaxDateStep,  # ✅ เพิ่ม import
        SetMaxDateParamStep,
    )
    STREAMING_AVAILABLE = True
except ImportError:
    STREAMING_AVAILABLE = False

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
    # "ReadBigTable": ReadBigTableStep,
    # "ProcessWithDLQ": ProcessWithDLQStep,
    # "ExtractKeys": ExtractKeysStep,
    # "Window": WindowStep,
    # # BigTable Batch Steps (Option C)
    # "ReadBigTableBatch": ReadBigTableBatchStep,
    # "WriteBigTableBatch": WriteBigTableBatchStep,
    
    # # BigTable Real-time Steps (Option D)
    # "ReadBigTableRealtime": ReadBigTableRealtimeStep,
    # "WriteBigTableRealtime": WriteBigTableRealtimeStep,
    # "WriteToBigQuery": WriteToBigQueryStep,  # เพิ่มนี้
    # "CreateFixedMapping": CreateFixedMappingStep,  # เพิ่ม
    # "CreateEmpty": CreateEmptyStep,  # เพิ่ม
    # # "ReadGCS": ReadGCSStep,  # ✅ เพิ่มใน registry
    "WriteGCS": WriteGCSStep,  # ✅ เพิ่มใน registry
    "GetNewMaxDate": GetNewMaxDateStep,  # ✅ เพิ่มใน registry
    "SetMaxDateParam": SetMaxDateParamStep,  # ✅ เพิ่มใน registry

}
# Add streaming steps only if available
if STREAMING_AVAILABLE:
    STEP_REGISTRY.update({
        "ConsumePubSub": ConsumePubSubStep,
        "ExtractKeys": ExtractKeysStep,
        "ReadBigTableRealtime": ReadBigTableRealtimeStep,
        "ProcessWithDLQ": ProcessWithDLQStep,
        "Window": WindowStep,
        "WriteToBigQuery": WriteToBigQueryStep,
        "CreateFixedMapping": CreateFixedMappingStep,
        "CreateEmpty": CreateEmptyStep,
    })
    
__all__ = ["STEP_REGISTRY"]