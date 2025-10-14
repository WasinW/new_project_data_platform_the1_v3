"""
Step registry with string references (no direct imports)
"""

# String references to step implementations
# These will be dynamically imported on the worker
STEP_REGISTRY = {
    "ReadBQQuery": "dataflow_worker.steps.ReadBQQueryStep",
    "BuildMappingDict": "dataflow_worker.steps.BuildMappingDictStep",
    "ParseProfiles": "dataflow_worker.steps.ParseProfilesStep",
    "MapRecord": "dataflow_worker.steps.MapRecordStep",
    "KVPairs": "dataflow_worker.steps.KVPairsStep",
    "CoGroupByKey": "dataflow_worker.steps.CoGroupByKeyStep",
    "CoalesceByMapping": "dataflow_worker.steps.CoalesceByMappingStep",
    "NormalizeToSchema": "dataflow_worker.steps.NormalizeToSchemaStep",
    "WriteParquet": "dataflow_worker.steps.WriteParquetStep",
    "ConsumePubSub": "dataflow_worker.steps.ConsumePubSubStep",
    "ExtractKeys": "dataflow_worker.steps.ExtractKeysStep",
    "ReadBigTableRealtime": "dataflow_worker.steps.ReadBigTableRealtimeStep",
    "ProcessWithDLQ": "dataflow_worker.steps.ProcessWithDLQStep",
    "Window": "dataflow_worker.steps.WindowStep",
    "WriteToBigQuery": "dataflow_worker.steps.WriteToBigQueryStep",
    "CreateFixedMapping": "dataflow_worker.steps.CreateFixedMappingStep",
    "CreateEmpty": "dataflow_worker.steps.CreateEmptyStep",
    "WriteGCS": "dataflow_worker.steps.WriteGCSStep",
    "GetNewMaxDate": "dataflow_worker.steps.GetNewMaxDateStep",
    "SetMaxDateParam": "dataflow_worker.steps.SetMaxDateParamStep",
    
    # BigTable steps
    "ReadBigTableBatch": "dataflow_worker.steps.bigtable_batch_steps.ReadBigTableBatchStep",
    "WriteBigTableBatch": "dataflow_worker.steps.bigtable_batch_steps.WriteBigTableBatchStep",
    "ReadBigTableRealtime": "dataflow_worker.steps.bigtable_realtime_steps.ReadBigTableRealtimeStep",
    "WriteBigTableRealtime": "dataflow_worker.steps.bigtable_realtime_steps.WriteBigTableRealtimeStep",
}

__all__ = ["STEP_REGISTRY"]
