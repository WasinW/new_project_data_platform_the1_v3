# dataflow_common/__init__.py
"""Common modules for Dataflow pipelines

This package contains reusable components for building data pipelines:
- core: Base classes and abstractions
- config: Configuration management
- connectors: Data source/sink connectors
- transformers: Data transformation utilities
- steps: Reusable pipeline steps
- orchestrator: Pipeline orchestration
"""

from .core import (
    PipelineStep,
    DataConnector,
    Transformer,
    AuditLogger,
    WindowedAuditLogger,
    ErrorHandler,
    MetricsCollector,
    PipelineContext,
    create_test_pipeline,
    format_table_id,
    parse_table_id,
    safe_json_loads,
    batch_elements
)

from .config import (
    PipelineConfig,
    JobConfig,
    DataflowJobConfig
)

from .connectors import (
    BigQueryConnector,
    PubSubConnector,
    BigtableConnector,
    CloudStorageConnector,
    ConnectorFactory
)

from .transformers import (
    MappingLoader,
    ColumnMapper,
    StreamingColumnMapper,
    EnrichAndMapColumns,
    MappingCacheLoader,
    DataQualityTransformer,
    RecordHasher,
    WindowedAggregator,
    NotificationParser
)

from .steps import (
    ReadFromBigQueryStep,
    ReadFromPubSubStep,
    BigtableEnrichmentStep,
    ColumnMappingStep,
    DataQualityStep,
    WriteToBigQueryStep,
    AuditLoggingStep,
    CreateMappingSideInput,
    BranchingStep
)

from .orchestrator import (
    PipelineOrchestrator,
    ConfigManager
)

__version__ = "1.0.0"
__all__ = [
    # Core
    'PipelineStep',
    'DataConnector',
    'Transformer',
    'AuditLogger',
    'WindowedAuditLogger',
    'ErrorHandler',
    'MetricsCollector',
    'PipelineContext',
    'create_test_pipeline',
    'format_table_id',
    'parse_table_id',
    'safe_json_loads',
    'batch_elements',
    
    # Config
    'PipelineConfig',
    'JobConfig',
    'DataflowJobConfig',
    
    # Connectors
    'BigQueryConnector',
    'PubSubConnector',
    'BigtableConnector',
    'CloudStorageConnector',
    'ConnectorFactory',
    
    # Transformers
    'MappingLoader',
    'ColumnMapper',
    'StreamingColumnMapper',
    'EnrichAndMapColumns',
    'MappingCacheLoader',
    'DataQualityTransformer',
    'RecordHasher',
    'WindowedAggregator',
    'NotificationParser',
    
    # Steps
    'ReadFromBigQueryStep',
    'ReadFromPubSubStep',
    'BigtableEnrichmentStep',
    'ColumnMappingStep',
    'DataQualityStep',
    'WriteToBigQueryStep',
    'AuditLoggingStep',
    'CreateMappingSideInput',
    'BranchingStep',
    
    # Orchestrator
    'PipelineOrchestrator',
    'ConfigManager'
]
