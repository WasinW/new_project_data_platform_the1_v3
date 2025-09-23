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

from .__version__ import __version__, __author__, __email__

# Core imports
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

# Config imports
from .config import (
    CommonPipelineConfig,
    JobConfig,
    DataflowJobConfig
)

# Connector imports
from .connectors import (
    BigQueryConnector,
    PubSubConnector,
    BigtableConnector,
    CloudStorageConnector,
    ConnectorFactory
)

# Transformer imports
from .transformers import (
    MappingLoader,
    ColumnMapper,
    StreamingColumnMapper,
    EnrichAndMapColumns,
    MappingCacheLoader,
    DataQualityTransformer,
    RecordHasher,
    WindowedAggregator,
    NotificationParser,
    CDCFormatter,          # Add these
    CDCUpsertFormatter,    # Add these
    CDCDeleteFormatter     # Add these
)

# Step imports
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

# Orchestrator imports
from .orchestrator import (
    PipelineOrchestrator,
    ConfigManager
)

__version__ = "1.0.0"
# Define what's available when using "from dataflow_common import *"
__all__ = [
    # Version info
    '__version__',
    '__author__',
    '__email__',
    
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
    'CommonPipelineConfig',
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
    'CDCFormatter',
    'CDCUpsertFormatter', 
    'CDCDeleteFormatter',

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

# Package metadata
__package_name__ = 'dataflow-common-the1'
__description__ = 'Common modules for THE1 Dataflow pipelines'
__url__ = 'https://github.com/the1/dataflow-common'
__license__ = 'Apache 2.0'