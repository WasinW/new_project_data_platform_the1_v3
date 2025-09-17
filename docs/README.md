# THE1 Member Data Pipeline (Refactored)

## Overview

This is a refactored version of the THE1 Member Data Pipeline that improves maintainability through:
- **Configuration-driven architecture**: All settings externalized to YAML files
- **Common modules**: Reusable components for pipeline operations
- **Modular design**: Clear separation of concerns between components
- **Enhanced monitoring**: Built-in health checks and audit logging

## Architecture

### Directory Structure

```
refactored_pipeline/
├── composer/
│   ├── dags/                       # Airflow DAGs
│   │   ├── dag_short_term_hourly.py    # Short-term batch processing
│   │   ├── dag_streaming_realtime.py   # Mid/Long-term streaming
│   │   ├── pl_init.py                  # Initialization pipeline
│   │   └── full_patch_pl.py            # Reconciliation pipeline
│   └── config/
│       └── ms_member/
│           ├── batch/              # Batch pipeline configs
│           │   └── short_term_hourly.yaml
│           ├── streaming/          # Streaming pipeline configs
│           │   └── streaming_realtime.yaml
│           ├── reconcile/          # Reconciliation configs
│           │   └── full_patch_config.yaml
│           └── init/               # Initialization configs
│               └── pl_init_config.yaml
├── dataflow/
│   ├── FW/                        # Framework pipelines
│   │   └── unified_dataflow_pipeline_bigtable.py
│   └── dataflow_common/           # Common modules
│       ├── __init__.py
│       ├── core.py                # Base classes
│       ├── config.py              # Configuration management
│       ├── connectors.py          # Data connectors
│       ├── transformers.py        # Transformation utilities
│       ├── steps.py               # Reusable pipeline steps
│       └── orchestrator.py        # Pipeline orchestration
├── docs/
│   ├── README.md                  # This file
│   ├── ARCHITECTURE.md            # Architecture details
│   └── DEPLOYMENT.md              # Deployment guide
└── sql/
    └── create_audit_tables.sql    # Audit table definitions
```

## Components

### 1. Configuration Management

All pipeline configurations are externalized to YAML files:

- **Batch Configuration** (`batch/short_term_hourly.yaml`): Settings for hourly batch processing
- **Streaming Configuration** (`streaming/streaming_realtime.yaml`): Real-time streaming pipeline settings
- **Reconciliation Configuration** (`reconcile/full_patch_config.yaml`): Data reconciliation settings
- **Initialization Configuration** (`init/pl_init_config.yaml`): Initial data load settings

### 2. Common Modules

The `dataflow_common` package provides reusable components:

#### Core (`core.py`)
- `PipelineStep`: Abstract base class for pipeline steps
- `DataConnector`: Abstract base for data connectors
- `AuditLogger`: Audit logging functionality
- `ErrorHandler`: Error handling and routing
- `MetricsCollector`: Metrics collection and aggregation

#### Configuration (`config.py`)
- `PipelineConfig`: Main configuration class
- `JobConfig`: Job execution configuration
- `DataflowJobConfig`: Dataflow-specific settings

#### Connectors (`connectors.py`)
- `BigQueryConnector`: BigQuery read/write operations
- `PubSubConnector`: Pub/Sub messaging
- `BigtableConnector`: Bigtable enrichment
- `CloudStorageConnector`: GCS operations

#### Transformers (`transformers.py`)
- `ColumnMapper`: Column mapping transformations
- `DataQualityTransformer`: Data quality validation
- `MappingLoader`: Load mapping configurations
- `WindowedAggregator`: Window-based aggregation

#### Steps (`steps.py`)
- `ReadFromBigQueryStep`: Read BigQuery data
- `ReadFromPubSubStep`: Read streaming data
- `BigtableEnrichmentStep`: Enrich with Bigtable
- `DataQualityStep`: Validate data quality
- `WriteToBigQueryStep`: Write to BigQuery
- `AuditLoggingStep`: Audit logging

### 3. Pipeline Types

#### Short-Term Batch (Hourly)
- **Schedule**: Every hour
- **Mode**: Batch processing
- **Purpose**: Process recent data in hourly batches
- **Configuration**: `batch/short_term_hourly.yaml`

#### Mid-Term Streaming
- **Schedule**: Continuous
- **Mode**: Streaming with windowing
- **Purpose**: Near real-time processing with reconciliation
- **Configuration**: `streaming/streaming_realtime.yaml` (term_type=mid)

#### Long-Term Streaming
- **Schedule**: Continuous
- **Mode**: Streaming without reconciliation
- **Purpose**: Real-time processing for refined data only
- **Configuration**: `streaming/streaming_realtime.yaml` (term_type=long)

## Key Features

### 1. Configuration-Driven

All settings are externalized to YAML files, making it easy to:
- Adjust settings without code changes
- Manage different environments
- Version control configurations
- Deploy to different projects

### 2. Health Monitoring

Built-in health checks include:
- Data freshness monitoring
- Record count validation
- Processing latency tracking
- Error rate monitoring

### 3. Audit Logging

Comprehensive audit logging tracks:
- Pipeline execution status
- Record processing counts
- Error details
- Window processing statistics

### 4. Flexible Mapping

Dynamic column mapping supports:
- Source-to-staging transformations
- Staging-to-refined mappings
- Periodic refresh of mappings
- Reconciliation status tracking

## Configuration Examples

### Batch Pipeline Configuration

```yaml
pipeline:
  name: ms_member_short_term_hourly
  schedule: "@hourly"
  term_type: short
  mode: batch

gcp:
  project_id: the1-insight-dev
  location: asia-southeast1
  environment: dev

datasets:
  source_dataset: insight
  staging_dataset: insight_dev
  refined_dataset: insight_dev

dataflow:
  machine_type: n1-standard-2
  max_num_workers: 5
  execution_timeout_minutes: 30
```

### Streaming Pipeline Configuration

```yaml
pipeline:
  term_type: mid
  mode: streaming

streaming:
  pubsub:
    topic_template: personas-updates-{env}
  window_duration_hours: 1
  enable_streaming_engine: true

dataflow:
  machine_type: n1-standard-4
  max_num_workers: 10
```

## Usage

### 1. Running with Configuration File

```python
# Using configuration file
python unified_dataflow_pipeline_bigtable.py \
  --config_file=gs://bucket/config/batch/short_term_hourly.yaml \
  --runner=DataflowRunner
```

### 2. Running with Command Line Arguments

```python
# Using command line arguments
python unified_dataflow_pipeline_bigtable.py \
  --project_id=the1-insight-dev \
  --term_type=short \
  --env=staging \
  --runner=DataflowRunner \
  --region=asia-southeast1 \
  --temp_location=gs://bucket/temp \
  --staging_location=gs://bucket/staging
```

### 3. Setting Environment Variables for DAGs

```bash
# Set configuration paths
export SHORT_TERM_CONFIG=/path/to/batch/short_term_hourly.yaml
export STREAMING_CONFIG=/path/to/streaming/streaming_realtime.yaml
export RECONCILE_CONFIG=/path/to/reconcile/full_patch_config.yaml
export INIT_CONFIG=/path/to/init/pl_init_config.yaml
```

## Deployment

### Prerequisites

1. **Google Cloud Project**: With appropriate APIs enabled
2. **Service Account**: With required permissions
3. **Storage Buckets**: For temp, staging, and outputs
4. **BigQuery Datasets**: For staging and refined data
5. **Pub/Sub Topics**: For streaming pipelines (mid/long term)
6. **Bigtable Instance**: For enrichment (streaming only)

### Installation

1. **Deploy Configuration Files**:
   ```bash
   gsutil cp -r composer/config gs://your-composer-bucket/dags/composer/
   ```

2. **Deploy DAGs**:
   ```bash
   gsutil cp composer/dags/*.py gs://your-composer-bucket/dags/
   ```

3. **Deploy Dataflow Code**:
   ```bash
   gsutil cp -r dataflow gs://your-dataflow-bucket/
   ```

4. **Create Audit Tables**:
   ```bash
   bq query --use_legacy_sql=false < sql/create_audit_tables.sql
   ```

### Environment-Specific Configuration

Modify YAML files for different environments:

```yaml
# For production
gcp:
  project_id: the1-insight-prod
  environment: prod

datasets:
  source_dataset: insight
  staging_dataset: insight_prod
  refined_dataset: insight_prod
```

## Monitoring

### Key Metrics

1. **Pipeline Health**:
   - Data freshness (minutes since last update)
   - Record processing rates
   - Error rates
   - Processing latency

2. **Data Quality**:
   - Null value counts
   - Record count differences
   - Schema validation errors

3. **System Performance**:
   - Worker utilization
   - Memory usage
   - Network throughput

### Dashboards

Create monitoring dashboards using:
- **Cloud Monitoring**: System metrics
- **BigQuery**: Data quality metrics
- **Dataflow UI**: Pipeline performance
- **Airflow UI**: DAG execution status

## Troubleshooting

### Common Issues

1. **Configuration Not Found**:
   - Check CONFIG_PATH environment variable
   - Verify file exists in specified location
   - Check file permissions

2. **Pipeline Fails to Start**:
   - Verify all required config fields are present
   - Check service account permissions
   - Validate GCS paths exist

3. **Data Quality Failures**:
   - Review audit_job_log table
   - Check data_quality validation rules
   - Verify source data schema

4. **Streaming Lag**:
   - Check Pub/Sub subscription backlog
   - Review window processing times
   - Scale workers if needed

## Best Practices

1. **Configuration Management**:
   - Use version control for config files
   - Keep sensitive data in Secret Manager
   - Use environment-specific configs

2. **Error Handling**:
   - Configure dead-letter queues
   - Set appropriate retry policies
   - Monitor error rates

3. **Performance Optimization**:
   - Tune batch sizes for enrichment
   - Adjust window durations based on volume
   - Use appropriate machine types

4. **Security**:
   - Use service accounts with minimal permissions
   - Encrypt data in transit and at rest
   - Audit access to sensitive data

## Support

For issues or questions:
1. Check the troubleshooting guide
2. Review audit logs in BigQuery
3. Contact the data engineering team

## License

Copyright (c) 2024 THE1 Digital
All rights reserved.
