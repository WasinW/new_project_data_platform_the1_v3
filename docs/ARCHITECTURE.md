# Architecture Documentation

## System Architecture

### Overview

The refactored pipeline follows a modular, configuration-driven architecture that separates concerns and maximizes code reuse. The system is designed to handle three distinct data processing patterns:

1. **Batch Processing** (Short-term): Hourly batch loads for recent data
2. **Streaming with Reconciliation** (Mid-term): Real-time processing with data reconciliation
3. **Pure Streaming** (Long-term): Direct real-time processing to refined tables

### Component Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Configuration Layer                       │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐    │
│  │ YAML Configs │ │ Environment  │ │ Secret Manager  │    │
│  │              │ │   Variables  │ │                  │    │
│  └──────────────┘ └──────────────┘ └──────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Orchestration Layer                        │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐    │
│  │ Airflow DAGs │ │  Pipeline    │ │    Config       │    │
│  │              │ │ Orchestrator │ │    Manager      │    │
│  └──────────────┘ └──────────────┘ └──────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Processing Layer                           │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐    │
│  │  Dataflow    │ │   Common     │ │   Transform     │    │
│  │  Pipelines   │ │   Modules    │ │    Steps        │    │
│  └──────────────┘ └──────────────┘ └──────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       Data Layer                              │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐    │
│  │   BigQuery   │ │   Pub/Sub    │ │    Bigtable     │    │
│  │              │ │              │ │                  │    │
│  └──────────────┘ └──────────────┘ └──────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow Architecture

#### Batch Pipeline (Short-term)

```
Source (BigQuery) 
    ↓
Read & Validate
    ↓
Column Mapping & Enrichment
    ├─→ stg_ms_personas
    └─→ stg_ms_member
    ↓
Audit Logging
```

#### Streaming Pipeline (Mid-term)

```
Pub/Sub Notification
    ↓
Parse & Enrich (Bigtable)
    ↓
Apply Windows
    ↓
Column Mapping (with Side Input)
    ├─→ stg_ms_member
    └─→ ms_personas (refined)
    ↓
Windowed Audit
```

#### Streaming Pipeline (Long-term)

```
Pub/Sub Notification
    ↓
Parse & Enrich (Bigtable)
    ↓
Apply Windows
    ↓
Direct Write
    └─→ ms_personas (refined)
    ↓
Windowed Audit
```

### Module Architecture

#### Core Module Structure

```python
dataflow_common/
├── core.py           # Abstract base classes
│   ├── PipelineStep
│   ├── DataConnector
│   ├── Transformer
│   └── AuditLogger
│
├── config.py         # Configuration management
│   ├── PipelineConfig
│   ├── JobConfig
│   └── DataflowJobConfig
│
├── connectors.py     # Data source/sink connectors
│   ├── BigQueryConnector
│   ├── PubSubConnector
│   ├── BigtableConnector
│   └── CloudStorageConnector
│
├── transformers.py   # Data transformations
│   ├── ColumnMapper
│   ├── DataQualityTransformer
│   ├── MappingLoader
│   └── WindowedAggregator
│
├── steps.py          # Reusable pipeline steps
│   ├── ReadFromBigQueryStep
│   ├── DataQualityStep
│   ├── WriteToBigQueryStep
│   └── AuditLoggingStep
│
└── orchestrator.py   # Pipeline orchestration
    ├── PipelineOrchestrator
    └── ConfigManager
```

### Configuration Architecture

#### Configuration Hierarchy

```yaml
# Base configuration structure
pipeline:
  name: pipeline_name
  term_type: short|mid|long
  mode: batch|streaming

gcp:
  project_id: project-id
  location: region
  environment: dev|staging|prod

datasets:
  source_dataset: source
  staging_dataset: staging  
  refined_dataset: refined

dataflow:
  machine_type: n1-standard-2
  max_num_workers: 5
  # ... other settings

# Environment-specific overrides
environments:
  prod:
    gcp:
      project_id: prod-project
    datasets:
      staging_dataset: prod_staging
```

### Mapping Architecture

#### Column Mapping Flow

```
mapping_reconcile table
    ↓
MappingLoader.load_mapping()
    ↓
Creates mapping dictionary:
{
  'source_to_ongoing': {
    'data_col': 'tech_col',  # If RECONCILE_RETRIEVED = 'Y'
    ...
  },
  'source_to_origin': {
    'data_col': 'tech_col',  # If RECONCILE_CONFIRMED = 'Y'
    ...
  }
}
    ↓
Applied by ColumnMapper
```

#### Mapping Refresh Strategy (Streaming)

```
PeriodicImpulse (every 10 min)
    ↓
MappingCacheLoader
    ↓
Query mapping_reconcile
    ↓
Create Side Input
    ↓
StreamingColumnMapper uses cached mapping
```

### Security Architecture

#### Credential Management

```
1. Default: Application Default Credentials (ADC)
2. Service Account: Via --service_account_email
3. Secret Manager: Via --specific_sa (disabled but available)
```

#### Permission Model

```
Required Permissions:
├── BigQuery
│   ├── bigquery.datasets.get
│   ├── bigquery.tables.create
│   ├── bigquery.tables.get
│   └── bigquery.jobs.create
│
├── Dataflow
│   ├── dataflow.jobs.create
│   └── dataflow.jobs.get
│
├── Pub/Sub
│   ├── pubsub.topics.create
│   └── pubsub.subscriptions.create
│
└── Storage
    ├── storage.objects.create
    └── storage.objects.get
```

### Monitoring Architecture

#### Metrics Collection

```
Pipeline Execution
    ↓
Beam Metrics API
    ├─→ Counters (processed, errors)
    ├─→ Distributions (latency)
    └─→ Gauges (current state)
    ↓
Cloud Monitoring
```

#### Audit Trail

```
Every Record
    ↓
Add Metadata (_timestamp, _status)
    ↓
AuditLogger
    ↓
audit_job_log table
    ├── job_time
    ├── pipeline
    ├── mode
    ├── records_processed
    └── status
```

### Error Handling Architecture

#### Error Flow

```
Record Processing
    ↓
Try Process
    ├─→ Success → Continue
    └─→ Error → ErrorHandler
              ├─→ Log Error
              ├─→ Add to Error Counter
              └─→ Route to Error Table (optional)
```

#### Retry Strategy

```
DAG Level:
  retries: 2
  retry_delay: 5 minutes
  retry_exponential_backoff: true

Dataflow Level:
  - Automatic retries for transient errors
  - Dead letter queues for persistent failures
```

### Performance Considerations

#### Batch Optimization

```python
# Batch enrichment for efficiency
beam.BatchElements(
    min_batch_size=100,
    max_batch_size=500
) >> EnrichAndMapColumns()
```

#### Streaming Optimization

```python
# Window aggregation
beam.WindowInto(
    FixedWindows(3600),  # 1 hour
    trigger=AfterWatermark(
        early=AfterProcessingTime(60)  # Early firing
    ),
    accumulation_mode=DISCARDING
)
```

#### Bigtable Optimization

```python
# Column filtering for reduced latency
row_filter = row_filters.ColumnQualifierRegexFilter(
    b'(member_number|first_name|last_name|email)'
)
```

### Scalability Design

#### Horizontal Scaling

- **Workers**: Auto-scaling based on load
- **Partitioning**: Time-based partitioning in BigQuery
- **Sharding**: Automatic in Dataflow

#### Vertical Scaling

- **Machine Types**: Configurable via YAML
- **Memory**: Adjustable per pipeline requirements
- **Network**: Streaming engine for better throughput

### Deployment Architecture

#### CI/CD Pipeline

```
Code Push
    ↓
GitHub Actions
    ├─→ Lint & Test
    ├─→ Build Artifacts
    └─→ Deploy
        ├─→ Upload to GCS
        ├─→ Update Composer
        └─→ Trigger Validation
```

#### Environment Promotion

```
Development
    ↓ (Test & Validate)
Staging
    ↓ (UAT)
Production
```

### Disaster Recovery

#### Backup Strategy

- **Configuration**: Version controlled in Git
- **Data**: BigQuery snapshots and time travel
- **State**: Dataflow checkpoint storage

#### Recovery Procedures

1. **Pipeline Failure**: Automatic retry with exponential backoff
2. **Data Corruption**: Rollback using BigQuery time travel
3. **Complete Failure**: Restore from backups and replay from Pub/Sub

### Future Enhancements

1. **Multi-region Support**: Cross-region replication
2. **Schema Evolution**: Automatic schema migration
3. **ML Integration**: Real-time scoring pipelines
4. **Cost Optimization**: Spot instances and committed use
5. **Advanced Monitoring**: Custom metrics and alerting

## Design Principles

1. **Separation of Concerns**: Clear boundaries between components
2. **Configuration over Code**: Externalize all settings
3. **DRY (Don't Repeat Yourself)**: Reusable modules
4. **SOLID Principles**: Applied to class design
5. **Fail-Safe Defaults**: Graceful degradation
6. **Observability First**: Built-in monitoring and logging
