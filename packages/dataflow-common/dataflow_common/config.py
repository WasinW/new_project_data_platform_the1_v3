# dataflow_common/config.py
"""Configuration classes for pipeline management"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# @dataclass
# class PipelineConfig:
#     """Enhanced configuration holder for pipeline parameters"""
    
#     project_id: str
#     env: str = 'staging'
#     term_type: str = 'short'  # short, mid, or long
    
#     # Datasets
#     source_dataset: str = 'insight'
#     staging_dataset: str = 'insight_dev'
#     refined_dataset: str = 'insight_dev'
    
#     # Tables
#     source_table: str = 'personas_sync'
#     stg_source_table: str = 'stg_ms_personas'
#     stg_origin_table: str = 'stg_ms_member'
#     refined_ongoing_table: str = 'ms_personas'
#     audit_table: str = 'audit_job_log'
#     mapping_table: str = 'mapping_reconcile'
    
#     # Bigtable config
#     bigtable_instance_id: Optional[str] = None
#     bigtable_table_id: Optional[str] = None
#     bigtable_app_profile_id: Optional[str] = None
    
#     # Streaming config
#     pubsub_topic: Optional[str] = None
#     window_duration_hours: int = 1
#     audit_window_duration_hours: int = 1
#     mapping_refresh_minutes: int = 10
    
#     # Security
#     specific_sa: Optional[str] = None
#     service_account_credentials: Optional[str] = None
#     streaming_mode: str = 'exactly_once'
    
#     # Additional fields
#     metadata: Dict[str, Any] = field(default_factory=dict)
    
#     def __post_init__(self):
#         """Post-initialization processing"""
#         self.mode = 'batch' if self.term_type == 'short' else 'streaming'
#         self.window_duration = self.window_duration_hours * 3600
#         self.audit_window_duration = self.audit_window_duration_hours * 3600
#         self.mapping_refresh_interval = self.mapping_refresh_minutes * 60
        
#     def get_full_table_id(self, dataset: str, table: str) -> str:
#         """Get full BigQuery table ID"""
#         return f"{self.project_id}.{dataset}.{table}"
    
#     @classmethod
#     def from_dict(cls, config_dict: Dict[str, Any]) -> 'PipelineConfig':
#         """Create config from dictionary (typically from YAML)"""
#         # Map nested config structure to flat dataclass fields
#         flat_config = {
#             'project_id': config_dict.get('gcp', {}).get('project_id'),
#             'env': config_dict.get('gcp', {}).get('environment', 'staging'),
#             'term_type': config_dict.get('pipeline', {}).get('term_type', 'short'),
#         }
        
#         # Add dataset configuration
#         datasets = config_dict.get('datasets', {})
#         flat_config.update({
#             'source_dataset': datasets.get('source_dataset', 'insight'),
#             'staging_dataset': datasets.get('staging_dataset', 'insight_dev'),
#             'refined_dataset': datasets.get('refined_dataset', 'insight_dev'),
#         })
        
#         # Add table configuration
#         tables = config_dict.get('tables', {})
#         if tables:
#             flat_config.update({
#                 'source_table': tables.get('source_table', 'personas_sync'),
#                 'stg_source_table': tables.get('stg_source_table', 'stg_ms_personas'),
#                 'stg_origin_table': tables.get('stg_origin_table', 'stg_ms_member'),
#                 'refined_ongoing_table': tables.get('refined_ongoing_table', 'ms_personas'),
#                 'audit_table': tables.get('audit_table', 'audit_job_log'),
#                 'mapping_table': tables.get('mapping_table', 'mapping_reconcile'),
#             })
        
#         # Add Bigtable configuration
#         bigtable = config_dict.get('bigtable', {})
#         if bigtable:
#             flat_config.update({
#                 'bigtable_instance_id': bigtable.get('instance_id'),
#                 'bigtable_table_id': bigtable.get('table_id'),
#                 'bigtable_app_profile_id': bigtable.get('app_profile_id'),
#             })
        
#         # Add streaming configuration
#         streaming = config_dict.get('streaming', {})
#         if streaming:
#             flat_config.update({
#                 'pubsub_topic': streaming.get('pubsub', {}).get('topic'),
#                 'window_duration_hours': streaming.get('window_duration_hours', 1),
#                 'audit_window_duration_hours': streaming.get('audit_window_duration_hours', 1),
#                 'mapping_refresh_minutes': streaming.get('mapping_refresh_minutes', 10),
#                 'streaming_mode': streaming.get('mode', 'exactly_once'),
#             })
        
#         # Add security configuration
#         security = config_dict.get('security', {})
#         if security:
#             flat_config.update({
#                 'specific_sa': security.get('service_account'),
#             })
        
#         # Store original config as metadata
#         flat_config['metadata'] = config_dict
        
#         # Filter out None values
#         flat_config = {k: v for k, v in flat_config.items() if v is not None}
        
#         return cls(**flat_config)
    
#     @classmethod
#     def from_args(cls, args) -> 'PipelineConfig':
#         """Create config from argparse arguments"""
#         config_dict = {
#             'project_id': args.project_id,
#             'env': args.env,
#             'term_type': args.term_type,
#             'source_dataset': args.source_dataset,
#             'staging_dataset': args.staging_dataset,
#             'refined_dataset': args.refined_dataset,
#             'bigtable_instance_id': getattr(args, 'bigtable_instance_id', None),
#             'bigtable_table_id': getattr(args, 'bigtable_table_id', None),
#             'bigtable_app_profile_id': getattr(args, 'bigtable_app_profile_id', None),
#             'pubsub_topic': getattr(args, 'pubsub_topic', None),
#             'window_duration_hours': getattr(args, 'window_duration_hours', 1),
#             'specific_sa': getattr(args, 'specific_sa', None),
#             'streaming_mode': getattr(args, 'streaming_mode', 'exactly_once'),
#             'mapping_refresh_minutes': getattr(args, 'mapping_refresh_minutes', 10),
#         }
        
#         # Filter out None values
#         config_dict = {k: v for k, v in config_dict.items() if v is not None}
        
#         return cls(**config_dict)
    
#     def to_dict(self) -> Dict[str, Any]:
#         """Convert config to dictionary"""
#         return {
#             'project_id': self.project_id,
#             'env': self.env,
#             'term_type': self.term_type,
#             'mode': self.mode,
#             'source_dataset': self.source_dataset,
#             'staging_dataset': self.staging_dataset,
#             'refined_dataset': self.refined_dataset,
#             'source_table': self.source_table,
#             'stg_source_table': self.stg_source_table,
#             'stg_origin_table': self.stg_origin_table,
#             'refined_ongoing_table': self.refined_ongoing_table,
#             'audit_table': self.audit_table,
#             'mapping_table': self.mapping_table,
#             'bigtable_instance_id': self.bigtable_instance_id,
#             'bigtable_table_id': self.bigtable_table_id,
#             'bigtable_app_profile_id': self.bigtable_app_profile_id,
#             'pubsub_topic': self.pubsub_topic,
#             'window_duration_hours': self.window_duration_hours,
#             'audit_window_duration_hours': self.audit_window_duration_hours,
#             'mapping_refresh_minutes': self.mapping_refresh_minutes,
#             'streaming_mode': self.streaming_mode,
#             'specific_sa': self.specific_sa,
#         }
    
#     def validate(self) -> List[str]:
#         """Validate configuration and return list of issues"""
#         issues = []
        
#         # Basic validation
#         if not self.project_id:
#             issues.append("project_id is required")
        
#         if self.term_type not in ['short', 'mid', 'long']:
#             issues.append(f"Invalid term_type: {self.term_type}")
        
#         # Streaming validation
#         if self.mode == 'streaming':
#             if not self.pubsub_topic:
#                 issues.append("pubsub_topic is required for streaming mode")
            
#             if self.term_type in ['mid', 'long']:
#                 if not self.bigtable_instance_id or not self.bigtable_table_id:
#                     issues.append("Bigtable configuration required for mid/long term streaming")
        
#         return issues


@dataclass
class JobConfig:
    """Configuration for job execution"""
    
    name: str
    pipeline_type: str
    schedule: Optional[str] = None
    max_active_runs: int = 1
    catchup: bool = False
    tags: List[str] = field(default_factory=list)
    owner: str = 'data-engineering'
    email_on_failure: bool = False
    email_on_retry: bool = False
    email_recipients: List[str] = field(default_factory=list)
    retries: int = 2
    retry_delay_minutes: int = 5
    depends_on_past: bool = False
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'JobConfig':
        """Create job config from dictionary"""
        job_dict = config_dict.get('job', {})
        pipeline_dict = config_dict.get('pipeline', {})
        
        return cls(
            name=pipeline_dict.get('name', 'unnamed_job'),
            pipeline_type=pipeline_dict.get('term_type', 'batch'),
            schedule=pipeline_dict.get('schedule'),
            max_active_runs=job_dict.get('max_active_runs', 1),
            catchup=job_dict.get('catchup', False),
            tags=job_dict.get('tags', []),
            owner=config_dict.get('owner', 'data-engineering'),
            email_on_failure=config_dict.get('email_on_failure', False),
            email_on_retry=config_dict.get('email_on_retry', False),
            email_recipients=config_dict.get('email_recipients', []),
            retries=config_dict.get('retries', 2),
            retry_delay_minutes=config_dict.get('retry_delay_minutes', 5),
            depends_on_past=config_dict.get('depends_on_past', False),
        )


@dataclass
class DataflowJobConfig:
    """Configuration specifically for Dataflow jobs"""
    
    runner: str = 'DataflowRunner'
    machine_type: str = 'n1-standard-2'
    max_num_workers: int = 5
    disk_size_gb: int = 100
    use_public_ips: bool = False
    network: Optional[str] = None
    subnetwork: Optional[str] = None
    service_account_email: Optional[str] = None
    temp_location: Optional[str] = None
    staging_location: Optional[str] = None
    labels: Dict[str, str] = field(default_factory=dict)
    additional_experiments: List[str] = field(default_factory=list)
    dataflow_service_options: List[str] = field(default_factory=list)
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'DataflowJobConfig':
        """Create Dataflow config from dictionary"""
        df_config = config_dict.get('dataflow', {})
        storage_config = config_dict.get('storage', {})
        
        return cls(
            runner=df_config.get('runner', 'DataflowRunner'),
            machine_type=df_config.get('machine_type', 'n1-standard-2'),
            max_num_workers=df_config.get('max_num_workers', 5),
            disk_size_gb=df_config.get('disk_size_gb', 100),
            use_public_ips=df_config.get('use_public_ips', False),
            network=df_config.get('network'),
            subnetwork=df_config.get('subnetwork'),
            service_account_email=df_config.get('service_account'),
            temp_location=storage_config.get('temp_location'),
            staging_location=storage_config.get('staging_location'),
            labels=df_config.get('labels', {}),
            additional_experiments=df_config.get('additional_experiments', []),
            dataflow_service_options=df_config.get('dataflow_service_options', []),
        )
    
    def to_pipeline_options(self) -> Dict[str, Any]:
        """Convert to pipeline options dictionary"""
        options = {
            'runner': self.runner,
            'machine_type': self.machine_type,
            'max_num_workers': self.max_num_workers,
            'disk_size_gb': self.disk_size_gb,
            'use_public_ips': self.use_public_ips,
            'temp_location': self.temp_location,
            'staging_location': self.staging_location,
        }
        
        if self.network:
            options['network'] = self.network
        if self.subnetwork:
            options['subnetwork'] = self.subnetwork
        if self.service_account_email:
            options['service_account_email'] = self.service_account_email
        if self.labels:
            options['labels'] = self.labels
        if self.additional_experiments:
            options['experiments'] = self.additional_experiments
        if self.dataflow_service_options:
            options['dataflow_service_options'] = self.dataflow_service_options
        
        # Filter out None values
        return {k: v for k, v in options.items() if v is not None}
@dataclass
class CommonPipelineConfig:
    """Generic configuration holder that can be used across different pipelines
    This is a flexible config class that accepts any parameters via kwargs
    """
    
    # Required base fields
    project_id: str
    
    # Optional common fields with defaults
    env: str = 'dev'
    mode: str = 'batch'  # batch or streaming
    
    # Flexible configuration storage
    config_data: Dict[str, Any] = field(default_factory=dict)
    
    def __init__(self, project_id: str, **kwargs):
        """Initialize with project_id and any additional parameters"""
        self.project_id = project_id
        self.env = kwargs.pop('env', 'dev')
        self.mode = kwargs.pop('mode', 'batch')
        
        # Store all other parameters in config_data
        self.config_data = kwargs
        
        # Allow direct attribute access for common fields
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value with optional default"""
        # Check direct attributes first
        if hasattr(self, key):
            return getattr(self, key)
        # Then check config_data
        return self.config_data.get(key, default)
    
    def set(self, key: str, value: Any):
        """Set configuration value"""
        setattr(self, key, value)
        self.config_data[key] = value
    
    def get_full_table_id(self, dataset: str, table: str) -> str:
        """Utility method to get full BigQuery table ID"""
        return f"{self.project_id}.{dataset}.{table}"
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'CommonPipelineConfig':
        """Create config from dictionary (from Airflow parameters)"""
        # Extract project_id (required)
        project_id = config_dict.pop('project_id', None)
        if not project_id:
            raise ValueError("project_id is required in config")
        
        # Create instance with all parameters
        return cls(project_id=project_id, **config_dict)
    
    @classmethod
    def from_args(cls, args) -> 'CommonPipelineConfig':
        """Create config from argparse arguments"""
        # Convert args to dict
        args_dict = vars(args)
        
        # Remove None values
        args_dict = {k: v for k, v in args_dict.items() if v is not None}
        
        return cls.from_dict(args_dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary"""
        result = {
            'project_id': self.project_id,
            'env': self.env,
            'mode': self.mode
        }
        result.update(self.config_data)
        return result
    
    def validate(self) -> List[str]:
        """Basic validation - can be extended by specific pipelines"""
        issues = []
        
        # Basic validation
        if not self.project_id:
            issues.append("project_id is required")
        
        if self.mode not in ['batch', 'streaming']:
            issues.append(f"Invalid mode: {self.mode}")
            
        # Dataset name validation (แบบ safe - ถ้าไม่มีก็ข้าม)
        import re
        dataset_pattern = r'^[a-zA-Z][a-zA-Z0-9_]*$'
        
        for dataset_field in ['source_dataset', 'staging_dataset', 'refined_dataset']:
            value = self.get(dataset_field)
            if value and not re.match(dataset_pattern, value):
                logger.warning(f"Dataset name may be invalid: {value}")
                # ไม่ใส่ใน issues เพื่อไม่ให้ pipeline พัง
        
        # Table name validation (warning only)
        table_pattern = r'^[a-zA-Z][a-zA-Z0-9_]*$'
        
        for table_field in ['source_table', 'stg_source_table', 'stg_origin_table']:
            value = self.get(table_field)
            if value and not re.match(table_pattern, value):
                logger.warning(f"Table name may be invalid: {value}")
        
        # Critical validations only
        if self.mode == 'streaming' and not self.get('pubsub_topic'):
            issues.append("pubsub_topic is required for streaming mode")
        
        return issues
