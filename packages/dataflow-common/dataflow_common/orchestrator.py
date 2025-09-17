# dataflow_common/orchestrator.py
"""Pipeline orchestrator for managing execution"""

from typing import Dict, Any, Optional, List
import logging
import yaml
import json
from datetime import datetime
from pathlib import Path
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    """Orchestrate pipeline execution"""
    
    def __init__(self, config_path: str = None, config_dict: Dict[str, Any] = None):
        """Initialize with configuration file or dictionary"""
        if config_path:
            self.config = self.load_config(config_path)
        elif config_dict:
            self.config = config_dict
        else:
            raise ValueError("Either config_path or config_dict must be provided")
        
        self.pipeline_options = None
        self.steps = []
        
    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """Load configuration from file"""
        path = Path(config_path)
        
        if config_path.startswith('gs://'):
            # Load from GCS
            from apache_beam.io.filesystems import FileSystems
            with FileSystems.open(config_path) as f:
                content = f.read().decode('utf-8')
                if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                    return yaml.safe_load(content)
                elif config_path.endswith('.json'):
                    return json.loads(content)
        else:
            # Load from local file
            with open(path, 'r') as f:
                if path.suffix in ['.yaml', '.yml']:
                    return yaml.safe_load(f)
                elif path.suffix == '.json':
                    return json.load(f)
        
        raise ValueError(f"Unsupported configuration file format: {config_path}")
    
    def build_pipeline_options(self, **kwargs) -> PipelineOptions:
        """Build pipeline options from configuration"""
        options_dict = {
            'project': self.config.get('gcp', {}).get('project_id'),
            'region': self.config.get('gcp', {}).get('location'),
            'temp_location': self.config.get('storage', {}).get('temp_location'),
            'staging_location': self.config.get('storage', {}).get('staging_location'),
            'runner': kwargs.get('runner', 'DataflowRunner'),
            'save_main_session': self.config.get('dataflow', {}).get('save_main_session', True),
        }
        
        # Add job name if template provided
        if 'job_name_template' in self.config.get('dataflow', {}):
            options_dict['job_name'] = self.config['dataflow']['job_name_template'].format(
                term_type=self.config.get('pipeline', {}).get('term_type', 'batch'),
                env=self.config.get('gcp', {}).get('environment', 'dev'),
                timestamp=datetime.now().strftime('%Y%m%d-%H%M%S')
            )
        
        # Add machine type configuration
        if 'machine_type' in self.config.get('dataflow', {}):
            options_dict['machine_type'] = self.config['dataflow']['machine_type']
        
        if 'max_num_workers' in self.config.get('dataflow', {}):
            options_dict['max_num_workers'] = self.config['dataflow']['max_num_workers']
        
        # Add streaming specific options
        if self.config.get('pipeline', {}).get('mode') == 'streaming':
            options_dict['streaming'] = True
            if self.config.get('streaming', {}).get('enable_streaming_engine'):
                options_dict['enable_streaming_engine'] = True
        
        # Override with any provided kwargs
        options_dict.update(kwargs)
        
        self.pipeline_options = PipelineOptions(**{k: v for k, v in options_dict.items() if v is not None})
        return self.pipeline_options
    
    def add_step(self, step):
        """Add a pipeline step"""
        self.steps.append(step)
    
    def run(self, **kwargs):
        """Execute the pipeline"""
        if not self.pipeline_options:
            self.build_pipeline_options(**kwargs)
        
        with beam.Pipeline(options=self.pipeline_options) as pipeline:
            current_pcoll = None
            
            for step in self.steps:
                if hasattr(step, 'execute'):
                    current_pcoll = step.execute(pipeline, current_pcoll)
                elif callable(step):
                    current_pcoll = step(pipeline, current_pcoll)
                else:
                    raise ValueError(f"Step {step} is not executable")
            
        logger.info("Pipeline execution completed successfully")
        return True

class ConfigManager:
    """Manage configuration across different environments"""
    
    def __init__(self, base_config_path: str):
        self.base_config_path = base_config_path
        self.config_cache = {}
        
    def get_config(self, domain: str, config_type: str, 
                   environment: str = None) -> Dict[str, Any]:
        """Get configuration for specific domain and type"""
        cache_key = f"{domain}_{config_type}_{environment}"
        
        if cache_key in self.config_cache:
            return self.config_cache[cache_key]
        
        # Build config path
        if self.base_config_path.startswith('gs://'):
            config_path = f"{self.base_config_path}/{domain}/{config_type}.yaml"
        else:
            config_path = Path(self.base_config_path) / domain / f"{config_type}.yaml"
            config_path = str(config_path)
        
        # Load config
        config = PipelineOrchestrator.load_config(config_path)
        
        # Apply environment overrides if provided
        if environment and 'environments' in config:
            env_config = config.get('environments', {}).get(environment, {})
            config = self._merge_configs(config, env_config)
        
        self.config_cache[cache_key] = config
        return config
    
    @staticmethod
    def _merge_configs(base_config: Dict[str, Any], 
                      override_config: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively merge configuration dictionaries"""
        import copy
        result = copy.deepcopy(base_config)
        
        for key, value in override_config.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = ConfigManager._merge_configs(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def get_dataset_config(self, domain: str, environment: str = 'dev') -> Dict[str, str]:
        """Get dataset configuration for a domain"""
        config = self.get_config(domain, 'datasets', environment)
        return config.get('datasets', {})
    
    def get_dataflow_config(self, domain: str, term_type: str) -> Dict[str, Any]:
        """Get dataflow configuration for specific term type"""
        config_type = f"{term_type}_config"
        config = self.get_config(domain, config_type)
        return config.get('dataflow', {})
