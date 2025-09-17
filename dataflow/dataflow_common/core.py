# dataflow_common/core.py
"""Core abstractions and base classes for pipeline components"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime
import apache_beam as beam

logger = logging.getLogger(__name__)


class PipelineStep(ABC):
    """Abstract base class for pipeline steps"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.step_name = config.get('step_name', self.__class__.__name__)
        self.enabled = config.get('enabled', True)
        
    @abstractmethod
    def execute(self, pipeline: beam.Pipeline, 
                input_pcoll: Optional[beam.PCollection] = None) -> Optional[beam.PCollection]:
        """Execute the step"""
        pass
    
    def is_enabled(self) -> bool:
        """Check if step is enabled"""
        return self.enabled
    
    def validate_config(self) -> List[str]:
        """Validate step configuration"""
        return []


class DataConnector(ABC):
    """Abstract base class for data connectors"""
    
    @abstractmethod
    def read(self, *args, **kwargs):
        """Read data from source"""
        pass
    
    @abstractmethod
    def write(self, *args, **kwargs):
        """Write data to destination"""
        pass


class Transformer(ABC):
    """Abstract base class for transformers"""
    
    @abstractmethod
    def transform(self, element: Dict[str, Any]) -> Dict[str, Any]:
        """Transform single element"""
        pass


class AuditLogger(beam.DoFn):
    """Base class for audit logging"""
    
    def __init__(self, step_name: str, config: Dict[str, Any] = None):
        self.step_name = step_name
        self.config = config or {}
        self.start_time = None
        self.counter = beam.metrics.Metrics.counter(step_name, 'processed')
        self.error_counter = beam.metrics.Metrics.counter(step_name, 'errors')
        
    def start_bundle(self):
        """Called before processing bundle"""
        self.start_time = datetime.utcnow()
        
    def process(self, element, *args, **kwargs):
        """Process element and count"""
        self.counter.inc()
        yield element
        
    def finish_bundle(self):
        """Called after processing bundle"""
        duration = (datetime.utcnow() - self.start_time).total_seconds() if self.start_time else 0
        logger.info(f"Audit [{self.step_name}] Bundle processed in {duration:.2f} seconds")


class WindowedAuditLogger(beam.DoFn):
    """Audit logger with window tracking for streaming"""
    
    def __init__(self, step_name: str):
        self.step_name = step_name
        self.window_records = {}
        
    def process(self, element, window=beam.DoFn.WindowParam):
        """Process element and track window information"""
        window_key = f"{window.start}_{window.end}"
        
        if window_key not in self.window_records:
            self.window_records[window_key] = {
                'count': 0,
                'start_time': window.start,
                'end_time': window.end,
                'first_process': datetime.utcnow()
            }
        
        self.window_records[window_key]['count'] += 1
        self.window_records[window_key]['last_process'] = datetime.utcnow()
        
        yield element
        
    def finish_bundle(self):
        """Log window statistics"""
        for window_key, stats in self.window_records.items():
            logger.info(
                f"Audit [{self.step_name}] Window {window_key}: "
                f"{stats['count']} records, "
                f"Duration: {stats['first_process']} to {stats.get('last_process', 'N/A')}"
            )
        self.window_records.clear()


class ErrorHandler(beam.DoFn):
    """Handle and route errors in pipeline"""
    
    def __init__(self, error_table: str = None, max_errors_percent: float = 0.1):
        self.error_table = error_table
        self.max_errors_percent = max_errors_percent
        self.error_counter = beam.metrics.Metrics.counter('pipeline', 'errors')
        self.total_counter = beam.metrics.Metrics.counter('pipeline', 'total')
        
    def process(self, element):
        """Process element and handle errors"""
        self.total_counter.inc()
        
        if element.get('_error'):
            self.error_counter.inc()
            
            # Route to error output if configured
            if self.error_table:
                yield beam.pvalue.TaggedOutput('errors', {
                    'record': element,
                    'error': element.get('_error'),
                    'timestamp': datetime.utcnow().isoformat()
                })
            else:
                logger.error(f"Pipeline error: {element.get('_error')}")
        else:
            # Pass through successful records
            yield beam.pvalue.TaggedOutput('success', element)


class MetricsCollector:
    """Collect and aggregate pipeline metrics"""
    
    def __init__(self):
        self.metrics = {}
        
    def add_metric(self, name: str, value: Any, metric_type: str = 'counter'):
        """Add a metric value"""
        if name not in self.metrics:
            self.metrics[name] = {
                'type': metric_type,
                'values': []
            }
        self.metrics[name]['values'].append(value)
        
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics"""
        summary = {}
        
        for name, data in self.metrics.items():
            if data['type'] == 'counter':
                summary[name] = sum(data['values'])
            elif data['type'] == 'gauge':
                summary[name] = data['values'][-1] if data['values'] else 0
            elif data['type'] == 'histogram':
                values = data['values']
                summary[name] = {
                    'count': len(values),
                    'sum': sum(values),
                    'avg': sum(values) / len(values) if values else 0,
                    'min': min(values) if values else 0,
                    'max': max(values) if values else 0
                }
        
        return summary
    
    def reset(self):
        """Reset all metrics"""
        self.metrics.clear()


class PipelineContext:
    """Context object for sharing state across pipeline steps"""
    
    def __init__(self):
        self.state = {}
        self.metrics = MetricsCollector()
        self.errors = []
        
    def set(self, key: str, value: Any):
        """Set context value"""
        self.state[key] = value
        
    def get(self, key: str, default: Any = None) -> Any:
        """Get context value"""
        return self.state.get(key, default)
        
    def add_error(self, error: str, element: Any = None):
        """Add error to context"""
        self.errors.append({
            'error': error,
            'element': element,
            'timestamp': datetime.utcnow().isoformat()
        })
        
    def has_errors(self) -> bool:
        """Check if context has errors"""
        return len(self.errors) > 0
    
    def get_summary(self) -> Dict[str, Any]:
        """Get context summary"""
        return {
            'state': self.state,
            'metrics': self.metrics.get_summary(),
            'error_count': len(self.errors),
            'errors': self.errors[:10]  # First 10 errors
        }


def create_test_pipeline(config: Dict[str, Any]) -> beam.Pipeline:
    """Create a test pipeline for unit testing"""
    from apache_beam.testing.test_pipeline import TestPipeline
    from apache_beam.options.pipeline_options import PipelineOptions
    
    options = PipelineOptions(**config)
    return TestPipeline(options=options)


def format_table_id(project: str, dataset: str, table: str) -> str:
    """Format BigQuery table ID"""
    return f"{project}.{dataset}.{table}"


def parse_table_id(table_id: str) -> Dict[str, str]:
    """Parse BigQuery table ID into components"""
    parts = table_id.split('.')
    if len(parts) != 3:
        raise ValueError(f"Invalid table ID format: {table_id}")
    
    return {
        'project': parts[0],
        'dataset': parts[1],
        'table': parts[2]
    }


def safe_json_loads(json_str: str, default: Any = None) -> Any:
    """Safely load JSON with default value"""
    import json
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return default


def batch_elements(elements: List[Any], batch_size: int) -> List[List[Any]]:
    """Batch elements into chunks"""
    for i in range(0, len(elements), batch_size):
        yield elements[i:i + batch_size]
