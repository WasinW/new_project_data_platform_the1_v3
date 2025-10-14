# dataflow_common/src/dataflow_common/steps/bigtable_realtime_steps.py

"""
BigTable Real-time Processing Steps
Low-latency per-message operations
"""

from typing import Dict, Any
import apache_beam as beam
from dataflow_worker.core import BaseStep
from dataflow_worker.connectors.bigtable_realtime import BigTableRealtimeConnector
import logging

LOGGER = logging.getLogger(__name__)


class ReadBigTableRealtimeStep(BaseStep):
    """
    Real-time read from BigTable with advanced caching
    """
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing input '{input_key}'")
        
        keys = self.state[input_key]
        
        # Get configuration
        bt_config = self.config.io.get("bigtable", {})
        project = self.spec.get("project") or bt_config.get("project")
        instance = self.spec.get("instance") or bt_config.get("instance")
        table = self.spec.get("table") or bt_config.get("table")
        column_families = self.spec.get("column_families") or bt_config.get("column_families", ["profiles"])
        
        # Cache configuration
        cache_config = self.spec.get("cache_config", {
            'hot_cache_size': 1000,
            'hot_cache_ttl': 60,
            'warm_cache_size': 10000,
            'warm_cache_ttl': 300
        })
        
        # Advanced features
        enable_prefetch = self.spec.get("enable_prefetch", True)
        circuit_breaker_threshold = self.spec.get("circuit_breaker_threshold", 5)
        
        LOGGER.info(
            f"[{self.step_id}] Real-time read configuration: "
            f"hot_cache={cache_config['hot_cache_size']}, "
            f"prefetch={enable_prefetch}, project={project}"
        )
        
        return BigTableRealtimeConnector.read_realtime(
            keys=keys,
            project_id=project,
            instance_id=instance,
            table_id=table,
            column_families=column_families,
            cache_config=cache_config,
            enable_prefetch=enable_prefetch,
            circuit_breaker_threshold=circuit_breaker_threshold,
            label=self.step_id
        )


class WriteBigTableRealtimeStep(BaseStep):
    """
    Real-time write to BigTable
    """
    
    def execute(self, pipeline: beam.Pipeline) -> None:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing input '{input_key}'")
        
        records = self.state[input_key]
        
        # Get configuration
        bt_config = self.config.io.get("bigtable", {})
        project = self.spec.get("project") or bt_config.get("project")
        instance = self.spec.get("instance") or bt_config.get("instance")
        table = self.spec.get("table") or bt_config.get("table")
        column_family = self.spec.get("column_family", "profiles")
        
        # Real-time specific
        async_write = self.spec.get("async_write", True)
        
        LOGGER.info(
            f"[{self.step_id}] Real-time write configuration: "
            f"async={async_write}"
        )
        
        BigTableRealtimeConnector.write_realtime(
            records=records,
            project_id=project,
            instance_id=instance,
            table_id=table,
            column_family=column_family,
            async_write=async_write,
            label=self.step_id
        )
        
        return None