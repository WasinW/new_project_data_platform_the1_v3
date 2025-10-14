# dataflow_common/src/dataflow_common/steps/bigtable_batch_steps.py

"""
BigTable Batch Processing Steps
Cost-optimized batch operations
"""

from typing import Dict, Any
import apache_beam as beam
from ..core import BaseStep
from ..connectors.bigtable_batch import BigTableBatchConnector
import logging

LOGGER = logging.getLogger(__name__)


class ReadBigTableBatchStep(BaseStep):
    """
    Optimized batch read from BigTable
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
        
        # Batch-specific settings
        batch_size = self.spec.get("batch_size", 100)
        max_batch_wait_ms = self.spec.get("max_batch_wait_ms", 200)
        enable_cache = self.spec.get("enable_cache", True)
        cache_ttl = self.spec.get("cache_ttl_seconds", 300)
        
        LOGGER.info(
            f"[{self.step_id}] Batch read configuration: "
            f"batch_size={batch_size}, max_wait={max_batch_wait_ms}ms, "
            f"cache={enable_cache}, project={project}"
        )
        
        return BigTableBatchConnector.read_batch(
            keys=keys,
            project_id=project,
            instance_id=instance,
            table_id=table,
            column_families=column_families,
            batch_size=batch_size,
            max_batch_wait_ms=max_batch_wait_ms,
            enable_cache=enable_cache,
            cache_ttl_seconds=cache_ttl,
            label=self.step_id
        )


class WriteBigTableBatchStep(BaseStep):
    """
    Optimized batch write to BigTable
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
        
        # Batch-specific settings
        batch_size = self.spec.get("batch_size", 500)
        max_batch_wait_ms = self.spec.get("max_batch_wait_ms", 1000)
        
        LOGGER.info(
            f"[{self.step_id}] Batch write configuration: "
            f"batch_size={batch_size}, max_wait={max_batch_wait_ms}ms"
        )
        
        BigTableBatchConnector.write_batch(
            records=records,
            project_id=project,
            instance_id=instance,
            table_id=table,
            column_family=column_family,
            batch_size=batch_size,
            max_batch_wait_ms=max_batch_wait_ms,
            label=self.step_id
        )
        
        return None