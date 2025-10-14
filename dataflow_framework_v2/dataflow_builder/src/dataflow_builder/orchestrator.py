"""
Orchestrate a Beam pipeline with deferred step loading for split packages.
"""

from __future__ import annotations

import re
import importlib
import logging
from typing import Any, Dict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from .config import PipelineConfig
from .registry import STEP_REGISTRY

logger = logging.getLogger(__name__)

_TOKEN = re.compile(r"\{([a-zA-Z0-9_\.]+)\}")


def _get_by_path(obj: Any, path: str) -> Any:
    """Get a nested attribute or key from a config object."""
    parts = path.split(".")
    cur = obj
    for part in parts:
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            cur = getattr(cur, part, None)
    return cur


def _format_value(value: str, cfg: PipelineConfig) -> str:
    """Format a string using config fields for placeholders."""
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        val = _get_by_path(cfg, key)
        return "" if val is None else str(val)
    return _TOKEN.sub(repl, value)


class DeferredStepDoFn(beam.DoFn):
    """DoFn that loads and executes step on worker."""
    
    def __init__(self, step_name: str, step_path: str, spec: Dict, config: PipelineConfig):
        self.step_name = step_name
        self.step_path = step_path
        self.spec = spec
        self.config = config
        self.step_instance = None
        
    def setup(self):
        """Load the actual step class on worker."""
        try:
            module_path, class_name = self.step_path.rsplit('.', 1)
            module = importlib.import_module(module_path)
            step_class = getattr(module, class_name)
            
            # Create instance with minimal state
            self.step_instance = step_class(
                spec=self.spec,
                config=self.config,
                state={}
            )
            logger.info(f"Loaded step {self.step_name} on worker")
        except Exception as e:
            logger.error(f"Failed to load step {self.step_name}: {e}")
            raise
            
    def process(self, element):
        """Process element using the step."""
        if hasattr(self.step_instance, 'process'):
            yield from self.step_instance.process(element)
        else:
            yield element


class Orchestrator:
    """Construct and run a Beam pipeline from a PipelineConfig."""

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.state: Dict[str, Any] = {}

    def run(self, pipeline_options: PipelineOptions | None = None) -> Dict[str, Any]:
        """Execute the pipeline according to the configured plan."""
        cfg = self.config
        plan = cfg.plan or []
    
        # Format schema fields if they exist
        if cfg.schema and cfg.schema.bq:
            if cfg.schema.bq.project:
                cfg.schema.bq.project = _format_value(cfg.schema.bq.project, cfg)
            if cfg.schema.bq.dataset:
                cfg.schema.bq.dataset = _format_value(cfg.schema.bq.dataset, cfg)
            if cfg.schema.bq.table:
                cfg.schema.bq.table = _format_value(cfg.schema.bq.table, cfg)
            if cfg.schema.bq.query:
                cfg.schema.bq.query = _format_value(cfg.schema.bq.query, cfg)
        
        # Format string fields in the plan prior to execution
        for spec in plan:
            for key, val in list(spec.items()):
                if isinstance(val, str):
                    spec[key] = _format_value(val, cfg)
                    
        # Run the pipeline
        with beam.Pipeline(options=pipeline_options) as p:
            self.state["__pipeline__"] = p
            
            for idx, spec in enumerate(plan):
                step_name = spec.get("step")
                if not step_name:
                    raise ValueError(f"Plan step #{idx} is missing the 'step' field: {spec}")
                
                # Get step path from registry
                step_path = STEP_REGISTRY.get(step_name)
                if step_path is None:
                    raise ValueError(f"Unknown step type '{step_name}' at plan index {idx}")
                
                # Try to import step class locally (for driver-side steps)
                try:
                    module_path, class_name = step_path.rsplit('.', 1)
                    module = importlib.import_module(module_path)
                    cls = getattr(module, class_name)
                    
                    # Step is available locally, use it directly
                    step = cls(spec=spec, config=cfg, state=self.state)
                    output = step.execute(p)
                    
                except (ImportError, AttributeError) as e:
                    # Step not available on driver, defer to worker
                    logger.info(f"Step {step_name} not available on driver, deferring to worker")
                    
                    # Check if this is a source step or transform
                    input_key = spec.get("in")
                    
                    if input_key and input_key in self.state:
                        # Transform step - apply to existing PCollection
                        pcoll = self.state[input_key]
                        output = pcoll | f"{step_name}_{idx}" >> beam.ParDo(
                            DeferredStepDoFn(step_name, step_path, spec, cfg)
                        )
                    else:
                        # Source step - need special handling
                        # For now, create a dummy element to trigger the step
                        output = (
                            p 
                            | f"Create_{step_name}_{idx}" >> beam.Create([None])
                            | f"{step_name}_{idx}" >> beam.ParDo(
                                DeferredStepDoFn(step_name, step_path, spec, cfg)
                            )
                        )
                
                # Store output in state
                out_key = spec.get("out") or spec.get("id")
                if out_key:
                    self.state[out_key] = output
                    
        return self.state
