#!/usr/bin/env python
"""
Fix orchestrator.py to handle string references from registry
"""

import os
from pathlib import Path

def fix_orchestrator():
    """Update orchestrator.py to handle dynamic imports"""
    
    orchestrator_path = Path("dataflow_framework_v2/dataflow_builder/src/dataflow_builder/orchestrator.py")
    
    # Read current file
    with open(orchestrator_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find the line with STEP_REGISTRY import
    if "from .registry import STEP_REGISTRY" not in content:
        # Add import at the top
        import_section = """import importlib
from .registry import STEP_REGISTRY"""
        content = content.replace("from .registry import STEP_REGISTRY", import_section)
    
    # Replace the step instantiation section
    old_code = """cls = STEP_REGISTRY.get(step_name)
                if cls is None:
                    raise ValueError(f"Unknown step type '{step_name}' at plan index {idx}")
                # Each step will read from state and write to it
                step = cls(spec=spec, config=cfg, state=self.state)"""
    
    new_code = """step_path = STEP_REGISTRY.get(step_name)
                if step_path is None:
                    raise ValueError(f"Unknown step type '{step_name}' at plan index {idx}")
                
                # Dynamic import of step class
                try:
                    module_path, class_name = step_path.rsplit('.', 1)
                    module = importlib.import_module(module_path)
                    cls = getattr(module, class_name)
                except (ImportError, AttributeError) as e:
                    raise ValueError(f"Failed to import step '{step_name}' from '{step_path}': {e}")
                
                # Each step will read from state and write to it
                step = cls(spec=spec, config=cfg, state=self.state)"""
    
    # Try to replace the exact pattern
    if "cls = STEP_REGISTRY.get(step_name)" in content:
        # Find and replace the block
        lines = content.split('\n')
        new_lines = []
        i = 0
        while i < len(lines):
            if "cls = STEP_REGISTRY.get(step_name)" in lines[i]:
                # Replace this block
                indent = len(lines[i]) - len(lines[i].lstrip())
                new_block = new_code.replace('\n', '\n' + ' ' * indent)
                new_lines.append(' ' * indent + new_block.lstrip())
                # Skip old lines
                while i < len(lines) and "step = cls(spec=spec" not in lines[i]:
                    i += 1
                i += 1  # Skip the step = cls line too
            else:
                new_lines.append(lines[i])
                i += 1
        content = '\n'.join(new_lines)
    
    # Save updated file
    with open(orchestrator_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("✅ Fixed orchestrator.py")

# Alternative: Create a completely new orchestrator.py
def create_new_orchestrator():
    """Create a new orchestrator.py with dynamic imports"""
    
    new_orchestrator = '''"""
Orchestrate a Beam pipeline based on a configuration plan with dynamic imports.
"""

from __future__ import annotations

import re
import importlib
from typing import Any, Dict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from .config import PipelineConfig
from .registry import STEP_REGISTRY


_TOKEN = re.compile(r"\\{([a-zA-Z0-9_\\.]+)\\}")


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
                
                # Dynamic import of step class
                try:
                    module_path, class_name = step_path.rsplit('.', 1)
                    module = importlib.import_module(module_path)
                    cls = getattr(module, class_name)
                except (ImportError, AttributeError) as e:
                    raise ValueError(f"Failed to import step '{step_name}' from '{step_path}': {e}")
                
                # Create and execute step
                step = cls(spec=spec, config=cfg, state=self.state)
                output = step.execute(p)
                
                # Store output in state
                out_key = spec.get("out") or spec.get("id")
                if out_key:
                    self.state[out_key] = output
                    
        return self.state
'''
    
    orchestrator_path = Path("dataflow_framework_v2/dataflow_builder/src/dataflow_builder/orchestrator.py")
    
    # Backup old file
    backup_path = orchestrator_path.with_suffix('.py.bak')
    if orchestrator_path.exists():
        import shutil
        shutil.copy(orchestrator_path, backup_path)
        print(f"✅ Backed up old orchestrator.py to {backup_path}")
    
    # Write new file
    with open(orchestrator_path, 'w', encoding='utf-8') as f:
        f.write(new_orchestrator)
    
    print("✅ Created new orchestrator.py with dynamic imports")

if __name__ == "__main__":
    # Try to fix existing file first
    try:
        fix_orchestrator()
    except Exception as e:
        print(f"⚠️ Failed to patch existing file: {e}")
        print("Creating new orchestrator.py instead...")
        create_new_orchestrator()
    
    print("\n📦 Now rebuild the wheel:")
    print("cd dataflow_framework_v2/dataflow_builder")
    print("python setup.py bdist_wheel")
    print("\n📤 Then upload:")
    print("gsutil cp dist/dataflow-builder-2.0.0-py3-none-any.whl gs://t1-airflow-composer-bucket/dags/packages/")