"""
Orchestrate a Beam pipeline based on a configuration plan.

The :class:`Orchestrator` class walks through a pipeline plan defined
in a :class:`~dataflow_common.config.PipelineConfig` instance,
instantiates the corresponding step classes from the registry and
executes them in order.  Each step consumes input PCollections from
the orchestration state and stores its output back into the state.

Before a pipeline run string values in the plan are formatted with
values from the config using ``str.format`` syntax.  Placeholders
can reference nested fields in the config via dot notation, e.g.
``{io.bq.project}`` or ``{params.run_dt}``.
"""

from __future__ import annotations

import re
from typing import Any, Dict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from .config import PipelineConfig
from .registry import STEP_REGISTRY


_TOKEN = re.compile(r"\{([a-zA-Z0-9_\.]+)\}")


def _get_by_path(obj: Any, path: str) -> Any:
    """Get a nested attribute or key from a config object.

    Supports dot‑separated lookups through dataclasses (attributes)
    and dictionaries (keys).  Returns ``None`` if the path is not
    found.
    """
    parts = path.split(".")
    cur = obj
    for part in parts:
        if cur is None:
            return None
        # support dataclass attributes and dict keys
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            cur = getattr(cur, part, None)
    return cur


def _format_value(value: str, cfg: PipelineConfig) -> str:
    """Format a string using config fields for placeholders.

    Placeholders of the form ``{io.bq.project}`` will be replaced
    with the corresponding attribute or key from ``cfg``.  Missing
    placeholders are replaced with an empty string.
    """
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        val = _get_by_path(cfg, key)
        return "" if val is None else str(val)
    return _TOKEN.sub(repl, value)


class Orchestrator:
    """Construct and run a Beam pipeline from a :class:`PipelineConfig`.

    Parameters
    ----------
    config: :class:`PipelineConfig`
        The parsed pipeline configuration.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        # state holds intermediate PCollections keyed by the step's
        # output identifier.  It also stores the pipeline object
        # under the special key ``__pipeline__``.
        self.state: Dict[str, Any] = {}

    def run(self, pipeline_options: PipelineOptions | None = None) -> Dict[str, Any]:
        """Execute the pipeline according to the configured plan.

        Parameters
        ----------
        pipeline_options: :class:`~apache_beam.options.pipeline_options.PipelineOptions`, optional
            Custom pipeline options to pass to the Beam pipeline.  If
            ``None`` a default set of options will be used.

        Returns
        -------
        dict
            A mapping of step outputs and internal state accumulated
            during the run.
        """
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
            # Provide global config to steps via state
            for idx, spec in enumerate(plan):
                step_name = spec.get("step")
                if not step_name:
                    raise ValueError(f"Plan step #{idx} is missing the 'step' field: {spec}")
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
                                
                                # Each step will read from state and write to it
                                step = cls(spec=spec, config=cfg, state=self.state)
                # The execute method must return a PCollection or None
                output = step.execute(p)
                out_key = spec.get("out") or spec.get("id")
                if out_key:
                    self.state[out_key] = output
        return self.state