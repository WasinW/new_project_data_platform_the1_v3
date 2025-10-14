"""
Orchestrator modifications for dynamic imports
Apply these changes to orchestrator.py
"""

# Add this import at the top
import importlib
import logging

LOGGER = logging.getLogger(__name__)

# Replace the step instantiation section in run() method:
# OLD CODE:
#     cls = STEP_REGISTRY.get(step_name)
#     if cls is None:
#         raise ValueError(f"Unknown step type '{step_name}' at plan index {idx}")
#
# NEW CODE:
def get_step_class(step_name: str):
    """Dynamically import step class from string reference"""
    from .registry import STEP_REGISTRY
    
    step_path = STEP_REGISTRY.get(step_name)
    if not step_path:
        raise ValueError(f"Unknown step type '{step_name}'")
    
    try:
        # Check if we're on driver or worker
        # Try direct import first (worker side)
        module_path, class_name = step_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    except ImportError as e:
        # On driver side - create a deferred loader
        LOGGER.warning(f"Step {step_name} not available on driver, using deferred loading")
        return create_deferred_step_class(step_path)

def create_deferred_step_class(step_path: str):
    """Create a deferred step class for driver side"""
    from dataflow_builder.config import PipelineConfig
    
    class DeferredStep:
        def __init__(self, *, spec, config, state):
            self.spec = spec
            self.config = config
            self.state = state
            self.step_path = step_path
            
        def execute(self, pipeline):
            # This will be serialized and executed on worker
            import apache_beam as beam
            
            # Use ParDo with deferred loading
            input_key = self.spec.get("in")
            if input_key and input_key in self.state:
                pcoll = self.state[input_key]
                return pcoll | beam.ParDo(DeferredStepDoFn(self.spec, self.config, self.step_path))
            else:
                # Source step
                return pipeline | beam.Create([None]) | beam.ParDo(
                    DeferredStepDoFn(self.spec, self.config, self.step_path)
                )
    
    return DeferredStep

class DeferredStepDoFn(beam.DoFn):
    """DoFn that loads step implementation on worker"""
    
    def __init__(self, spec, config, step_path):
        self.spec = spec
        self.config = config
        self.step_path = step_path
        self.step_instance = None
        
    def setup(self):
        """Load the actual step class on worker"""
        import importlib
        module_path, class_name = self.step_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        step_class = getattr(module, class_name)
        
        # Create instance with minimal state
        self.step_instance = step_class(
            spec=self.spec,
            config=self.config,
            state={}  # State will be managed differently
        )
        
    def process(self, element):
        # Delegate to actual step
        yield from self.step_instance.process(element)
