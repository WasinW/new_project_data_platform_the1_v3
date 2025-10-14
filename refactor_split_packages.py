#!/usr/bin/env python
"""
Script to split dataflow_common into dataflow_builder and dataflow_worker packages
Fixed for Windows encoding issues
Usage: python refactor_split_packages.py
"""

import os
import shutil
import re
from pathlib import Path
from typing import Dict, List, Tuple

class DataflowRefactorer:
    def __init__(self, source_dir: str = "dataflow_common", target_dir: str = "dataflow_framework_v2"):
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self.builder_dir = self.target_dir / "dataflow_builder"
        self.worker_dir = self.target_dir / "dataflow_worker"
        
    def run(self):
        """Execute the refactoring process"""
        print("Starting Dataflow Refactoring...")
        
        # Step 1: Create directory structure
        self.create_directory_structure()
        
        # Step 2: Copy files to appropriate packages
        self.copy_files()
        
        # Step 3: Update imports
        self.update_imports()
        
        # Step 4: Create setup files
        self.create_setup_files()
        
        # Step 5: Create modified files
        self.create_modified_files()
        
        print("Refactoring completed successfully!")
        print(f"New packages created in: {self.target_dir}")
        
    def create_directory_structure(self):
        """Create the new package structure"""
        print("Creating directory structure...")
        
        # Builder package
        (self.builder_dir / "src" / "dataflow_builder").mkdir(parents=True, exist_ok=True)
        
        # Worker package  
        (self.worker_dir / "src" / "dataflow_worker").mkdir(parents=True, exist_ok=True)
        (self.worker_dir / "src" / "dataflow_worker" / "steps").mkdir(exist_ok=True)
        (self.worker_dir / "src" / "dataflow_worker" / "transforms").mkdir(exist_ok=True)
        (self.worker_dir / "src" / "dataflow_worker" / "connectors").mkdir(exist_ok=True)
        
    def copy_files(self):
        """Copy files to appropriate packages"""
        print("Copying files...")
        
        source_base = self.source_dir / "src" / "dataflow_common"
        
        # Files for dataflow_builder (Driver side)
        builder_files = [
            "config.py",
            "orchestrator.py",
            "__init__.py"
        ]
        
        for file in builder_files:
            src = source_base / file
            if src.exists():
                dst = self.builder_dir / "src" / "dataflow_builder" / file
                shutil.copy2(src, dst)
                print(f"  Copied {file} to builder")
        
        # Files for dataflow_worker (Worker side)
        worker_files = [
            "core.py",
            "__init__.py"
        ]
        
        for file in worker_files:
            src = source_base / file
            if src.exists():
                dst = self.worker_dir / "src" / "dataflow_worker" / file
                shutil.copy2(src, dst)
                print(f"  Copied {file} to worker")
        
        # Copy entire directories
        for subdir in ["steps", "transforms", "connectors"]:
            src_dir = source_base / subdir
            if src_dir.exists():
                dst_dir = self.worker_dir / "src" / "dataflow_worker" / subdir
                shutil.copytree(src_dir, dst_dir, dirs_exist_ok=True)
                print(f"  Copied {subdir}/ to worker")
                
    def update_imports(self):
        """Update import statements in copied files"""
        print("Updating imports...")
        
        # Update worker files
        worker_base = self.worker_dir / "src" / "dataflow_worker"
        
        # Import replacements for worker files
        worker_replacements = [
            (r'from \.\.config import', 'from dataflow_builder.config import'),
            (r'from \.\.core import', 'from ..core import'),
            (r'from \.\.transforms import', 'from ..transforms import'),
            (r'from \.\.connectors import', 'from ..connectors import'),
            (r'from dataflow_common\.', 'from dataflow_worker.'),
        ]
        
        # Apply replacements to all Python files in worker
        for py_file in worker_base.rglob("*.py"):
            self._update_file_imports(py_file, worker_replacements)
            
    def _update_file_imports(self, file_path: Path, replacements: List[Tuple[str, str]]):
        """Update imports in a single file"""
        try:
            # Use UTF-8 encoding explicitly
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original = content
            for pattern, replacement in replacements:
                content = re.sub(pattern, replacement, content)
            
            if content != original:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"  Updated imports in {file_path.name}")
        except Exception as e:
            print(f"  Error updating {file_path.name}: {e}")
            
    def create_setup_files(self):
        """Create setup.py and pyproject.toml for each package"""
        print("Creating setup files...")
        
        # Builder setup.py
        builder_setup = '''from setuptools import setup, find_packages

setup(
    name="dataflow-builder",
    version="2.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "apache-beam>=2.59.0",
        "pyyaml>=6.0",
    ],
    description="Dataflow pipeline builder (driver side)",
)
'''
        (self.builder_dir / "setup.py").write_text(builder_setup, encoding='utf-8')
        
        # Worker setup.py
        worker_setup = '''from setuptools import setup, find_packages

setup(
    name="dataflow-worker",
    version="2.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "dataflow-builder>=2.0.0",  # For shared config
        "apache-beam>=2.59.0",
        "google-cloud-bigquery>=3.25.0",
        "pyarrow>=14.0.0",
        "boto3>=1.34.0",
        "pyyaml>=6.0",
    ],
    description="Dataflow pipeline worker (execution side)",
)
'''
        (self.worker_dir / "setup.py").write_text(worker_setup, encoding='utf-8')
        
        print("  Created setup.py files")
        
    def create_modified_files(self):
        """Create modified registry.py and orchestrator.py"""
        print("Creating modified core files...")
        
        # Modified registry.py for builder
        registry_content = '''"""
Step registry with string references (no direct imports)
"""

# String references to step implementations
# These will be dynamically imported on the worker
STEP_REGISTRY = {
    "ReadBQQuery": "dataflow_worker.steps.ReadBQQueryStep",
    "BuildMappingDict": "dataflow_worker.steps.BuildMappingDictStep",
    "ParseProfiles": "dataflow_worker.steps.ParseProfilesStep",
    "MapRecord": "dataflow_worker.steps.MapRecordStep",
    "KVPairs": "dataflow_worker.steps.KVPairsStep",
    "CoGroupByKey": "dataflow_worker.steps.CoGroupByKeyStep",
    "CoalesceByMapping": "dataflow_worker.steps.CoalesceByMappingStep",
    "NormalizeToSchema": "dataflow_worker.steps.NormalizeToSchemaStep",
    "WriteParquet": "dataflow_worker.steps.WriteParquetStep",
    "ConsumePubSub": "dataflow_worker.steps.ConsumePubSubStep",
    "ExtractKeys": "dataflow_worker.steps.ExtractKeysStep",
    "ReadBigTableRealtime": "dataflow_worker.steps.ReadBigTableRealtimeStep",
    "ProcessWithDLQ": "dataflow_worker.steps.ProcessWithDLQStep",
    "Window": "dataflow_worker.steps.WindowStep",
    "WriteToBigQuery": "dataflow_worker.steps.WriteToBigQueryStep",
    "CreateFixedMapping": "dataflow_worker.steps.CreateFixedMappingStep",
    "CreateEmpty": "dataflow_worker.steps.CreateEmptyStep",
    "WriteGCS": "dataflow_worker.steps.WriteGCSStep",
    "GetNewMaxDate": "dataflow_worker.steps.GetNewMaxDateStep",
    "SetMaxDateParam": "dataflow_worker.steps.SetMaxDateParamStep",
    
    # BigTable steps
    "ReadBigTableBatch": "dataflow_worker.steps.bigtable_batch_steps.ReadBigTableBatchStep",
    "WriteBigTableBatch": "dataflow_worker.steps.bigtable_batch_steps.WriteBigTableBatchStep",
    "ReadBigTableRealtime": "dataflow_worker.steps.bigtable_realtime_steps.ReadBigTableRealtimeStep",
    "WriteBigTableRealtime": "dataflow_worker.steps.bigtable_realtime_steps.WriteBigTableRealtimeStep",
}

__all__ = ["STEP_REGISTRY"]
'''
        (self.builder_dir / "src" / "dataflow_builder" / "registry.py").write_text(
            registry_content, encoding='utf-8'
        )
        
        # Create orchestrator patch
        self._create_orchestrator_patch()
        
        print("  Created registry.py with string references")
        print("  Created orchestrator_patch.py")
        
    def _create_orchestrator_patch(self):
        """Create a patch file for orchestrator.py modifications"""
        patch_content = '''"""
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
'''
        
        (self.builder_dir / "orchestrator_patch.py").write_text(
            patch_content, encoding='utf-8'
        )

def create_build_scripts():
    """Create build and deployment scripts"""
    
    # Build script for Windows
    build_script_bat = '''@echo off
REM Build script for dataflow packages (Windows)

echo Building dataflow packages...

REM Build dataflow-builder wheel
echo Building dataflow-builder...
cd dataflow_framework_v2\\dataflow_builder
python -m pip install build
python -m build
echo Built dataflow-builder

REM Build dataflow-worker
echo Preparing dataflow-worker...
cd ..\\dataflow_worker
python -m build
echo Built dataflow-worker

cd ..\\..
echo Build completed!
echo.
echo Next steps:
echo 1. Upload wheel: gsutil cp dataflow_framework_v2\\dataflow_builder\\dist\\*.whl gs://t1-airflow-composer-bucket/dags/packages/
echo 2. Build Docker image with Dockerfile
'''
    
    Path("build_packages.bat").write_text(build_script_bat, encoding='utf-8')
    
    # Dockerfile
    dockerfile = '''FROM apache/beam_python3.11_sdk:2.59.0

# Copy wheels
COPY dataflow_framework_v2/dataflow_builder/dist/*.whl /tmp/
COPY dataflow_framework_v2/dataflow_worker/dist/*.whl /tmp/

# Install packages
RUN pip install /tmp/dataflow-builder-*.whl && \\
    pip install /tmp/dataflow-worker-*.whl && \\
    rm /tmp/*.whl

# Install additional dependencies
RUN pip install --no-cache-dir \\
    boto3==1.34.106 \\
    pyarrow==14.0.2 \\
    google-cloud-bigquery==3.25.0

WORKDIR /
'''
    
    Path("Dockerfile").write_text(dockerfile, encoding='utf-8')
    
    # Test script
    test_script = '''#!/usr/bin/env python
"""Test the refactored packages locally"""

import sys
sys.path.insert(0, 'dataflow_framework_v2/dataflow_builder/src')
sys.path.insert(0, 'dataflow_framework_v2/dataflow_worker/src')

from dataflow_builder.config import load_config
from dataflow_builder.registry import STEP_REGISTRY

def test_basic_import():
    """Test that packages can be imported"""
    print("Testing imports...")
    
    # Test builder imports
    try:
        from dataflow_builder import config, orchestrator, registry
        print("  OK: dataflow_builder imports")
    except ImportError as e:
        print(f"  FAIL: dataflow_builder import failed: {e}")
        return False
    
    # Test worker imports  
    try:
        from dataflow_worker import core, steps, transforms, connectors
        print("  OK: dataflow_worker imports")
    except ImportError as e:
        print(f"  FAIL: dataflow_worker import failed: {e}")
        return False
        
    return True

def test_registry():
    """Test that registry uses string references"""
    print("\\nTesting registry...")
    
    for step_name, step_path in STEP_REGISTRY.items():
        if not isinstance(step_path, str):
            print(f"  FAIL: {step_name} is not a string reference!")
            return False
        if not step_path.startswith("dataflow_worker."):
            print(f"  FAIL: {step_name} doesn't reference dataflow_worker!")
            return False
    
    print(f"  OK: Registry has {len(STEP_REGISTRY)} string references")
    return True

def test_dynamic_import():
    """Test dynamic import of step"""
    print("\\nTesting dynamic import...")
    
    import importlib
    
    # Try to import a step dynamically
    step_path = STEP_REGISTRY.get("ReadBQQuery")
    if step_path:
        try:
            module_path, class_name = step_path.rsplit('.', 1)
            module = importlib.import_module(module_path)
            step_class = getattr(module, class_name)
            print(f"  OK: Successfully imported {class_name}")
            return True
        except Exception as e:
            print(f"  FAIL: Failed to import: {e}")
            return False
    
    return False

if __name__ == "__main__":
    print("Testing refactored packages...\\n")
    
    results = []
    results.append(test_basic_import())
    results.append(test_registry())
    results.append(test_dynamic_import())
    
    if all(results):
        print("\\n=== All tests passed! ===")
    else:
        print("\\n=== Some tests failed ===")
        sys.exit(1)
'''
    
    Path("test_refactor.py").write_text(test_script, encoding='utf-8')

if __name__ == "__main__":
    # Run the refactoring
    refactorer = DataflowRefactorer()
    refactorer.run()
    
    # Create additional scripts
    create_build_scripts()
    
    print("\nAdditional files created:")
    print("  - build_packages.bat - Build script for Windows")
    print("  - Dockerfile - Docker build file")
    print("  - test_refactor.py - Test script")
    print("\nRun 'python test_refactor.py' to test the refactoring")
    print("Run 'build_packages.bat' to build packages")