# pack_deploy/ms_personas/scripts/unittest/conftest.py
"""Pytest configuration"""
import warnings
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
dataflow_common_path = project_root / "dataflow_common" / "src"
sys.path.insert(0, str(dataflow_common_path))

# Configure warnings
def pytest_configure(config):
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="google._upb._message")
    warnings.filterwarnings("ignore", message="cannot collect test class 'TestPipeline'")
    warnings.filterwarnings("ignore", message="datetime.datetime.utcnow")