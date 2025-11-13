"""Test suite for dataflow_common package"""
import os
import sys
import warnings
from pathlib import Path

# Suppress specific warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="google._upb._message")
warnings.filterwarnings("ignore", message="cannot collect test class 'TestPipeline'")

# Add dataflow_common to path
project_root = Path(__file__).parent.parent.parent  # ขึ้นไป 3 ระดับจาก testcase
# dataflow_common_path = project_root / "scripts" / "dataflow_common" / "src"
dataflow_common_path = project_root / "dataflow_common" / "src"
sys.path.insert(0, str(dataflow_common_path))

# Test configuration
TEST_CONFIG = {
    "test_project": "test-project",
    "test_dataset": "test_dataset",
    "test_bucket": "test-bucket",
    "test_temp_location": "gs://test-bucket/temp"
}

__all__ = ['TEST_CONFIG']