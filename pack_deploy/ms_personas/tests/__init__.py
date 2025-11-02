"""Test suite for dataflow_common package"""
import os
import sys
from pathlib import Path

# Add dataflow_common to path
project_root = Path(__file__).parent.parent
dataflow_common_path = project_root / "scripts" / "dataflow_common" / "src"
sys.path.insert(0, str(dataflow_common_path))

# Test configuration
TEST_CONFIG = {
    "test_project": "test-project",
    "test_dataset": "test_dataset",
    "test_bucket": "test-bucket",
    "test_temp_location": "gs://test-bucket/temp"
}

__all__ = ['TEST_CONFIG']