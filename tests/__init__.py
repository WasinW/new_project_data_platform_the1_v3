"""
Test suite for dataflow_common package

This module initializes the test package and provides common utilities
for all test modules.
"""

import os
import sys
import logging
from pathlib import Path

# Add parent directory to Python path so we can import dataflow_common
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure test logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Disable verbose logging from external libraries during tests
logging.getLogger('apache_beam').setLevel(logging.WARNING)
logging.getLogger('google').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# Test configuration
TEST_CONFIG = {
    "test_project": "test-project",
    "test_dataset": "test_dataset",
    "test_bucket": "test-bucket",
    "test_temp_location": "gs://test-bucket/temp"
}

# Common test utilities
def get_test_config_path():
    """Get path to test config file"""
    return os.path.join(
        os.path.dirname(__file__), 
        "mock_data", 
        "sample_config.yaml"
    )

def get_test_schema_path():
    """Get path to test schema file"""
    return os.path.join(
        os.path.dirname(__file__),
        "mock_data",
        "sample_schema.json"
    )

def get_test_mapping_path():
    """Get path to test mapping file"""
    return os.path.join(
        os.path.dirname(__file__),
        "mock_data",
        "sample_mapping.json"
    )

# Mock data generators
def generate_mock_bq_record(num_records=10):
    """Generate mock BigQuery records"""
    records = []
    for i in range(num_records):
        records.append({
            "member_number": f"M{1000 + i}",
            "email": f"user{i}@example.com",
            "created_date": "2025-01-01",
            "updated_timestamp": "2025-01-01 12:00:00",
            "is_active": i % 2 == 0,
            "score": float(i * 10)
        })
    return records

def generate_mock_personas_record(num_records=10):
    """Generate mock personas records"""
    import json
    records = []
    for i in range(num_records):
        records.append({
            "personaId": f"P{2000 + i}",
            "profiles": json.dumps({
                "memberId": f"M{1000 + i}",
                "email": f"user{i}@example.com",
                "firstName": f"First{i}",
                "lastName": f"Last{i}"
            }),
            "status": "active" if i % 2 == 0 else "inactive",
            "timestamp": "2025-01-01 12:00:00"
        })
    return records

def generate_mock_mapping_rows():
    """Generate mock mapping configuration"""
    return [
        {
            "RECONCILE_COLUMN_NAME": "MEMBER_NUMBER",
            "PERSONAS_MAPPING_COLUMN_NAME": "profiles.memberId",
            "RECONCILE_RETRIEVED": True,
            "RECONCILE_CONFIRMED": False,
            "UPDATED_DATE": "2025-01-01"
        },
        {
            "RECONCILE_COLUMN_NAME": "EMAIL",
            "PERSONAS_MAPPING_COLUMN_NAME": "profiles.email",
            "RECONCILE_RETRIEVED": True,
            "RECONCILE_CONFIRMED": True,
            "UPDATED_DATE": "2025-01-01"
        },
        {
            "RECONCILE_COLUMN_NAME": "FIRST_NAME",
            "PERSONAS_MAPPING_COLUMN_NAME": "profiles.firstName",
            "RECONCILE_RETRIEVED": True,
            "RECONCILE_CONFIRMED": False,
            "UPDATED_DATE": "2025-01-01"
        },
        {
            "RECONCILE_COLUMN_NAME": "LAST_NAME",
            "PERSONAS_MAPPING_COLUMN_NAME": "profiles.lastName",
            "RECONCILE_RETRIEVED": False,
            "RECONCILE_CONFIRMED": True,
            "UPDATED_DATE": "2025-01-01"
        }
    ]

# Test fixtures
class TestFixtures:
    """Common test fixtures"""
    
    @staticmethod
    def get_sample_config_dict():
        """Get sample configuration dictionary"""
        return {
            "pipeline": {
                "name": "test_pipeline",
                "mode": "batch",
                "term": "short"
            },
            "params": {
                "pk": "member_number",
                "run_dt": "2025010100",
                "max_date": None,
                "run_par_month": "202501",
                "run_par_day": "01",
                "run_par_hour": "00"
            },
            "io": {
                "bq": {
                    "project": TEST_CONFIG["test_project"],
                    "dataset": TEST_CONFIG["test_dataset"],
                    "temp_gcs": TEST_CONFIG["test_temp_location"]
                },
                "s3": {
                    "refined_prefix": f"s3://{TEST_CONFIG['test_bucket']}/refined",
                    "num_shards": 2,
                    "region": "ap-southeast-1"
                }
            },
            "formats": {
                "date": ["%Y-%m-%d"],
                "timestamp": ["%Y-%m-%d %H:%M:%S"]
            },
            "schema": {
                "gcs_uri": None,
                "bq": {
                    "project": TEST_CONFIG["test_project"],
                    "dataset": TEST_CONFIG["test_dataset"],
                    "table": "test_schema"
                }
            },
            "plan": []
        }
    
    @staticmethod
    def get_sample_step_specs():
        """Get sample step specifications"""
        return [
            {
                "step": "ReadBQQuery",
                "id": "mapping_rows",
                "out": "mapping_rows",
                "query": "SELECT * FROM mapping_table"
            },
            {
                "step": "BuildMappingDict",
                "in": "mapping_rows",
                "out": "mapping_dict",
                "mapping_fields": {
                    "src_field": "PERSONAS_MAPPING_COLUMN_NAME",
                    "dest_field": "RECONCILE_COLUMN_NAME",
                    "retrieved_flag_field": "RECONCILE_RETRIEVED",
                    "confirmed_flag_field": "RECONCILE_CONFIRMED"
                }
            },
            {
                "step": "ParseJson",
                "in": "personas_rows",
                "out": "personas_parsed",
                "json_fields": ["profiles"]
            },
            {
                "step": "MapRecord",
                "in": "personas_parsed",
                "side": "mapping_dict",
                "out": "mapped_records",
                "mode": "reconcile"
            },
            {
                "step": "KVPairs",
                "in": "mapped_records",
                "out": "kv_records",
                "key_field": "member_number"
            },
            {
                "step": "WriteParquet",
                "in": "final_data",
                "prefix": "{io.s3.refined_prefix}/output/run_dt={params.run_dt}"
            }
        ]

# Test assertions
class TestAssertions:
    """Custom assertions for dataflow tests"""
    
    @staticmethod
    def assert_valid_record(record, required_fields):
        """Assert record has all required fields"""
        for field in required_fields:
            assert field in record, f"Missing required field: {field}"
            assert record[field] is not None, f"Field {field} should not be None"
    
    @staticmethod
    def assert_valid_kv_pair(kv_pair):
        """Assert valid key-value pair"""
        assert isinstance(kv_pair, tuple), "KV pair should be a tuple"
        assert len(kv_pair) == 2, "KV pair should have exactly 2 elements"
        key, value = kv_pair
        assert key is not None, "Key should not be None"
        assert isinstance(value, dict), "Value should be a dictionary"
    
    @staticmethod
    def assert_valid_mapping_dict(mapping_dict):
        """Assert valid mapping dictionary structure"""
        assert isinstance(mapping_dict, dict), "Mapping should be a dictionary"
        
        for dest_field, config in mapping_dict.items():
            assert isinstance(dest_field, str), f"Destination field should be string: {dest_field}"
            assert isinstance(config, dict), f"Config should be dict for {dest_field}"
            assert "src_path" in config, f"Missing src_path in config for {dest_field}"
            assert "reconcile" in config, f"Missing reconcile flag for {dest_field}"
            assert "original" in config, f"Missing original flag for {dest_field}"
            assert isinstance(config["src_path"], list), f"src_path should be list for {dest_field}"

# Performance testing utilities
class PerformanceTimer:
    """Timer for performance testing"""
    
    def __init__(self, test_name):
        self.test_name = test_name
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        import time
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        print(f"⏱️  {self.test_name} took {duration:.3f} seconds")
    
    @property
    def duration(self):
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None

# Export test utilities
__all__ = [
    'TEST_CONFIG',
    'get_test_config_path',
    'get_test_schema_path',
    'get_test_mapping_path',
    'generate_mock_bq_record',
    'generate_mock_personas_record',
    'generate_mock_mapping_rows',
    'TestFixtures',
    'TestAssertions',
    'PerformanceTimer'
]

# Print test environment info when imported
def print_test_environment():
    """Print test environment information"""
    print("\n" + "="*60)
    print("DATAFLOW_COMMON TEST ENVIRONMENT")
    print("="*60)
    print(f"Python version: {sys.version.split()[0]}")
    print(f"Project root: {project_root}")
    print(f"Test config: {TEST_CONFIG['test_project']}")
    print("="*60 + "\n")

# Only print in verbose mode
if os.environ.get('TEST_VERBOSE', '').lower() == 'true':
    print_test_environment()