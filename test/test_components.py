"""Unit tests for dataflow_common components"""

import unittest
from unittest.mock import MagicMock, patch
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

class TestMappingFunctions(unittest.TestCase):
    """Test mapping functions"""
    
    def test_normalize_path(self):
        from dataflow_common.transforms.mapping import normalize_path
        
        # Test various path formats
        self.assertEqual(normalize_path("profiles.memberId"), ["profiles", "memberId"])
        self.assertEqual(normalize_path("profiles['memberId']"), ["profiles", "memberId"])
        self.assertEqual(normalize_path("['profiles']['memberId']"), ["profiles", "memberId"])
    
    def test_extract_by_path(self):
        from dataflow_common.transforms.mapping import extract_by_path
        
        record = {
            "profiles": {
                "memberId": "12345",
                "email": "test@example.com"
            }
        }
        
        self.assertEqual(extract_by_path(record, ["profiles", "memberId"]), "12345")
        self.assertEqual(extract_by_path(record, ["profiles", "invalid"]), None)
    
    def test_create_mapping_dict(self):
        from dataflow_common.transforms.mapping import create_mapping_dict
        
        rows = [
            {
                "RECONCILE_COLUMN_NAME": "member_id",
                "PERSONAS_MAPPING_COLUMN_NAME": "profiles.memberId",
                "RECONCILE_RETRIEVED": True,
                "RECONCILE_CONFIRMED": False
            }
        ]
        
        result = create_mapping_dict(
            rows,
            src_field="PERSONAS_MAPPING_COLUMN_NAME",
            dest_field="RECONCILE_COLUMN_NAME",
            retrieved_flag_field="RECONCILE_RETRIEVED",
            confirmed_flag_field="RECONCILE_CONFIRMED"
        )
        
        self.assertIn("member_id", result)
        self.assertEqual(result["member_id"]["src_path"], ["profiles", "memberId"])
        self.assertTrue(result["member_id"]["reconcile"])
        self.assertFalse(result["member_id"]["original"])

class TestBeamTransforms(unittest.TestCase):
    """Test Beam transforms"""
    
    def test_kv_pairs_step(self):
        """Test KVPairs transform"""
        from dataflow_common.steps import KVPairsStep
        from dataflow_common.config import PipelineConfig
        
        with TestPipeline() as p:
            # Create test data
            test_data = [
                {"member_number": "123", "name": "Alice"},
                {"member_number": "456", "name": "Bob"},
                {"member_number": None, "name": "Invalid"},  # Should be filtered
            ]
            
            # Setup step
            config = PipelineConfig(
                name="test",
                mode="batch", 
                term="short",
                params={"pk": "member_number"}
            )
            
            state = {
                "test_input": p | beam.Create(test_data)
            }
            
            spec = {
                "step": "KVPairs",
                "in": "test_input",
                "key_field": "member_number"
            }
            
            step = KVPairsStep(spec=spec, config=config, state=state)
            result = step.execute(p)
            
            # Verify results
            expected = [
                ("123", {"member_number": "123", "name": "Alice"}),
                ("456", {"member_number": "456", "name": "Bob"})
            ]
            
            assert_that(result, equal_to(expected))

class TestSchemaFunctions(unittest.TestCase):
    """Test schema functions"""
    
    def test_build_pyarrow_schema(self):
        from dataflow_common.transforms.schema import build_pyarrow_schema
        import pyarrow as pa
        
        schema_def = [
            {"name": "member_id", "type": "STRING"},
            {"name": "age", "type": "INT64"},
            {"name": "score", "type": "FLOAT64"},
            {"name": "active", "type": "BOOLEAN"},
            {"name": "created_date", "type": "DATE"},
            {"name": "updated_time", "type": "TIMESTAMP"}
        ]
        
        schema = build_pyarrow_schema(schema_def)
        
        self.assertEqual(len(schema), 6)
        self.assertEqual(schema.field("member_id").type, pa.string())
        self.assertEqual(schema.field("age").type, pa.int64())
        self.assertEqual(schema.field("score").type, pa.float64())
        self.assertEqual(schema.field("active").type, pa.bool_())

if __name__ == "__main__":
    unittest.main()