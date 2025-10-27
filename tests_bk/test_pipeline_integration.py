# tests/test_pipeline_integration.py
import unittest
from unittest.mock import Mock, patch, MagicMock
import json

class TestPipelineIntegration(unittest.TestCase):
    """Test the complete flow of short term pipeline"""
    
    def test_complete_flow(self):
        """Test complete data flow from personas to parquet"""
        print("\n=== INTEGRATION TEST: Complete Pipeline Flow ===")
        
        # 1. Mock BigQuery data
        mapping_rows = [
            {
                "RECONCILE_COLUMN_NAME": "member_number",
                "PERSONAS_MAPPING_COLUMN_NAME": "profiles.memberId",
                "RECONCILE_RETRIEVED": 1,
                "RECONCILE_CONFIRMED": 1
            }
        ]
        
        personas_rows = [{
            "personaId": "P1",
            "profiles": '{"memberId": "M001", "nationalityId": "THA"}',
            "status": "ACTIVE"
        }]
        
        member_rows = [{
            "MEMBER_NUMBER": "M001",
            "nationalityId": "Thai",
            "updated_date": "2023-01-01"
        }]
        
        print("Step 1: Load mapping rows ✅")
        
        # 2. Build mapping dict
        from dataflow_common.transforms.mapping import create_mapping_dict
        
        mapping_dict = create_mapping_dict(
            mapping_rows,
            src_field="PERSONAS_MAPPING_COLUMN_NAME",
            dest_field="RECONCILE_COLUMN_NAME",
            retrieved_flag_field="RECONCILE_RETRIEVED",
            confirmed_flag_field="RECONCILE_CONFIRMED"
        )
        print(f"Step 2: Build mapping dict ✅ -> {len(mapping_dict)} fields")
        
        # 3. Parse JSON
        personas_parsed = []
        for row in personas_rows:
            rec = dict(row)
            if isinstance(rec["profiles"], str):
                rec["profiles"] = json.loads(rec["profiles"])
            personas_parsed.append(rec)
        print("Step 3: Parse JSON ✅")
        
        # 4. Map records
        from dataflow_common.transforms.mapping import map_record
        
        mapped_records = []
        for rec in personas_parsed:
            mapped = map_record(rec, mapping_dict, mode="reconcile")
            mapped_records.append(mapped)
        print(f"Step 4: Map records ✅ -> {len(mapped_records)} records")
        
        # 5. Create KV pairs
        new_kvs = [(r.get("member_number"), r) for r in mapped_records if r.get("member_number")]
        old_kvs = [(r.get("MEMBER_NUMBER"), r) for r in member_rows if r.get("MEMBER_NUMBER")]
        print(f"Step 5: Create KV pairs ✅ -> new: {len(new_kvs)}, old: {len(old_kvs)}")
        
        # 6. Group by key (simulate)
        grouped = {}
        for k, v in new_kvs:
            if k not in grouped:
                grouped[k] = {"new": [], "old": []}
            grouped[k]["new"].append(v)
        
        for k, v in old_kvs:
            if k not in grouped:
                grouped[k] = {"new": [], "old": []}
            grouped[k]["old"].append(v)
        print(f"Step 6: Group by key ✅ -> {len(grouped)} groups")
        
        # 7. Coalesce
        from dataflow_common.transforms.coalesce import coalesce_by_mapping
        
        final_records = []
        for key, groups in grouped.items():
            result = coalesce_by_mapping(
                (key, groups),
                columns=mapping_rows,
                flag_field="RECONCILE_RETRIEVED",
                pk_field="member_number",
                dest_field="RECONCILE_COLUMN_NAME"
            )
            if result:
                final_records.append(result)
        
        print(f"Step 7: Coalesce ✅ -> {len(final_records)} final records")
        
        # Verify final output
        self.assertEqual(len(final_records), 1)
        self.assertEqual(final_records[0]["member_number"], "M001")
        
        print("\n✅ INTEGRATION TEST PASSED!")
        print(f"Final output: {final_records[0]}")
        
        return final_records


if __name__ == "__main__":
    test = TestPipelineIntegration()
    test.test_complete_flow()