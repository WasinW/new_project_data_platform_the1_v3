#!/usr/bin/env python
"""Test pipeline locally with DirectRunner"""

import sys
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from dataflow_common.config import load_config
from dataflow_common.orchestrator import Orchestrator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_imports():
    """Test all imports work correctly"""
    try:
        # Test critical imports
        from dataflow_common.config import PipelineConfig
        from dataflow_common.connectors.bigtable import BigTableConnector
        from dataflow_common.connectors.pubsub import PubSubConnector
        from dataflow_common.steps.streaming import ReadBigTableRealtimeStep
        print("✅ All imports successful")
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def test_config_loading():
    """Test config file loading"""
    try:
        # Use local config file for testing
        config = load_config("composer/config/ms_member/batch/ms_member_short.yaml")
        print(f"✅ Config loaded: {config.name}")
        return True
    except Exception as e:
        print(f"❌ Config loading error: {e}")
        return False

def test_pipeline_local():
    """Run pipeline with DirectRunner locally"""
    try:
        # Load config
        config = load_config("composer/config/ms_member/batch/ms_member_short.yaml")
        
        # Override for local testing
        config.params.run_dt = "2025100915"
        
        # Create minimal pipeline options for DirectRunner
        pipeline_options = PipelineOptions([
            '--runner=DirectRunner',
            '--direct_num_workers=1',
            '--direct_running_mode=multi_threading',
        ])
        
        # Run with limited data for testing
        orchestrator = Orchestrator(config)
        
        # You might want to modify the plan for local testing
        # e.g., limit queries, use test data
        test_plan = [
            {
                "step": "ReadBQQuery",
                "id": "test_data",
                "query": "SELECT 'test' as member_number, 'data' as field LIMIT 10"
            }
        ]
        config.plan = test_plan  # Override with test plan
        
        orchestrator.run(pipeline_options)
        print("✅ Pipeline executed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Pipeline execution error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Starting local tests...")
    
    # Test 1: Check imports
    if not test_imports():
        sys.exit(1)
    
    # Test 2: Check config loading
    if not test_config_loading():
        sys.exit(1)
    
    # Test 3: Run minimal pipeline
    # Uncomment when ready to test full pipeline
    # test_pipeline_local()
    
    print("\n✅ All tests passed!")