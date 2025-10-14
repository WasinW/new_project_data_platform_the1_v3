#!/usr/bin/env python
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
    print("\nTesting registry...")
    
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
    print("\nTesting dynamic import...")
    
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
    print("Testing refactored packages...\n")
    
    results = []
    results.append(test_basic_import())
    results.append(test_registry())
    results.append(test_dynamic_import())
    
    if all(results):
        print("\n=== All tests passed! ===")
    else:
        print("\n=== Some tests failed ===")
        sys.exit(1)
