#!/usr/bin/env python
"""
Main test runner for dataflow_common package
"""
import sys
import os
import unittest
import logging
from datetime import datetime
import json

# Import test utilities from package
from tests import (
    TEST_CONFIG,
    TestFixtures,
    PerformanceTimer,
    print_test_environment
)
# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class TestReport:
    """Generate test report"""
    def __init__(self):
        self.results = []
        
    def add_result(self, module, test_name, status, message="", duration=0):
        self.results.append({
            "module": module,
            "test": test_name,
            "status": status,
            "message": message,
            "duration": duration,
            "timestamp": datetime.now().isoformat()
        })
    
    def generate_report(self, output_file="test_report.json"):
        """Generate JSON report"""
        summary = {
            "total": len(self.results),
            "passed": sum(1 for r in self.results if r["status"] == "PASS"),
            "failed": sum(1 for r in self.results if r["status"] == "FAIL"),
            "errors": sum(1 for r in self.results if r["status"] == "ERROR"),
            "timestamp": datetime.now().isoformat(),
            "results": self.results
        }
        
        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        # Print summary
        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)
        print(f"Total Tests: {summary['total']}")
        print(f"Passed: {summary['passed']} ✅")
        print(f"Failed: {summary['failed']} ❌")
        print(f"Errors: {summary['errors']} 🔥")
        print(f"Success Rate: {(summary['passed']/summary['total']*100):.1f}%")
        print("="*60)
        
        return summary

def run_all_tests():
    """Run all test modules"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test modules
    test_modules = [
        'test_config',
        'test_orchestrator', 
        'test_transforms',
        'test_connectors',
        'test_steps'
    ]
    
    for module in test_modules:
        try:
            suite.addTests(loader.loadTestsFromName(module))
            print(f"✅ Loaded tests from {module}")
        except Exception as e:
            print(f"❌ Failed to load {module}: {e}")
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Generate report
    report = TestReport()
    for test, traceback in result.failures:
        report.add_result(
            test.__class__.__module__,
            test._testMethodName,
            "FAIL",
            str(traceback)
        )
    
    for test, traceback in result.errors:
        report.add_result(
            test.__class__.__module__,
            test._testMethodName,
            "ERROR", 
            str(traceback)
        )
    
    for test in result.successes if hasattr(result, 'successes') else []:
        report.add_result(
            test.__class__.__module__,
            test._testMethodName,
            "PASS"
        )
    
    return report.generate_report()

if __name__ == "__main__":
    print("🚀 Starting dataflow_common test suite...")
    summary = run_all_tests()
    
    # Exit with error code if tests failed
    if summary["failed"] > 0 or summary["errors"] > 0:
        sys.exit(1)
    sys.exit(0)