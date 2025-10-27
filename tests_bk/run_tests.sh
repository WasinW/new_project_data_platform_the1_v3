#!/bin/bash
# run_tests.sh

echo "Installing dependencies..."
pip install pyarrow pyyaml apache-beam

echo -e "\n\nRunning Unit Tests..."
python tests/test_short_term_functions.py

echo -e "\n\nRunning Integration Tests..."
python tests/test_pipeline_integration.py

echo -e "\n\nAll tests completed!"