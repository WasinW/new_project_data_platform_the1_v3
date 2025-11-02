#!/bin/bash
# Run tests for dataflow_common

echo "🚀 Running dataflow_common tests"
echo "================================"

# Set paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH="${SCRIPT_DIR}/scripts/dataflow_common/src:${PYTHONPATH}"

# Run tests
cd "${SCRIPT_DIR}"

# Run individual test modules
echo "Testing transforms..."
python -m pytest tests/test_transforms.py -v

echo "Testing connectors..."
python -m pytest tests/test_connectors.py -v

echo "Testing steps..."
python -m pytest tests/test_steps.py -v

echo "Testing config..."
python -m pytest tests/test_config.py -v

echo "Testing orchestrator..."
python -m pytest tests/test_orchestrator.py -v

# Run all tests
echo "Running all tests..."
python -m pytest tests/ -v --tb=short

echo "✅ Tests completed!"