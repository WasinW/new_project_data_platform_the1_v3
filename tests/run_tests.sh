#!/bin/bash
# Run all tests for dataflow_common

echo "🚀 Running dataflow_common Test Suite"
echo "======================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Set Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Create test report directory
mkdir -p test_reports

# Run main test suite
echo -e "\n${YELLOW}Running main test suite...${NC}"
python tests/test_main.py

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ All tests passed!${NC}"
else
    echo -e "${RED}❌ Some tests failed!${NC}"
    exit 1
fi

# Run individual test modules if specified
if [ "$1" == "--verbose" ]; then
    echo -e "\n${YELLOW}Running individual test modules...${NC}"
    
    for module in config orchestrator transforms connectors steps; do
        echo -e "\n${YELLOW}Testing $module module...${NC}"
        python -m pytest tests/test_$module.py -v --tb=short
    done
fi

# Generate coverage report if coverage installed
if command -v coverage &> /dev/null; then
    echo -e "\n${YELLOW}Generating coverage report...${NC}"
    coverage run -m pytest tests/
    coverage report
    coverage html -d test_reports/coverage
    echo -e "${GREEN}Coverage report saved to test_reports/coverage/index.html${NC}"
fi

echo -e "\n${GREEN}Test suite complete!${NC}"