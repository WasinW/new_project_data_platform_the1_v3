#!/bin/bash
# Pre-deployment validation for dataflow_common

echo "🔍 Pre-Deployment Validation"
echo "============================="

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Track validation status
VALIDATION_PASSED=true

# Function to check command
check_command() {
    if command -v $1 &> /dev/null; then
        echo -e "${GREEN}✅ $1 is installed${NC}"
        return 0
    else
        echo -e "${RED}❌ $1 is not installed${NC}"
        return 1
    fi
}

# Function to run validation step
validate() {
    local step_name=$1
    local command=$2
    
    echo -e "\n${YELLOW}Validating: $step_name${NC}"
    
    if eval $command; then
        echo -e "${GREEN}✅ $step_name passed${NC}"
    else
        echo -e "${RED}❌ $step_name failed${NC}"
        VALIDATION_PASSED=false
    fi
}

# 1. Check prerequisites
echo -e "\n${YELLOW}1. Checking Prerequisites${NC}"
check_command python3 || VALIDATION_PASSED=false
check_command pip || VALIDATION_PASSED=false
check_command docker || VALIDATION_PASSED=false
check_command gcloud || VALIDATION_PASSED=false

# 2. Check Python version
echo -e "\n${YELLOW}2. Checking Python Version${NC}"
python_version=$(python3 --version | cut -d' ' -f2)
required_version="3.9"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" = "$required_version" ]; then
    echo -e "${GREEN}✅ Python version $python_version meets requirement (>=$required_version)${NC}"
else
    echo -e "${RED}❌ Python version $python_version does not meet requirement (>=$required_version)${NC}"
    VALIDATION_PASSED=false
fi

# 3. Validate directory structure
echo -e "\n${YELLOW}3. Validating Directory Structure${NC}"
required_dirs=(
    "dataflow_common/src/dataflow_common"
    "dataflow_common/src/dataflow_common/steps"
    "dataflow_common/src/dataflow_common/transforms"
    "dataflow_common/src/dataflow_common/connectors"
)

for dir in "${required_dirs[@]}"; do
    if [ -d "$dir" ]; then
        echo -e "${GREEN}✅ Directory exists: $dir${NC}"
    else
        echo -e "${RED}❌ Directory missing: $dir${NC}"
        VALIDATION_PASSED=false
    fi
done

# 4. Validate Python syntax
echo -e "\n${YELLOW}4. Validating Python Syntax${NC}"
validate "Python syntax check" "python3 -m py_compile dataflow_common/src/dataflow_common/*.py"

# 5. Install dependencies
echo -e "\n${YELLOW}5. Installing Dependencies${NC}"
validate "Install requirements" "pip install -r requirements.txt --quiet"

# 6. Run unit tests
echo -e "\n${YELLOW}6. Running Unit Tests${NC}"
validate "Unit tests" "python tests/test_main.py"

# 7. Build wheel package
echo -e "\n${YELLOW}7. Building Wheel Package${NC}"
cd dataflow_common
validate "Build wheel" "python setup.py bdist_wheel"
cd ..

# Check if wheel was created
if [ -f "dataflow_common/dist/dataflow_common-1.0.0-py3-none-any.whl" ]; then
    echo -e "${GREEN}✅ Wheel file created successfully${NC}"
    
    # Validate wheel contents
    echo -e "\n${YELLOW}Validating wheel contents...${NC}"
    pip show -f dataflow_common/dist/dataflow_common-1.0.0-py3-none-any.whl &> /dev/null
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Wheel file is valid${NC}"
    else
        echo -e "${RED}❌ Wheel file validation failed${NC}"
        VALIDATION_PASSED=false
    fi
else
    echo -e "${RED}❌ Wheel file not found${NC}"
    VALIDATION_PASSED=false
fi

# 8. Build Docker image
echo -e "\n${YELLOW}8. Building Docker Image${NC}"

# Create Dockerfile if not exists
if [ ! -f "Dockerfile" ]; then
    cat > Dockerfile << 'EOF'
FROM apache/beam_python3.9_sdk:2.59.0

# Copy dataflow_common
COPY dataflow_common/dist/dataflow_common-1.0.0-py3-none-any.whl /tmp/
RUN pip install --no-cache-dir /tmp/dataflow_common-1.0.0-py3-none-any.whl && \
    rm /tmp/dataflow_common-1.0.0-py3-none-any.whl

# Install additional dependencies
RUN pip install --no-cache-dir \
    google-cloud-bigquery==3.25.0 \
    pyarrow>=14.0.0 \
    pyyaml>=6.0 \
    boto3

# Verify installation
RUN python -c "import dataflow_common; print(f'dataflow_common {dataflow_common.__version__} installed')"
EOF
    echo -e "${YELLOW}Created Dockerfile${NC}"
fi

validate "Build Docker image" "docker build -t dataflow-common:test ."

# 9. Test Docker image
echo -e "\n${YELLOW}9. Testing Docker Image${NC}"
validate "Test Docker container" "docker run --rm dataflow-common:test python -c 'import dataflow_common; print(dataflow_common.__version__)'"

# 10. Validate GCS access
echo -e "\n${YELLOW}10. Validating GCS Access${NC}"
if gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo -e "${GREEN}✅ GCloud authenticated${NC}"
    
    # Test GCS access
    test_bucket="gs://t1-insight-audit-bucket"
    if gsutil ls $test_bucket &> /dev/null; then
        echo -e "${GREEN}✅ Can access GCS bucket: $test_bucket${NC}"
    else
        echo -e "${YELLOW}⚠️  Cannot access GCS bucket: $test_bucket (may need permissions)${NC}"
    fi
else
    echo -e "${RED}❌ Not authenticated with gcloud${NC}"
    VALIDATION_PASSED=false
fi

# 11. Check Terraform files
echo -e "\n${YELLOW}11. Checking Terraform Configuration${NC}"
if [ -d "terraform" ]; then
    cd terraform
    validate "Terraform format check" "terraform fmt -check"
    validate "Terraform validation" "terraform init && terraform validate"
    cd ..
else
    echo -e "${YELLOW}⚠️  Terraform directory not found${NC}"
fi

# 12. Check GitLab CI configuration
echo -e "\n${YELLOW}12. Checking GitLab CI Configuration${NC}"
if [ -f ".gitlab-ci.yml" ]; then
    echo -e "${GREEN}✅ GitLab CI file exists${NC}"
    
    # Basic YAML validation
    python3 -c "import yaml; yaml.safe_load(open('.gitlab-ci.yml'))" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ GitLab CI YAML is valid${NC}"
    else
        echo -e "${RED}❌ GitLab CI YAML is invalid${NC}"
        VALIDATION_PASSED=false
    fi
else
    echo -e "${YELLOW}⚠️  .gitlab-ci.yml not found${NC}"
fi

# Final report
echo -e "\n${YELLOW}=====================================${NC}"
echo -e "${YELLOW}Pre-Deployment Validation Summary${NC}"
echo -e "${YELLOW}=====================================${NC}"

if [ "$VALIDATION_PASSED" = true ]; then
    echo -e "${GREEN}✅ ALL VALIDATIONS PASSED!${NC}"
    echo -e "${GREEN}Ready for deployment via GitLab CI${NC}"
    exit 0
else
    echo -e "${RED}❌ SOME VALIDATIONS FAILED${NC}"
    echo -e "${RED}Please fix the issues before deployment${NC}"
    exit 1
fi