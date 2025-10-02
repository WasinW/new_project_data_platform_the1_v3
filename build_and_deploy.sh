#!/bin/bash
set -e

# Configuration
PROJECT_ID="the1-insight-dev"
REGION="asia-southeast1"
REPOSITORY="dataflow-images"
IMAGE_NAME="beam-aws-pipeline"
IMAGE_TAG="v$(date +%Y%m%d-%H%M%S)"  # Auto version จาก timestamp
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${IMAGE_TAG}"

echo "======================================"
echo "Building Dataflow Custom Container"
echo "======================================"
echo "Image: ${IMAGE_URI}"
echo ""

# 1. Enable APIs (ถ้ายังไม่เปิด)
echo "→ Enabling Artifact Registry API..."
gcloud services enable artifactregistry.googleapis.com --project=${PROJECT_ID}

# 2. Create repository (ถ้ายังไม่มี)
echo "→ Creating Artifact Registry repository..."
gcloud artifacts repositories create ${REPOSITORY} \
    --repository-format=docker \
    --location=${REGION} \
    --description="Dataflow custom images" \
    --project=${PROJECT_ID} 2>/dev/null || echo "Repository already exists"

# 3. Build image with Cloud Build
echo "→ Building image with Cloud Build..."
gcloud builds submit \
    --tag="${IMAGE_URI}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --timeout=20m

# 4. Grant permissions
echo "→ Granting Artifact Registry permissions..."
gcloud artifacts repositories add-iam-policy-binding ${REPOSITORY} \
    --location=${REGION} \
    --member="serviceAccount:t1-ins-dev-sa-data@the1-insight-dev.iam.gserviceaccount.com" \
    --role="roles/artifactregistry.reader" \
    --project=${PROJECT_ID}

# 5. Test image
echo "→ Testing boto3 availability..."
docker run --rm ${IMAGE_URI} python -c "import boto3; print('✓ boto3 version:', boto3.__version__)"

echo ""
echo "======================================"
echo "✓ Build Complete!"
echo "======================================"
echo "Image URI: ${IMAGE_URI}"
echo ""
echo "Next steps:"
echo "1. Update your Airflow DAG with this image URI"
echo "2. Set sdk_container_image: '${IMAGE_URI}'"
echo "3. Remove py_requirements from BeamRunPythonPipelineOperator"
echo "======================================"

# Create a file with the image URI for easy copy
echo ${IMAGE_URI} > latest_image_uri.txt
echo "Image URI saved to: latest_image_uri.txt"