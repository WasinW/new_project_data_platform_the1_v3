export PROJECT_ID="the1-insight-dev"
export REGION="asia-southeast1"
export REPOSITORY="dataflow-images"
export IMAGE_NAME="beam-aws-pipeline"
export IMAGE_TAG="v1.0"

export IMAGE_URI="asia-southeast1-docker.pkg.dev/the1-insight-dev/dataflow-images/beam-aws-pipeline:v1.0"

# Build ด้วย Cloud Build (ต้องมี Dockerfile อยู่ใน directory ปัจจุบัน)
gcloud builds submit \
    --tag="${IMAGE_URI}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --timeout=20m
