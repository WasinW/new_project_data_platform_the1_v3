#!/usr/bin/env bash
set -euo pipefail
# Simple script to build the custom Dataflow worker image
# Usage: ./scripts/build_container.sh <REGION> <PROJECT_ID> <REPOSITORY> <IMAGE_NAME> <TAG>
# Example: ./scripts/build_container.sh asia-southeast1 my-project dataflow-images beam-aws-pipeline v1
REGION=${1:-asia-southeast1}
PROJECT_ID=${2:?GCP project ID is required}
REPOSITORY=${3:?Artifact Registry repository name is required}
IMAGE_NAME=${4:?Image name is required}
TAG=${5:-latest}

FULL_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${TAG}"

# Build the image
docker build -t "$FULL_IMAGE" .
# Push the image
docker push "$FULL_IMAGE"

echo "Image pushed: $FULL_IMAGE"
