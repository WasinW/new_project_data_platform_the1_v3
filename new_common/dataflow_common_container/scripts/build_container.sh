#!/usr/bin/env bash
# 'sdk_container_image': 'asia-southeast1-docker.pkg.dev/the1-insight-dev/dataflow-images/beam-aws-pipeline:v1.0',
set -euo pipefail
# Simple script to build the custom Dataflow worker image
# Usage: ./scripts/build_container.sh <REGION> <PROJECT_ID> <REPOSITORY> <IMAGE_NAME> <TAG>
# Example: ./scripts/build_container.sh asia-southeast1 my-project dataflow-images beam-aws-pipeline v1
REGION='asia-southeast1'
PROJECT_ID='the1-insight-dev'
REPOSITORY='dataflow-images'
IMAGE_NAME='beam-aws-pipeline'
TAG='v1.0'

FULL_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${TAG}"

# Build the image
docker build -t "$FULL_IMAGE" .
# Push the image
docker push "$FULL_IMAGE"

echo "Image pushed: $FULL_IMAGE"
