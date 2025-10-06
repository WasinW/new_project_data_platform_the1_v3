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

# Determine the directory of this script and change to it.  This ensures
# the Docker build context is correct regardless of where the script
# is invoked from.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR/.."

# Build the image from the root of the dataflow_common_container package
docker build -t "$FULL_IMAGE" .
# Push the image
docker push "$FULL_IMAGE"

echo "Image pushed: $FULL_IMAGE"
