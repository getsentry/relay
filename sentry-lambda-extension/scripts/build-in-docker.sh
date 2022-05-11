#!/bin/bash
set -euo pipefail

BUILD_IMAGE="sentry-lambda-extension-builder:latest"

echo "Prepare Docker image for building..."
docker build \
    -f Dockerfile \
    -t "${BUILD_IMAGE}" .

echo "Building the binary..."
DOCKER_RUN_OPTS="
  -v $(pwd):/work
  $BUILD_IMAGE
"

#docker run $DOCKER_RUN_OPTS \
#  cargo build --release --locked --target=x86_64-unknown-linux-gnu

docker run $DOCKER_RUN_OPTS \
  echo "TODO: add real build command here! ;-)"
