#!/bin/bash
set -eux

if [ "${BUILD_ARCH}" = "x86_64" ]; then
  DOCKER_ARCH="amd64"
  OPENSSL_ARCH="linux-x86_64"
elif [ "${BUILD_ARCH}" = "i686" ]; then
  DOCKER_ARCH="i386"
  OPENSSL_ARCH="linux-generic32"
else
  echo "Invalid architecture: ${BUILD_ARCH}"
  exit 1
fi

TARGET=${BUILD_ARCH}-unknown-linux-gnu
BUILD_IMAGE="us.gcr.io/sentryio/relay:deps"

# Prepare build environment first
docker pull $BUILD_IMAGE || true
docker build --build-arg DOCKER_ARCH=${DOCKER_ARCH} \
             --build-arg BUILD_ARCH=${BUILD_ARCH} \
             --build-arg OPENSSL_ARCH=${OPENSSL_ARCH} \
             --cache-from=${BUILD_IMAGE} \
             --target relay-deps \
             -t "${BUILD_IMAGE}" .

DOCKER_RUN_OPTS="
  -v $(pwd):/work
  -v ${HOME}/.cargo/registry:/usr/local/cargo/registry
  -e TARGET=${TARGET}
  $BUILD_IMAGE
"

# And now build the project
docker run $DOCKER_RUN_OPTS \
  make build-linux-release RELAY_FEATURES="${RELAY_FEATURES}"

# Smoke test
docker run $DOCKER_RUN_OPTS \
  make test-process-event CARGO_ARGS="--release --target=${TARGET}"

# Fix permissions for shared directories
USER_ID=$(id -u)
GROUP_ID=$(id -g)
sudo chown -R ${USER_ID}:${GROUP_ID} target/ ${HOME}/.cargo
