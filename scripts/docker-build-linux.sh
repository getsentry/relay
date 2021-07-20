#!/bin/bash
set -eux

if [ "${BUILD_ARCH}" = "x86_64" ]; then
  DOCKER_ARCH="amd64"
elif [ "${BUILD_ARCH}" = "i686" ]; then
  DOCKER_ARCH="i386"
else
  echo "Invalid architecture: ${BUILD_ARCH}"
  exit 1
fi

BUILD_LIBC="${BUILD_LIBC:-musl}"
if [ "${BUILD_LIBC}" = "gnu" ]; then
  DOCKERFILE="Dockerfile.debian"
elif [ "${BUILD_LIBC}" = "musl" ]; then
  DOCKERFILE="Dockerfile"
else
  echo "Invalid libc: ${BUILD_LIBC}"
  exit 1
fi

if [ "${BUILD_ARCH}" = "i686" ] && [ "${BUILD_LIBC}" = "musl" ]; then
  # For some reason there's no Docker image for this
  echo "i686 musl not supported"
  exit 1
fi

TARGET=${BUILD_ARCH}-unknown-linux-${BUILD_LIBC}
BUILD_IMAGE="us.gcr.io/sentryio/relay:deps"

# Prepare build environment first
docker pull $BUILD_IMAGE || true
docker build --build-arg DOCKER_ARCH=${DOCKER_ARCH} \
             --build-arg BUILD_ARCH=${BUILD_ARCH} \
             --cache-from=${BUILD_IMAGE} \
             --target relay-deps \
             -f "${DOCKERFILE}" \
             -t "${BUILD_IMAGE}" .

DOCKER_RUN_OPTS="
  -v $(pwd):/work
  -e TARGET=${TARGET}
  $BUILD_IMAGE
"

# And now build the project
docker run $DOCKER_RUN_OPTS \
  make build-linux-release RELAY_FEATURES="${RELAY_FEATURES}"

# Fix permissions for shared directories
USER_ID=$(id -u)
GROUP_ID=$(id -g)
sudo chown -R ${USER_ID}:${GROUP_ID} target/
