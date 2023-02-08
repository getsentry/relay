#!/usr/bin/env bash


set -euxo pipefail

ARCH=${ARCH:-$(uname -m)}

# Set the correct build target and update the arch if required.
if [[ "$ARCH" = "amd64" ]]; then
  BUILD_TARGET="x86_64-unknown-linux-gnu"
elif [[ "$ARCH" = "arm64" ]]; then
  BUILD_TARGET="aarch64-unknown-linux-gnu"
elif [[ "$ARCH" = "aarch64" ]]; then
  BUILD_TARGET="aarch64-unknown-linux-gnu"
  ARCH="arm64"
fi

# Images to use and build.
IMG_DEPS=${IMG_DEPS:-"ghcr.io/getsentry/relay-deps:$ARCH"}
IMG_VERSIONED=${IMG_VERSIONED:-"relay:latest"}

# Build a builder image with all the depdendencies.
args=(--progress auto)
if docker pull -q "$IMG_DEPS"; then
  args+=(--cache-from "$IMG_DEPS")
fi

docker buildx build \
    "${args[@]}" \
    --cache-to type=inline \
    --platform "linux/$ARCH" \
    --tag "$IMG_DEPS" \
    --target relay-deps \
    --file Dockerfile.builder \
    .

# Build the binary inside of the builder image.
docker run \
    -v "$(pwd):/work" \
    --platform "linux/$ARCH" \
    -e TARGET="$BUILD_TARGET" \
    "$IMG_DEPS" \
    scl enable devtoolset-10 llvm-toolset-7.0 -- make build-release-with-bundles RELAY_FEATURES="ssl,processing,crash-handler"

# Fix permissions for shared directories
USER_ID=$(id -u)
GROUP_ID=$(id -g)
sudo chown -R "${USER_ID}:${GROUP_ID}" target/

# Create a release image
docker buildx build \
    --platform "linux/$ARCH" \
    --tag "$IMG_VERSIONED" \
    --file Dockerfile.release \
    .
