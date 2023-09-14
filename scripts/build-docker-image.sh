#!/usr/bin/env bash


set -euxo pipefail

ARCH=${1:-$(uname -m)}
TOOLCHAIN=$2
IMAGE_NAME=${3:-"relay-internal"}

# Set the correct build target and update the arch if required.
case "$ARCH" in
  "amd64")
    BUILD_TARGET="x86_64-unknown-linux-gnu"
    ;;
  "arm64" | "aarch64" )
    BUILD_TARGET="aarch64-unknown-linux-gnu"
    ARCH="arm64"
    ;;
  *)
    echo "ERROR unsupported architecture"
    exit 1
esac

# Images to use and build.
IMG_DEPS=${IMG_DEPS:-"ghcr.io/getsentry/relay-deps:$ARCH"}
IMG_VERSIONED=${IMG_VERSIONED:-"$IMAGE_NAME:latest"}

# Relay features to enable.
RELAY_FEATURES="processing,crash-handler"
if [[ "$IMAGE_NAME" == "relay" ]]; then
  RELAY_FEATURES="crash-handler"
fi

# Build a builder image with all the depdendencies.
args=(--progress auto)
if docker pull -q "$IMG_DEPS"; then
  args+=(--cache-from "$IMG_DEPS")
fi

docker buildx build \
    "${args[@]}" \
    --build-arg RUST_TOOLCHAIN_VERSION="$TOOLCHAIN" \
    --build-arg UID="$(id -u)" \
    --build-arg GID="$(id -g)" \
    --cache-to type=inline \
    --platform "linux/$ARCH" \
    --tag "$IMG_DEPS" \
    --target relay-deps \
    --file Dockerfile.builder \
    .

# Build the binary inside of the builder image.
docker run \
    --volume "$PWD:/work:rw" \
    --platform "linux/$ARCH" \
    --user "$(id -u):$(id -g)" \
    -e TARGET="$BUILD_TARGET" \
    "$IMG_DEPS" \
    scl enable devtoolset-10 llvm-toolset-7.0 -- make build-release-with-bundles RELAY_FEATURES="$RELAY_FEATURES"

# Create a release image.
docker buildx build \
    --platform "linux/$ARCH" \
    --tag "$IMG_VERSIONED" \
    --file Dockerfile.release \
    .
