#!/usr/bin/env bash

set -e

IMAGE=${IMAGE:-quay.io/pypa/manylinux2014_x86_64}
TARGET=${TARGET:-aarch64}
TARGET_LINKER=$(echo $TARGET | tr '[:lower:]' '[:upper:]')
DEFAULT_BUILDER_NAME="relay-cabi-builder-${TARGET}:latest"
BUILDER_NAME="${BUILDER_NAME:-${DEFAULT_BUILDER_NAME}}"
USER_ID=$(id -u)
GROUP_ID=$(id -g)


# Build docker image with all dependencies for cross compilation
docker build --build-arg=TARGET=${TARGET} --build-arg=IMAGE=${IMAGE} --file py/Dockerfile -t ${BUILDER_NAME} py/

# Set additional env variables if the image is for another target
# It means we must cross compile
if [[ $IMAGE != *"${TARGET}"* ]]; then
  export "CARGO_TARGET_${TARGET_LINKER}_UNKNOWN_LINUX_GNU_LINKER"="${TARGET}-linux-gnu-gcc"
  export CARGO_BUILD_TARGET="${TARGET}-unknown-linux-gnu"
fi

# run the cross compilation
docker run \
  --rm \
  -w "/work" \
  -v "$(pwd):/work" \
  -e "CARGO_TARGET_${TARGET_LINKER}_UNKNOWN_LINUX_GNU_LINKER" \
  -e CARGO_BUILD_TARGET \
  $BUILDER_NAME \
  bash -c 'cargo build -p relay-cabi --release'

# Fix permissions for shared directories before manylinux run
sudo chown -R ${USER_ID}:${GROUP_ID} target/

# craete a wheel for the correct architecture
docker run \
  --rm \
  -w /work/py \
  -v "$(pwd):/work" \
  -e SKIP_RELAY_LIB_BUILD=1 \
  -e CARGO_BUILD_TARGET \
  quay.io/pypa/manylinux2014_${TARGET} \
  sh manylinux.sh

# Fix permissions for shared directories
sudo chown -R ${USER_ID}:${GROUP_ID} target/
