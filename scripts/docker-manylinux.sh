#!/usr/bin/env bash
set -e

if [ -z "$TARGET" ]; then
    echo "TARGET is not set"
    exit 1
fi

TARGET_LINKER="CARGO_TARGET_$(echo $TARGET | tr '[:lower:]' '[:upper:]')_UNKNOWN_LINUX_GNU_LINKER"

# Build docker image with all dependencies for cross compilation
BUILDER_NAME="${BUILDER_NAME:-relay-cabi-builder-${TARGET}}"
docker build --build-arg TARGET=${TARGET} -t ${BUILDER_NAME} py/

# run the cross compilation
docker run \
  --rm \
  -w "/work" \
  -v "$(pwd):/work" \
  -e $TARGET_LINKER \
  -e CARGO_BUILD_TARGET \
  ${BUILDER_NAME} \
  bash -c 'cargo build -p relay-cabi --profile release-cabi'

# create a wheel for the correct architecture
# Pinned to 2025.08.15-1 since manylinux 2025.08.22 onward removes setuptools
docker run \
  --rm \
  -w /work/py \
  -v "$(pwd):/work" \
  -e SKIP_RELAY_LIB_BUILD=1 \
  -e CARGO_BUILD_TARGET \
  quay.io/pypa/manylinux_2_28_${TARGET}:2025.08.15-1 \
  sh manylinux.sh

# Fix permissions for shared directories
USER_ID=$(id -u)
GROUP_ID=$(id -g)
sudo chown -R ${USER_ID}:${GROUP_ID} target/
