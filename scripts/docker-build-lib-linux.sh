#!/usr/bin/env bash

set -ex

TARGET=${TARGET:-aarch64}
TARGET_LINKER=$(echo $TARGET | tr '[:lower:]' '[:upper:]')
IMAGE_NAME="${IMAGE_NAME:-linux-crossbuild}-${TARGET}:latest"
USER_ID=$(id -u)
GROUP_ID=$(id -g)

MANYLINUX_IMAGE="manylinux2010"

export CARGO_BUILD_TARGET="${TARGET}-unknown-linux-gnu"

# Building AARCH64 separatelly using cross compilation on x86_64
if [ $TARGET == "aarch64" ]; then
  MANYLINUX_IMAGE="manylinux2014"

  # build container
  docker build --build-arg=TARGET="${TARGET}" --file py/Dockerfile.cross -t "$IMAGE_NAME" py/

  # run the cross compilation
  docker run \
    --rm \
    -w "/work" \
    -v "$(pwd):/work" \
    -e "CARGO_TARGET_${TARGET_LINKER}_UNKNOWN_LINUX_GNU_LINKER"="${TARGET}-linux-gnu-gcc" \
    -e CARGO_BUILD_TARGET \
    $IMAGE_NAME \
      bash -c 'source ~/.bashrc && cargo build -p relay-cabi --release'

  # Fix permissions for shared directories before manylinux run
  sudo chown -R ${USER_ID}:${GROUP_ID} target/
  # make sure we do not build the lib twice
  export SKIP_RELAY_LIB_BUILD=1
fi

MANYLINUX_IMAGE_NAME="${MANYLINUX_IMAGE}_${TARGET}"

# we still must build i686 and x86_64 in the manylinux containers, since those are old Centos 6
# and already have all the devtools and old version libc installed
docker run \
  --rm \
  -w /work/py \
  -v "$(pwd):/work" \
  -e SKIP_RELAY_LIB_BUILD \
  -e CARGO_BUILD_TARGET \
  quay.io/pypa/${MANYLINUX_IMAGE_NAME} \
  sh manylinux.sh

# Fix permissions for shared directories
sudo chown -R ${USER_ID}:${GROUP_ID} target/
