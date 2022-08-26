#!/bin/bash
set -ex

BUILD_DIR="/work"

docker run \
        --rm \
        -w /work/py \
        -v `pwd`:/work \
        -e SKIP_RELAY_LIB_BUILD \
        -e CARGO_BUILD_TARGET \
        quay.io/pypa/${BUILD_ARCH} \
        sh manylinux.sh

# Fix permissions for shared directories
USER_ID=$(id -u)
GROUP_ID=$(id -g)
sudo chown -R ${USER_ID}:${GROUP_ID} target/
