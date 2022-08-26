#!/usr/bin/env bash

set -ex

docker run \
        --rm \
        -w "/work" \
        -v "$(pwd):/work" \
        centos:7 \
        ./scripts/aarch64-libc2_17-cross-build.sh

# Fix permissions for shared directories
USER_ID=$(id -u)
GROUP_ID=$(id -g)
sudo chown -R ${USER_ID}:${GROUP_ID} target/
