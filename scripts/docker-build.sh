#!/bin/bash
set -ex

BUILD_DIR="/work"
TARGET="${BUILD_ARCH}-unknown-linux-musl"
CC_NAME="CC_${TARGET//\-/_}"

docker run \
        -e 'CC=' \
        -e "${CC_NAME}=${TARGET}-gcc" \
        -w ${BUILD_DIR} \
        -v `pwd`:${BUILD_DIR} \
        -v $HOME/.cargo/registry:/root/.cargo/registry \
        -it messense/rust-musl-cross:${BUILD_ARCH}-musl \
        cargo build --release --target=${TARGET} --locked

# Fix permissions for shared directories
USER_ID=$(id -u)
GROUP_ID=$(id -g)
sudo chown -R ${USER_ID}:${GROUP_ID} target/ ${HOME}/.cargo
