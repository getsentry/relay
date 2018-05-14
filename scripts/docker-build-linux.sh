#!/bin/bash
set -eux

TARGET="${BUILD_ARCH}-unknown-linux-gnu"

if [ "$BUILD_ARCH" = "x86_64" ]; then
    DOCKER_ARCH="amd64"
    OPENSSL_ARCH="linux-x86_64"
else
    DOCKER_ARCH="i386"
    OPENSSL_ARCH="linux-generic32"
fi

OPENSSL_DIR="/usr/local/build/$TARGET"

docker run \
        -w /work \
        -e TARGET=${TARGET} \
        -e BUILD_ARCH=${BUILD_ARCH} \
        -e OPENSSL_ARCH=${OPENSSL_ARCH} \
        -e OPENSSL_DIR=${OPENSSL_DIR} \
        -e OPENSSL_STATIC="1" \
        -v `pwd`:/work \
        -v $HOME/.cargo/registry:/usr/local/cargo/registry \
        -it $DOCKER_ARCH/rust:jessie \
        bash -c "./scripts/prepare-build.sh && cargo build --release --locked --target=${TARGET}"

# Fix permissions for shared directories
USER_ID=$(id -u)
GROUP_ID=$(id -g)
sudo chown -R ${USER_ID}:${GROUP_ID} target/ ${HOME}/.cargo
