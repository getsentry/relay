#!/bin/bash
set -ex

BUILD_DIR="/work"

docker run \
        -w /work/py \
        -v `pwd`:/work \
        -v $HOME/.cargo/registry:/root/.cargo/registry \
        -it quay.io/pypa/manylinux1_${BUILD_ARCH} \
        sh manylinux.sh

# Fix permissions for shared directories
USER_ID=$(id -u)
GROUP_ID=$(id -g)
sudo chown -R ${USER_ID}:${GROUP_ID} relay-cabi/target/ ${HOME}/.cargo
