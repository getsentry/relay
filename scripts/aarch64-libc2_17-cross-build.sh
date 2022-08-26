#!/usr/bin/env bash

## NOTE:
## This script must be run in the centos:7 container which is similar to "manylinux2014_aarch64"
## and has the same toolchain version / glibc / kernel / ABI.

set -e

# install required cross compiler/linker
yum install -y dnf epel-release gcc curl
yum install -y binutils-aarch64-linux-gnu gcc-aarch64-linux-gnu gcc-c++-aarch64-linux-gnu

# install Rust toolchain
curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"
# install our target
rustup target add aarch64-unknown-linux-gnu

# since the Centos is quite old, and were are on the different architecture, we will disable gpg verification for now
sed -i s/gpgcheck=1/gpgcheck=0/g /etc/yum.repos.d/CentOS-*
# install chroot with some of the deps we will need for cross compilation
dnf --forcearch aarch64 --release 7 install gcc glibc glibc-devel --installroot /usr/aarch64-linux-gnu/sys-root/ -y

# FIX: linker cannot find the libgcc_s, since we have libgcc_s.so.1, so we just link it
ln -s /usr/aarch64-linux-gnu/sys-root/usr/lib64/libgcc_s.so.1 /usr/aarch64-linux-gnu/sys-root/usr/lib64/libgcc_s.so

# build the library
env CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER="aarch64-linux-gnu-gcc" cargo build -p relay-cabi --release --target aarch64-unknown-linux-gnu
