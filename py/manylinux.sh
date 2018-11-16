#!/bin/sh
set -e

# Install dependencies needed by our wheel
echo "Installing packages..."
yum -y -q -e 0 install gcc libffi-devel openssl101e-devel

export OPENSSL_INCLUDE_DIR=/usr/include/openssl101e
export I686_UNKNOWN_LINUX_GNU_OPENSSL_LIB_DIR=/usr/lib/openssl101e
export X86_64_UNKNOWN_LINUX_GNU_OPENSSL_LIB_DIR=/usr/lib64/openssl101e

# Install Rust
curl https://sh.rustup.rs -sSf | sh -s -- -y
export PATH=~/.cargo/bin:$PATH

# Build wheels
which linux32 && LINUX32=linux32
$LINUX32 /opt/python/cp27-cp27mu/bin/python setup.py bdist_wheel

# Audit wheels
for wheel in dist/*-linux_*.whl; do
  auditwheel repair $wheel -w dist/
  rm $wheel
done
