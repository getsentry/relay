#!/bin/sh
set -e

# Install dependencies needed by our wheel
echo "Installing packages..."
yum -y -q install gcc libffi-devel

export PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig/

# Build wheels
if [ "$AUDITWHEEL_ARCH" == "i686" ]; then
  LINUX32=linux32
fi

$LINUX32 /opt/python/cp37-cp37m/bin/python setup.py bdist_wheel

# Audit wheels
for wheel in dist/*-linux_*.whl; do
  auditwheel repair $wheel -w dist/
  rm $wheel
done
