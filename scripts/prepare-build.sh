#!/bin/bash
set -eux

# Build static version of libz and openssl
PREFIX_DIR="$OPENSSL_DIR"

mkdir -p /home/rust/libs

echo "Building zlib"
ZLIB_VERS=1.2.11
cd /home/rust/libs
curl -sqLO http://zlib.net/zlib-$ZLIB_VERS.tar.gz
tar xzf zlib-$ZLIB_VERS.tar.gz && cd zlib-$ZLIB_VERS
./configure --static --archs="-fPIC" --prefix=$PREFIX_DIR
make && make install
cd .. && rm -rf zlib-$ZLIB_VERS.tar.gz zlib-$ZLIB_VERS

echo "Building OpenSSL"
OPENSSL_VERS=1.0.2o
curl -sqO https://www.openssl.org/source/openssl-$OPENSSL_VERS.tar.gz
tar xzf openssl-$OPENSSL_VERS.tar.gz && cd openssl-$OPENSSL_VERS
./Configure $OPENSSL_ARCH -fPIC --prefix=$PREFIX_DIR
make depend
make && make install
cd .. && rm -rf openssl-$OPENSSL_VERS.tar.gz openssl-$OPENSSL_VERS
