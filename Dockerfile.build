ARG DOCKER_ARCH=amd64
FROM $DOCKER_ARCH/rust:slim-stretch

ARG DOCKER_ARCH
ARG BUILD_ARCH=x86_64
ARG OPENSSL_ARCH=linux-x86_64

ENV DOCKER_ARCH=${DOCKER_ARCH}
ENV BUILD_ARCH=${BUILD_ARCH}
ENV OPENSSL_ARCH=${OPENSSL_ARCH}

ENV BUILD_TARGET=${BUILD_ARCH}-unknown-linux-gnu
ENV RUST_TOOLCHAIN=stable
ENV OPENSSL_DIR=/usr/local/build/$BUILD_TARGET
ENV OPENSSL_STATIC=1

RUN apt-get update \
    && apt-get install --no-install-recommends -y curl build-essential clang-3.9 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work

ENV PREFIX_DIR="$OPENSSL_DIR"
ENV LIBS_DIR="/home/rust/libs"

RUN echo "Building zlib" \
    && ZLIB_VERS=1.2.11 \
    && CHECKSUM=c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1 \
    && mkdir -p $LIBS_DIR \
    && cd $LIBS_DIR \
    && curl -sqLO https://zlib.net/zlib-$ZLIB_VERS.tar.gz \
    && echo "$CHECKSUM zlib-$ZLIB_VERS.tar.gz" > checksums.txt \
    && sha256sum -c checksums.txt \
    && tar xzf zlib-$ZLIB_VERS.tar.gz && cd zlib-$ZLIB_VERS \
    && ./configure --static --archs="-fPIC" --prefix=$PREFIX_DIR \
    && make && make install \
    && cd .. && rm -rf zlib-$ZLIB_VERS.tar.gz zlib-$ZLIB_VERS checksums.txt

RUN echo "Building OpenSSL" \
    && OPENSSL_VERS=1.0.2p \
    && CHECKSUM=50a98e07b1a89eb8f6a99477f262df71c6fa7bef77df4dc83025a2845c827d00 \
    && mkdir -p $LIBS_DIR \
    && cd $LIBS_DIR \
    && curl -sqO https://www.openssl.org/source/openssl-$OPENSSL_VERS.tar.gz \
    && echo "$CHECKSUM openssl-$OPENSSL_VERS.tar.gz" > checksums.txt \
    && sha256sum -c checksums.txt \
    && tar xzf openssl-$OPENSSL_VERS.tar.gz && cd openssl-$OPENSSL_VERS \
    && ./Configure $OPENSSL_ARCH -fPIC --prefix=$PREFIX_DIR \
    && make depend \
    && make && make install \
    && cd .. && rm -rf openssl-$OPENSSL_VERS.tar.gz openssl-$OPENSSL_VERS checksums.txt
