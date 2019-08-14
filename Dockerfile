ARG DOCKER_ARCH=amd64

##################
### Deps stage ###
##################

FROM $DOCKER_ARCH/rust:slim-stretch AS semaphore-deps

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
    && apt-get install --no-install-recommends -y curl build-essential git zip \
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
    && OPENSSL_VERS=1.0.2s \
    && CHECKSUM=cabd5c9492825ce5bd23f3c3aeed6a97f8142f606d893df216411f07d1abab96 \
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

#####################
### Builder stage ###
#####################

FROM semaphore-deps AS semaphore-builder

COPY . .

RUN make init-submodules

RUN cargo build --release --locked --target $BUILD_TARGET

RUN bash -c 'objcopy --only-keep-debug target/${BUILD_TARGET}/release/semaphore{,.debug} \
    && objcopy --strip-debug --strip-unneeded target/${BUILD_TARGET}/release/semaphore \
    && objcopy --add-gnu-debuglink target/${BUILD_TARGET}/release/semaphore{.debug,}'

RUN cp ./target/$BUILD_TARGET/release/semaphore /bin/semaphore \
    && zip /opt/semaphore-debug.zip target/$BUILD_TARGET/release/semaphore.debug

###################
### Final stage ###
###################

FROM debian:stretch-slim

RUN apt-get update \
    && apt-get install -y ca-certificates gosu --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV \
    SEMAPHORE_UID=10001 \
    SEMAPHORE_GID=10001

# Create a new user and group with fixed uid/gid
RUN groupadd --system semaphore --gid $SEMAPHORE_GID \
    && useradd --system --gid semaphore --uid $SEMAPHORE_UID semaphore

RUN mkdir /work /etc/semaphore \
    && chown semaphore:semaphore /work /etc/semaphore
VOLUME ["/work", "/etc/semaphore"]
WORKDIR /work

EXPOSE 3000

COPY --from=semaphore-builder /bin/semaphore /bin/semaphore
COPY --from=semaphore-builder /opt/semaphore-debug.zip /opt/semaphore-debug.zip

COPY ./docker-entrypoint.sh /
ENTRYPOINT ["/bin/bash", "/docker-entrypoint.sh"]
CMD ["run"]
