ARG DOCKER_ARCH=amd64

##################
### Deps stage ###
##################

FROM $DOCKER_ARCH/rust:slim-buster AS relay-deps

ARG DOCKER_ARCH
ARG BUILD_ARCH=x86_64
ARG OPENSSL_ARCH=linux-x86_64

ENV DOCKER_ARCH=${DOCKER_ARCH}
ENV BUILD_ARCH=${BUILD_ARCH}
ENV OPENSSL_ARCH=${OPENSSL_ARCH}

ENV BUILD_TARGET=${BUILD_ARCH}-unknown-linux-gnu
ENV OPENSSL_DIR=/usr/local/build/$BUILD_TARGET
ENV OPENSSL_STATIC=1

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    curl build-essential git zip cmake \
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
    && make -j$(nproc) && make install \
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
    && make -j$(nproc) && make install \
    && cd .. && rm -rf openssl-$OPENSSL_VERS.tar.gz openssl-$OPENSSL_VERS checksums.txt

#####################
### Builder stage ###
#####################

FROM getsentry/sentry-cli:1 AS sentry-cli
FROM relay-deps AS relay-builder

ARG RELAY_FEATURES=ssl,processing
ENV RELAY_FEATURES=${RELAY_FEATURES}

COPY --from=sentry-cli /bin/sentry-cli /bin/sentry-cli
COPY . .

# BUILD IT!
RUN make build-linux-release TARGET=${BUILD_TARGET} RELAY_FEATURES=${RELAY_FEATURES}

RUN cp ./target/$BUILD_TARGET/release/relay /bin/relay \
    && zip /opt/relay-debug.zip target/$BUILD_TARGET/release/relay.debug

# Collect source bundle
RUN sentry-cli --version \
    && sentry-cli difutil bundle-sources ./target/$BUILD_TARGET/release/relay.debug \
    && mv ./target/$BUILD_TARGET/release/relay.src.zip /opt/relay.src.zip

###################
### Final stage ###
###################

FROM debian:buster-slim

RUN apt-get update \
    && apt-get install -y ca-certificates gosu curl --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV \
    RELAY_UID=10001 \
    RELAY_GID=10001

# Create a new user and group with fixed uid/gid
RUN groupadd --system relay --gid $RELAY_GID \
    && useradd --system --gid relay --uid $RELAY_UID relay

RUN mkdir /work /etc/relay \
    && chown relay:relay /work /etc/relay
VOLUME ["/work", "/etc/relay"]
WORKDIR /work

EXPOSE 3000

COPY --from=relay-builder /bin/relay /bin/relay
COPY --from=relay-builder /opt/relay-debug.zip /opt/relay.src.zip /opt/

COPY ./docker-entrypoint.sh /
ENTRYPOINT ["/bin/bash", "/docker-entrypoint.sh"]
CMD ["run"]
