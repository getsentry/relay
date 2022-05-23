ARG DOCKER_ARCH=amd64

##################
### Deps stage ###
##################

FROM $DOCKER_ARCH/centos:7 AS relay-deps

ARG DOCKER_ARCH
ARG BUILD_ARCH=x86_64

ENV DOCKER_ARCH=${DOCKER_ARCH}
ENV BUILD_ARCH=${BUILD_ARCH}

ENV BUILD_TARGET=${BUILD_ARCH}-unknown-linux-gnu

RUN yum -y update \
    && yum -y install centos-release-scl epel-release \
    # install a modern compiler toolchain
    && yum -y install cmake3 devtoolset-10 git \
    # below required for sentry-native
    llvm-toolset-7.0-clang-devel \
    && yum clean all \
    && rm -rf /var/cache/yum \
    && ln -s /usr/bin/cmake3 /usr/bin/cmake

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal

WORKDIR /work

#####################
### Builder stage ###
#####################

FROM getsentry/sentry-cli:1 AS sentry-cli
FROM relay-deps AS relay-builder

ARG RELAY_FEATURES=ssl,processing,crash-handler
ENV RELAY_FEATURES=${RELAY_FEATURES}

COPY --from=sentry-cli /bin/sentry-cli /bin/sentry-cli
COPY . .

# Build with the modern compiler toolchain enabled
RUN scl enable devtoolset-10 llvm-toolset-7.0 -- \
    make build-linux-release \
    TARGET=${BUILD_TARGET} \
    RELAY_FEATURES=${RELAY_FEATURES}

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
