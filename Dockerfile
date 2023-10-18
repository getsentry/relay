##################
### Deps stage ###
##################

FROM getsentry/sentry-cli:2 AS sentry-cli
FROM centos:7 AS relay-deps

# Rust version must be provided by the caller.
ARG RUST_TOOLCHAIN_VERSION
ENV RUST_TOOLCHAIN_VERSION=${RUST_TOOLCHAIN_VERSION}

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

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
  | sh -s -- -y --profile minimal --default-toolchain=${RUST_TOOLCHAIN_VERSION} \
  && echo -e '[registries.crates-io]\nprotocol = "sparse"\n[net]\ngit-fetch-with-cli = true' > $CARGO_HOME/config

COPY --from=sentry-cli /bin/sentry-cli /bin/sentry-cli

WORKDIR /work

#####################
### Builder stage ###
#####################

FROM relay-deps AS relay-builder

ARG RELAY_FEATURES=processing,crash-handler
ENV RELAY_FEATURES=${RELAY_FEATURES}

COPY . .

# Build with the modern compiler toolchain enabled
RUN : \
  && export BUILD_TARGET="$(arch)-unknown-linux-gnu" \
  && scl enable devtoolset-10 llvm-toolset-7.0 -- \
  make build-linux-release \
  TARGET=${BUILD_TARGET} \
  RELAY_FEATURES=${RELAY_FEATURES}

# Collect source bundle
# Produces `relay-bin`, `relay-debug.zip` and `relay.src.zip` in current directory
RUN : \
  && export BUILD_TARGET="$(arch)-unknown-linux-gnu" \
  && make collect-source-bundle \
  TARGET=${BUILD_TARGET}

###################
### Final stage ###
###################

FROM debian:bookworm-slim

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

COPY --from=relay-builder /work/relay-bin /bin/relay
COPY --from=relay-builder /work/relay-debug.zip /work/relay.src.zip /opt/

COPY ./docker-entrypoint.sh /
ENTRYPOINT ["/bin/bash", "/docker-entrypoint.sh"]
CMD ["run"]
