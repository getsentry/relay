##################
### Deps stage ###
##################

FROM getsentry/sentry-cli:2 AS sentry-cli
FROM centos:7 AS relay-deps

# Rust version must be provided by the caller.
ARG RUST_TOOLCHAIN_VERSION
ENV RUST_TOOLCHAIN_VERSION=${RUST_TOOLCHAIN_VERSION}

ARG BUILD_ARCH

RUN yum -y update && yum clean all \
    && yum -y install centos-release-scl epel-release \
    # install a modern compiler toolchain
    && yum -y install cmake3 devtoolset-10 git \
    # below required for sentry-native
    llvm-toolset-7.0-clang-devel \
    && yum clean all \
    && rm -rf /var/cache/yum \
    && ln -s /usr/bin/cmake3 /usr/bin/cmake

RUN if [ ${BUILD_ARCH} == "aarch64" ]; then \
    yum -y install git make libffi-devel curl dnf ca-certificates \
    && curl -L -s https://www.centos.org/keys/RPM-GPG-KEY-CentOS-7-aarch64 > /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7-aarch64 \
    && cat /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7-aarch64 >> /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7 \
    # && yum -y install gcc glibc glibc-devel gcc-aarch64-linux-gnu \
    && dnf -y --release 7 install gcc glibc glibc-devel gcc-aarch64-linux-gnu \
    && dnf -y --release 7 --forcearch aarch64 --installroot "/usr/aarch64-linux-gnu/sys-root/" install gcc glibc glibc-devel \
    && ln -s "/usr/aarch64-linux-gnu/sys-root/lib64/libgcc_s.so.1" "/usr/aarch64-linux-gnu/sys-root/lib64/libgcc_s.so" \
    # NOTE(iker): work-around to create a cmake toolchain file for arch-specific
    # builds, since only objcopy is needed.
    && rm -rf "/usr/bin/objcopy" && ln -s "/usr/bin/aarch64-linux-gnu-objcopy" "/usr/bin/objcopy" ; \
    fi

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain=${RUST_TOOLCHAIN_VERSION} \
    && echo -e '[registries.crates-io]\nprotocol = "sparse"\n[net]\ngit-fetch-with-cli = true' > $CARGO_HOME/config

RUN if [ ${BUILD_ARCH} == "aarch64" ]; then \
    rustup target add aarch64-unknown-linux-gnu ; \
    fi

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

COPY --from=relay-builder /work/relay-bin /bin/relay
COPY --from=relay-builder /work/relay-debug.zip /work/relay.src.zip /opt/

COPY ./docker-entrypoint.sh /
ENTRYPOINT ["/bin/bash", "/docker-entrypoint.sh"]
CMD ["run"]
