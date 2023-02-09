FROM getsentry/sentry-cli:1 AS sentry-cli
FROM centos:7 AS relay-deps

# Pin the Rust version for now

ARG RUST_TOOLCHAIN_VERSION=1.67.0
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
    && echo -e "[net]\ngit-fetch-with-cli = true" > $CARGO_HOME/config \
    && chmod 777 $CARGO_HOME

COPY --from=sentry-cli /bin/sentry-cli /bin/sentry-cli

WORKDIR /work
