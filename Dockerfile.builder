FROM getsentry/sentry-cli:2 AS sentry-cli
FROM centos:7 AS relay-deps

# Rust version must be provided by the caller.
ARG RUST_TOOLCHAIN_VERSION
ENV RUST_TOOLCHAIN_VERSION=${RUST_TOOLCHAIN_VERSION}

RUN yum -y update \
    && yum -y install centos-release-scl epel-release \
    # install a modern compiler toolchain
    && yum -y install cmake3 devtoolset-10 git \
    perl-core openssl openssl-devel pkgconfig libatomic \
    # below required for sentry-native
    llvm-toolset-7.0-clang-devel \
    && yum clean all \
    && rm -rf /var/cache/yum \
    && ln -s /usr/bin/cmake3 /usr/bin/cmake

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

ARG UID=10000
ARG GID=10000

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain=${RUST_TOOLCHAIN_VERSION} \
    && echo -e '[registries.crates-io]\nprotocol = "sparse"\n[net]\ngit-fetch-with-cli = true' > $CARGO_HOME/config \
    # Adding user and group is the workaround for the old git version,
    # which cannot checkout the repos failing with error:
    # fatal: unable to look up current user in the passwd file: no such user
    && groupadd -f -g ${GID} builder \
    && useradd -o -ms /bin/bash -g ${GID} -u ${UID} builder \
    && chown -R ${UID}:${GID} $CARGO_HOME

COPY --from=sentry-cli /bin/sentry-cli /bin/sentry-cli

WORKDIR /work
