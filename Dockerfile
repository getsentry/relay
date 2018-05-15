FROM ubuntu:14.04

ENV BUILD_TARGET=x86_64-unknown-linux-gnu
ENV RUST_TOOLCHAIN=stable
ENV OPENSSL_ARCH=linux-x86_64
ENV OPENSSL_DIR=/usr/local/build/$BUILD_TARGET
ENV OPENSSL_STATIC=1

RUN apt-get update \
    && apt-get install -y curl build-essential

# Compile static OpenSSL
COPY scripts/prepare-build.sh /tmp
RUN bash /tmp/prepare-build.sh

# Install Rust
ENV PATH=/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
RUN curl https://sh.rustup.rs -sqSf | \
    sh -s -- -y --default-toolchain $RUST_TOOLCHAIN

WORKDIR /work

# TODO(tonyo): it can certainly be optimized
COPY . ./

RUN cargo build --target=$BUILD_TARGET --release --locked
RUN cp target/$BUILD_TARGET/release/semaphore /usr/local/bin/semaphore

# Copy the binary to a clean image
FROM ubuntu:14.04
RUN apt-get update \
    && apt-get install -y ca-certificates --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
COPY --from=0 /usr/local/bin/semaphore /bin/semaphore
WORKDIR /work
EXPOSE 3000
CMD ["/bin/semaphore"]
