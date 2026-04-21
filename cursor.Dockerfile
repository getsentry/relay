# Dockerfile for the Cursor fork of sentry-relay.
#
# Builds the standard Relay binary plus the `fanout-http` Cargo feature, which
# adds a fire-and-forget HTTP tee that POSTs every envelope to a configurable
# internal endpoint in parallel with the primary upstream forward. See
# `relay-server/src/services/fanout_http.rs` and the `fanout.http.*` config
# block in `relay-config/src/config.rs`.
#
# Build:
#   git submodule update --init --depth 1 \
#     relay-conventions/sentry-conventions \
#     relay-ua/uap-core
#   docker build -f cursor.Dockerfile -t cursor-sentry-relay:dev .
#
# Run (local):
#   docker run --rm -p 3000:3000 \
#     -e RELAY_HOST=0.0.0.0 \
#     -e RELAY_MODE=proxy \
#     -e RELAY_UPSTREAM_URL=https://o4504648565915648.ingest.us.sentry.io \
#     -v "$(pwd)/.relay:/etc/relay" \
#     cursor-sentry-relay:dev

# ---- Builder stage ----
# Pinned for reproducibility; bump in lockstep with rebases on upstream Relay.
FROM rust:1.95-bookworm AS builder

# `native-tls-vendored` (used by reqwest) builds OpenSSL from source, requiring
# a C toolchain and perl. Everything else is pure Rust (Kafka / processing
# features that need librdkafka + CMake are intentionally NOT enabled here).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        perl \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the entire workspace. We rely on `.dockerignore` to keep the context
# small (target/, .git/, etc.). Submodules MUST be initialized on the host
# before `docker build` (see header comment).
COPY . .

# Sanity-check that the required submodules are present. Failing fast here
# produces a much clearer error than a mid-build "file not found" panic.
RUN test -f relay-conventions/sentry-conventions/model/registry/general.json \
        || (echo "ERROR: submodule relay-conventions/sentry-conventions not initialized; run 'git submodule update --init --depth 1 relay-conventions/sentry-conventions relay-ua/uap-core' on the host" >&2 && exit 1) \
    && test -f relay-ua/uap-core/regexes.yaml \
        || (echo "ERROR: submodule relay-ua/uap-core not initialized" >&2 && exit 1)

# Build a release binary with the fanout-http feature. We deliberately do NOT
# enable `processing` (Kafka/Redis/Symbolic) in this image because the Cursor
# deployment runs in proxy mode; that keeps the builder lean and image small.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --bin relay --features relay-server/fanout-http \
    && cp target/release/relay /usr/local/bin/relay

# ---- Filesystem-prep stage ----
# Distroless images contain no shell/mkdir, so we use a debug variant to
# stage the empty config + work directories with the right ownership.
FROM gcr.io/distroless/cc-debian12:debug AS fs

RUN ["/busybox/busybox", "mkdir", "/etc/relay", "/work"]

# ---- Runtime stage ----
# Matches upstream `Dockerfile.release` (distroless cc nonroot on debian12)
# so the operational surface is identical except for the new feature.
FROM gcr.io/distroless/cc-debian12:nonroot

EXPOSE 3000

COPY --from=fs --chown=nonroot:nonroot /etc/relay /etc/relay
COPY --from=fs --chown=nonroot:nonroot /work /work
COPY --from=builder --chown=nonroot:nonroot /usr/local/bin/relay /bin/relay

VOLUME ["/etc/relay", "/work"]
WORKDIR /work

ENTRYPOINT ["/bin/relay"]
CMD ["run"]
