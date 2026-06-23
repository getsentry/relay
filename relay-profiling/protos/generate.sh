#!/usr/bin/env bash
#
# Regenerates the checked-in Rust protobuf bindings for Perfetto trace types
# using protoc with the protoc-gen-prost plugin.
#
# Usage:
#   ./relay-profiling/protos/generate.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROTO_FILE="$SCRIPT_DIR/perfetto_trace.proto"
OUTPUT_FILE="$SCRIPT_DIR/../src/perfetto/proto.rs"

if ! command -v protoc &>/dev/null; then
    echo "error: protoc is not installed." >&2
    echo "       Install it from https://github.com/protocolbuffers/protobuf/releases" >&2
    echo "       or: brew install protobuf" >&2
    exit 1
fi
echo "Using protoc: $(command -v protoc) ($(protoc --version))"

if ! command -v protoc-gen-prost &>/dev/null; then
    echo "error: protoc-gen-prost is not installed." >&2
    echo "       Install it with: cargo install protoc-gen-prost" >&2
    exit 1
fi
echo "Using protoc-gen-prost: $(command -v protoc-gen-prost)"

if [[ ! -f "$PROTO_FILE" ]]; then
    echo "error: proto file not found at $PROTO_FILE" >&2
    exit 1
fi

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

echo "Generating Rust bindings..."
protoc \
    --prost_out="$TMPDIR" \
    --proto_path="$SCRIPT_DIR" \
    "$PROTO_FILE"

# protoc-gen-prost mirrors the proto package path in the output directory.
GENERATED=$(find "$TMPDIR" -name '*.rs' -type f | head -1)

if [[ -z "$GENERATED" || ! -f "$GENERATED" ]]; then
    echo "error: no generated .rs file found in $TMPDIR" >&2
    exit 1
fi

if [[ ! -s "$GENERATED" ]]; then
    echo "error: generated file is empty" >&2
    exit 1
fi

cp "$GENERATED" "$OUTPUT_FILE"
echo "Updated $OUTPUT_FILE"
echo "Done."
