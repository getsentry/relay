# Perfetto Proto Definitions

`perfetto_trace.proto` contains a minimal subset of the
[Perfetto trace proto definitions](https://github.com/google/perfetto/tree/master/protos/perfetto/trace)
needed to decode profiling data. Field numbers match the upstream definitions.

The generated Rust code is checked in at `../src/perfetto/proto.rs`.

## Regenerating

Prerequisites:
- `protoc`: https://github.com/protocolbuffers/protobuf/releases (or `brew install protobuf`)
- `protoc-gen-prost`: `cargo install protoc-gen-prost`

Then run:
```sh
./relay-profiling/protos/generate.sh
```
