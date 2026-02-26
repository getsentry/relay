# Perfetto Proto Definitions

`perfetto_trace.proto` contains a minimal subset of the
[Perfetto trace proto definitions](https://github.com/google/perfetto/tree/master/protos/perfetto/trace)
needed to decode profiling data. Field numbers match the upstream definitions.

The generated Rust code is checked in at `../src/perfetto/proto.rs`.

## Regenerating

1. Install protoc: https://github.com/protocolbuffers/protobuf/releases
2. Add to `Cargo.toml` under `[build-dependencies]`:
   ```toml
   prost-build = { workspace = true }
   ```
3. Create a `build.rs` in the `relay-profiling` crate root:
   ```rust
   use std::io::Result;
   use std::path::PathBuf;

   fn main() -> Result<()> {
       let proto_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("protos");
       let proto_file = proto_dir.join("perfetto_trace.proto");
       prost_build::compile_protos(&[&proto_file], &[&proto_dir])?;
       Ok(())
   }
   ```
4. Run: `cargo build -p relay-profiling`
5. Copy the output to the checked-in file:
   ```sh
   cp target/debug/build/relay-profiling-*/out/perfetto.protos.rs relay-profiling/src/perfetto/proto.rs
   ```
6. Remove the `build.rs` and the `prost-build` dependency.
