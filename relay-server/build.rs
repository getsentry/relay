use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("constants.gen.rs");
    let mut f = File::create(&dest_path).unwrap();

    writeln!(
        f,
        "pub const SERVER: &str = \"sentry-relay/{}\";",
        env::var("CARGO_PKG_VERSION").unwrap()
    )
    .unwrap();
    writeln!(
        f,
        "pub const CLIENT: &str = \"sentry.relay/{}\";",
        env::var("CARGO_PKG_VERSION").unwrap()
    )
    .unwrap();
    println!("cargo:rerun-if-changed=build.rs\n");
    println!("cargo:rerun-if-changed=Cargo.toml\n");
}
