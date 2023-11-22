#![allow(missing_docs)]

use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("constants.gen.rs");
    let mut f = File::create(dest_path).unwrap();

    macro_rules! write_version {
        ($component:literal) => {{
            let value = env::var(concat!("CARGO_PKG_VERSION_", $component)).unwrap();
            writeln!(f, "#[allow(missing_docs)]").unwrap();
            writeln!(f, "pub const VERSION_{}: u8 = {value};", $component).unwrap();
        }};
    }

    write_version!("MAJOR");
    write_version!("MINOR");
    write_version!("PATCH");

    println!("cargo:rerun-if-changed=build.rs\n");
    println!("cargo:rerun-if-changed=Cargo.toml\n");
}
