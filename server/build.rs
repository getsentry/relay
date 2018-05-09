use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("constants.gen.rs");
    let mut f = File::create(&dest_path).unwrap();

    write!(
        f,
        "pub const SERVER: &'static str = \"semaphore/{}\";\n",
        env::var("CARGO_PKG_VERSION").unwrap()
    ).unwrap();
    println!("cargo:rerun-if-changed=build.rs\n");
    println!("cargo:rerun-if-changed=Cargo.toml\n");
}
