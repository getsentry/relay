mod raw;

use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use walkdir::WalkDir;

const ATTRIBUTE_DIR: &str = "sentry-conventions/model/attributes";

fn main() {
    let crate_dir: PathBuf = env::var("CARGO_MANIFEST_DIR").unwrap().into();
    let mut map = phf_codegen::Map::new();

    for file in WalkDir::new(crate_dir.join(ATTRIBUTE_DIR)) {
        let file = file.unwrap();
        if file.file_type().is_file()
            && let Some(ext) = file.path().extension()
            && ext.to_str() == Some("json")
        {
            let contents = std::fs::read_to_string(file.path()).unwrap();
            let attr: raw::Attribute = serde_json::from_str(&contents).unwrap();
            let (key, value) = raw::format_attribute_info(attr);
            map.entry(key, value);
        }
    }

    let out_path = Path::new(&env::var("OUT_DIR").unwrap()).join("attribute_map.rs");
    let mut out_file = BufWriter::new(File::create(&out_path).unwrap());

    writeln!(
        &mut out_file,
        "static ATTRIBUTES: phf::Map<&'static str, AttributeInfo> = {};",
        map.build()
    )
    .unwrap();

    println!("cargo::rerun-if-changed=.");
}
