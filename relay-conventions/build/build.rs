mod attributes;
mod name;

use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use walkdir::WalkDir;

const ATTRIBUTE_DIR: &str = "sentry-conventions/model/attributes";
const NAME_DIR: &str = "sentry-conventions/model/name";

fn main() {
    let crate_dir: PathBuf = env::var("CARGO_MANIFEST_DIR").unwrap().into();

    write_attribute_rs(&crate_dir);
    write_name_rs(&crate_dir);

    // Ideally this would only run when compiling for tests, but #[cfg(test)] doesn't seem to work
    // here.
    write_test_name_rs();

    println!("cargo::rerun-if-changed=.");
}

fn write_attribute_rs(crate_dir: &Path) {
    use attributes::{Attribute, RawNode, format_attribute_info, parse_segments};

    let mut root = RawNode::default();

    for file in WalkDir::new(crate_dir.join(ATTRIBUTE_DIR)) {
        let file = file.unwrap();
        if file.file_type().is_file()
            && let Some(ext) = file.path().extension()
            && ext.to_str() == Some("json")
        {
            let contents = std::fs::read_to_string(file.path()).unwrap();
            let attr: Attribute = serde_json::from_str(&contents).unwrap();
            let (key, value) = format_attribute_info(attr);

            let mut node = &mut root;
            let mut parts = parse_segments(&key).peekable();
            while let Some(part) = parts.next() {
                node = node
                    .children
                    .entry(part.to_owned())
                    .or_insert_with(RawNode::default);
                if parts.peek().is_none() {
                    node.info = Some(value);
                    break;
                }
            }
        }
    }

    let out_path = Path::new(&env::var("OUT_DIR").unwrap()).join("attribute_map.rs");
    let mut out_file = BufWriter::new(File::create(&out_path).unwrap());

    write!(&mut out_file, "static ATTRIBUTES: Node<AttributeInfo> = ",).unwrap();
    root.build(&mut out_file).unwrap();
    write!(&mut out_file, ";").unwrap();
}

fn write_name_rs(crate_dir: &Path) {
    let names = WalkDir::new(crate_dir.join(NAME_DIR))
        .into_iter()
        .flat_map(|file| {
            let file = file.unwrap();
            if file.file_type().is_file()
                && let Some(ext) = file.path().extension()
                && ext.to_str() == Some("json")
            {
                let contents = std::fs::read_to_string(file.path()).unwrap();
                Some(serde_json::from_str::<name::Name>(&contents).unwrap())
            } else {
                None
            }
        });

    let out_path = Path::new(&env::var("OUT_DIR").unwrap()).join("name_fn.rs");
    let mut out_file = BufWriter::new(File::create(&out_path).unwrap());
    let output = name::name_file_output(names);

    writeln!(&mut out_file, "{}", output).unwrap();
}

/// Writes a canned version of the `name_for_op_and_attributes` function exclusively for tests.
fn write_test_name_rs() {
    use crate::name::{Name, Operation};

    let names = vec![
        Name {
            operations: vec![
                Operation {
                    ops: vec!["op_with_literal_name".to_owned()],
                    templates: vec!["literal name".to_owned()],
                },
                Operation {
                    ops: vec![
                        "op_with_attributes_1".to_owned(),
                        "op_with_attributes_2".to_owned(),
                    ],
                    templates: vec![
                        "{{attr1}}".to_owned(),
                        "{{attr2}} {{attr3}}".to_owned(),
                        "prefix {{attr3}} suffix".to_owned(),
                        "fallback literal".to_owned(),
                    ],
                },
            ],
        },
        Name {
            operations: vec![Operation {
                ops: vec!["op_in_second_name_file".to_owned()],
                templates: vec!["second file literal name".to_owned()],
            }],
        },
    ];

    let out_path = Path::new(&env::var("OUT_DIR").unwrap()).join("test_name_fn.rs");
    let mut out_file = BufWriter::new(File::create(&out_path).unwrap());
    let output = name::name_file_output(names.into_iter());

    writeln!(&mut out_file, "{}", output).unwrap();
}
