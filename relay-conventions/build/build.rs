mod attributes;
mod description;
mod measurements;
mod name;

use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use walkdir::WalkDir;

const ATTRIBUTE_DIR: &str = "sentry-conventions/model/attributes";
const DESCRIPTION_DIR: &str = "sentry-conventions/model/description";
const MEASUREMENT_DIR: &str = "sentry-conventions/model/measurements";
const NAME_DIR: &str = "sentry-conventions/model/name";

fn main() {
    let crate_dir: PathBuf = env::var("CARGO_MANIFEST_DIR").unwrap().into();

    write_attribute_rs(&crate_dir);
    write_measurement_rs(&crate_dir);
    write_name_rs(&crate_dir);
    write_description_rs(&crate_dir);

    // Ideally this would only run when compiling for tests, but #[cfg(test)] doesn't seem to work
    // here.
    write_test_name_rs();

    println!("cargo::rerun-if-changed=.");
}

fn write_attribute_rs(crate_dir: &Path) {
    use attributes::*;

    let attribute_consts_path =
        Path::new(&env::var("OUT_DIR").unwrap()).join("attribute_consts.rs");
    let mut attribute_consts_file = BufWriter::new(File::create(&attribute_consts_path).unwrap());

    let interpolation_fns_path =
        Path::new(&env::var("OUT_DIR").unwrap()).join("interpolation_fns.rs");
    let mut interpolation_fns_file = BufWriter::new(File::create(&interpolation_fns_path).unwrap());

    let mut attribute_replacement_map = BTreeMap::new();

    let mut root = RawNode::default();

    for file in WalkDir::new(crate_dir.join(ATTRIBUTE_DIR)) {
        let file = file.unwrap();
        if file.file_type().is_file()
            && let Some(ext) = file.path().extension()
            && ext.to_str() == Some("json")
        {
            let contents = std::fs::read_to_string(file.path()).unwrap();
            let attr: Attribute = serde_json::from_str(&contents).unwrap();

            check_attribute(&attr);

            // Write attribute constant
            writeln!(&mut attribute_consts_file, "{}\n", format_constant(&attr)).unwrap();

            // Insert deprecated -> replacement into map
            if let Some((old, new)) = constant_pair(&attr) {
                attribute_replacement_map.insert(old, new);
            }

            // Write interpolating function, if applicable
            if let Some(fun) = format_interpolating_fn(&attr) {
                writeln!(&mut interpolation_fns_file, "{}\n", fun).unwrap();
            }

            // Put attribute info in the hierarchical map
            let info = format_attribute_info(&attr);

            let mut node = &mut root;
            let mut parts = parse_segments(&attr.key).peekable();
            while let Some(part) = parts.next() {
                node = node
                    .children
                    .entry(part.to_owned())
                    .or_insert_with(RawNode::default);
                if parts.peek().is_none() {
                    node.info = Some(info);
                    break;
                }
            }
        }
    }

    let canonical_fn_path = Path::new(&env::var("OUT_DIR").unwrap()).join("canonical_fn.rs");
    let mut canonical_fn_file = BufWriter::new(File::create(&canonical_fn_path).unwrap());

    write_canonical_fn(
        &mut canonical_fn_file,
        attribute_replacement_map.into_iter(),
    );

    let attribute_map_path = Path::new(&env::var("OUT_DIR").unwrap()).join("attribute_map.rs");
    let mut attribute_map_file = BufWriter::new(File::create(&attribute_map_path).unwrap());

    write!(
        &mut attribute_map_file,
        "static ATTRIBUTES: Node<AttributeInfo> = ",
    )
    .unwrap();
    root.write(&mut attribute_map_file).unwrap();
    write!(&mut attribute_map_file, ";").unwrap();
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

fn write_description_rs(crate_dir: &Path) {
    let descriptions = WalkDir::new(crate_dir.join(DESCRIPTION_DIR))
        .into_iter()
        .flat_map(|file| {
            let file = file.unwrap();
            if file.file_type().is_file()
                && let Some(ext) = file.path().extension()
                && ext.to_str() == Some("json")
            {
                let contents = std::fs::read_to_string(file.path()).unwrap();
                Some(serde_json::from_str::<description::Description>(&contents).unwrap())
            } else {
                None
            }
        });

    let out_path = Path::new(&env::var("OUT_DIR").unwrap()).join("description_fn.rs");
    let mut out_file = BufWriter::new(File::create(&out_path).unwrap());
    let output = description::description_file_output(descriptions);

    writeln!(&mut out_file, "{}", output).unwrap();
}

fn write_measurement_rs(crate_dir: &Path) {
    use measurements::{
        Measurement, format_constant, measurement_attribute_pair, write_replacement_fn,
    };

    let measurement_consts_path =
        Path::new(&env::var("OUT_DIR").unwrap()).join("measurement_consts.rs");
    let mut measurement_consts_file =
        BufWriter::new(File::create(&measurement_consts_path).unwrap());

    let mut measurement_replacement_map = BTreeMap::new();

    for file in WalkDir::new(crate_dir.join(MEASUREMENT_DIR)) {
        let file = file.unwrap();
        if file.file_type().is_file()
            && let Some(ext) = file.path().extension()
            && ext.to_str() == Some("json")
        {
            let contents = std::fs::read_to_string(file.path()).unwrap();
            let attr: Measurement = serde_json::from_str(&contents).unwrap();

            // Write measurment constant
            writeln!(&mut measurement_consts_file, "{}\n", format_constant(&attr)).unwrap();

            // Insert measurement -> attribute into map
            if let Some((old, new)) = measurement_attribute_pair(&attr) {
                measurement_replacement_map.insert(old, new);
            }
        }
    }

    let replacement_fn_path =
        Path::new(&env::var("OUT_DIR").unwrap()).join("measurement_replacement_fn.rs");
    let mut replacement_fn_file = BufWriter::new(File::create(&replacement_fn_path).unwrap());

    write_replacement_fn(
        &mut replacement_fn_file,
        measurement_replacement_map.into_iter(),
    );
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
