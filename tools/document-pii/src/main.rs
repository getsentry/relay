#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::collections::BTreeSet;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use clap::{command, Parser, ValueEnum};
use serde::Serialize;
use syn::ItemEnum;
use syn::ItemStruct;
use walkdir::WalkDir;

use crate::item_collector::AstItemCollector;
use crate::item_collector::TypesAndUseStatements;
use crate::pii_finder::find_pii_fields_of_all_types;
use crate::pii_finder::find_pii_fields_of_type;
use crate::pii_finder::TypeAndField;

pub mod item_collector;
pub mod pii_finder;

/// Structs and Enums are the only items that are relevant for finding pii fields.
#[derive(Clone)]
pub enum EnumOrStruct {
    Struct(ItemStruct),
    Enum(ItemEnum),
}

#[derive(Clone, Copy, Debug, ValueEnum, Default)]
pub enum SchemaFormat {
    #[default]
    Json,
    Yaml,
}

/// Gets all the .rs files in a given rust crate/workspace.
fn find_rs_files(dir: &PathBuf) -> Vec<std::path::PathBuf> {
    let walker = WalkDir::new(dir).into_iter();
    let mut rs_files = Vec::new();

    for entry in walker.filter_map(walkdir::Result::ok) {
        if !entry.path().to_string_lossy().contains("src") {
            continue;
        }
        if entry.file_type().is_file() && entry.path().extension().map_or(false, |ext| ext == "rs")
        {
            rs_files.push(entry.into_path());
        }
    }
    rs_files
}

/// Prints documentation for metrics.
#[derive(Debug, Parser, Default)]
#[command(verbatim_doc_comment)]
pub struct Cli {
    /// The format to output the documentation in.
    #[arg(value_enum, short, long, default_value = "json")]
    pub format: SchemaFormat,

    /// Optional output path. By default, documentation is printed on stdout.
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Path to the rust crate/workspace
    pub path: Option<PathBuf>,

    /// The struct or enum of which you want to find all pii fields. Checks all items if none is
    /// provided.
    #[arg(short, long)]
    pub item: Option<String>,

    /// Vector of which pii-values should be looked for, options are: "true, maybe, false".
    #[arg(long, default_value = "true")]
    pub pii_values: Vec<String>,
}

impl Cli {
    fn write_pii<W: Write>(&self, writer: W, metrics: &[Pii]) -> anyhow::Result<()> {
        match self.format {
            SchemaFormat::Json => serde_json::to_writer_pretty(writer, metrics)?,
            SchemaFormat::Yaml => serde_yaml::to_writer(writer, metrics)?,
        };

        Ok(())
    }

    pub fn run(self) -> anyhow::Result<()> {
        // User must either provide the path to a rust crate/workspace or be in one when calling this script.
        let path = match self.path.clone() {
            Some(path) => path,
            None => std::env::current_dir()?,
        };

        if !path.join("Cargo.toml").exists() {
            panic!("Please provide the path to a rust crate/workspace");
        }

        let rust_file_paths = find_rs_files(&path);

        // Before we can iterate over the pii fields properly, we make a mapping between all
        // paths to types and their AST node.
        let TypesAndUseStatements {
            all_types,
            use_statements,
        } = AstItemCollector::get_types_and_use_statements(&rust_file_paths).unwrap();

        let pii_types = match self.item.as_deref() {
            // If user provides path to an item, find pii_fields under this item in particular.
            Some(path) => {
                if let Some(value) = all_types.get(path).cloned() {
                    find_pii_fields_of_type(
                        path,
                        &value,
                        &all_types,
                        &use_statements,
                        &self.pii_values,
                    )
                } else {
                    panic!("Please provide a fully qualified path to a struct or an enum. E.g. 'relay_general::protocol::Event'");
                }
            }
            // If no item is provided, find pii fields of all types in crate/workspace.
            None => find_pii_fields_of_all_types(&all_types, &use_statements, &self.pii_values),
        }?;

        let output_vec = get_pii_fields_output(pii_types, "".to_string());

        match self.output {
            Some(ref path) => self.write_pii(File::create(path)?, &output_vec)?,
            None => self.write_pii(std::io::stdout(), &output_vec)?,
        }

        Ok(())
    }
}

/// Represents the output of a field which has the correct Pii value.
#[derive(Debug, Serialize, Default)]
struct Pii {
    path: String,
}

/// Represent the pii fields in a format that will be used in the final output.
fn get_pii_fields_output(
    pii_types: BTreeSet<Vec<TypeAndField>>,
    unnamed_replace: String,
) -> Vec<Pii> {
    let mut output_vec = vec![];
    for pii in pii_types {
        let mut output = Pii::default();
        output.path.push_str(&pii[0].qualified_type_name);

        for path in pii {
            output.path.push_str(&format!(".{}", path.field_ident));
        }

        output.path = output.path.replace("{{Unnamed}}.", &unnamed_replace);
        output_vec.push(output);
    }
    output_vec.sort_by_key(|pii| pii.path.clone());
    output_vec
}

fn print_error(error: &anyhow::Error) {
    eprintln!("Error: {error}");

    let mut cause = error.source();
    while let Some(ref e) = cause {
        eprintln!("  caused by: {e}");
        cause = e.source();
    }
}

fn main() {
    let cli = Cli::parse();

    match cli.run() {
        Ok(()) => (),
        Err(error) => {
            print_error(&error);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use path_slash::PathBufExt;

    use crate::item_collector::AstItemCollector;

    use super::*;

    const RUST_TEST_CRATE: &str = "../../tests/test_pii_docs";

    #[test]
    fn test_use_statements() {
        let rust_crate = PathBuf::from_slash(RUST_TEST_CRATE);

        let rust_file_paths = find_rs_files(&rust_crate);

        let TypesAndUseStatements { use_statements, .. } =
            AstItemCollector::get_types_and_use_statements(&rust_file_paths).unwrap();
        insta::assert_debug_snapshot!(use_statements);
    }

    #[test]
    fn test_pii_true() {
        let rust_crate = PathBuf::from_slash(RUST_TEST_CRATE);

        let rust_file_paths = find_rs_files(&rust_crate);

        let TypesAndUseStatements {
            all_types,
            use_statements,
        } = AstItemCollector::get_types_and_use_statements(&rust_file_paths).unwrap();

        let pii_types =
            find_pii_fields_of_all_types(&all_types, &use_statements, &vec!["true".to_string()])
                .unwrap();

        let output = get_pii_fields_output(pii_types, "".into());
        insta::assert_debug_snapshot!(output);
    }

    #[test]
    fn test_pii_false() {
        let rust_crate = PathBuf::from_slash(RUST_TEST_CRATE);

        let rust_file_paths = find_rs_files(&rust_crate);

        let TypesAndUseStatements {
            all_types,
            use_statements,
        } = AstItemCollector::get_types_and_use_statements(&rust_file_paths).unwrap();

        let pii_types =
            find_pii_fields_of_all_types(&all_types, &use_statements, &vec!["false".to_string()])
                .unwrap();

        let output = get_pii_fields_output(pii_types, "".into());
        insta::assert_debug_snapshot!(output);
    }

    #[test]
    fn test_pii_all() {
        let rust_crate = PathBuf::from_slash(RUST_TEST_CRATE);

        let rust_file_paths = find_rs_files(&rust_crate);

        let TypesAndUseStatements {
            all_types,
            use_statements,
        } = AstItemCollector::get_types_and_use_statements(&rust_file_paths).unwrap();

        let pii_types = find_pii_fields_of_all_types(
            &all_types,
            &use_statements,
            &vec!["true".to_string(), "false".to_string(), "maybe".to_string()],
        )
        .unwrap();

        let output = get_pii_fields_output(pii_types, "".into());
        insta::assert_debug_snapshot!(output);
    }
}
