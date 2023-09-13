#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::collections::BTreeSet;
use std::fs::File;
use std::path::PathBuf;

use clap::{command, Parser};
use serde::Serialize;
use syn::ItemEnum;
use syn::ItemStruct;
use walkdir::WalkDir;

use crate::item_collector::AstItemCollector;
use crate::pii_finder::FieldsWithAttribute;

pub mod item_collector;
pub mod pii_finder;

/// Structs and Enums are the only items that are relevant for finding PII fields.
#[derive(Clone)]
pub enum EnumOrStruct {
    Struct(ItemStruct),
    Enum(ItemEnum),
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
    /// Optional output path. By default, documentation is printed on stdout.
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Path to the rust crate/workspace.
    #[arg(short, long)]
    pub path: Option<PathBuf>,

    /// The struct or enum of which you want to find all PII fields. Checks all items if none is
    /// provided.
    #[arg(short, long)]
    pub item: Option<String>,

    /// Vector of which PII-values should be looked for, options are: "true, maybe, false".
    #[arg(long, default_value = "true")]
    pub pii_values: Vec<String>,
}

impl Cli {
    pub fn run(self) -> anyhow::Result<()> {
        // User must either provide the path to a rust crate/workspace or be in one when calling this script.
        let path = match self.path.clone() {
            Some(path) => {
                if !path.join("Cargo.toml").exists() {
                    anyhow::bail!("Please provide the path to a rust crate/workspace");
                }
                path
            }
            None => std::env::current_dir()?,
        };

        // Before we can iterate over the PII fields properly, we make a mapping between all
        // paths to types and their AST node, and of all modules and the items in their scope.
        let types_and_use_statements = {
            let rust_file_paths = find_rs_files(&path);
            AstItemCollector::collect(&rust_file_paths)?
        };

        let pii_types =
            types_and_use_statements.find_pii_fields(self.item.as_deref(), &self.pii_values)?;

        // Function also takes a string to replace unnamed fields, for now we just remove them.
        let output_vec = Output::from_btreeset(pii_types);

        match self.output {
            Some(ref path) => serde_json::to_writer_pretty(File::create(path)?, &output_vec)?,
            None => serde_json::to_writer_pretty(std::io::stdout(), &output_vec)?,
        };

        Ok(())
    }
}

#[derive(Serialize, Default, Debug)]
struct Output {
    path: String,
    additional_properties: bool,
}

impl Output {
    fn new(pii_type: FieldsWithAttribute) -> Self {
        let mut output = Self {
            additional_properties: pii_type.attributes.contains_key("additional_properties"),
            ..Default::default()
        };

        output
            .path
            .push_str(&pii_type.type_and_fields[0].qualified_type_name);

        let mut iter = pii_type.type_and_fields.iter().peekable();
        while let Some(path) = iter.next() {
            // If field has attribute "additional_properties" it means it's not a real field
            // but represents unstrucutred data. So we remove it and pass the information as a boolean
            // in order to properly document this fact in the docs.
            if !(output.additional_properties && iter.peek().is_none()) {
                output.path.push_str(&format!(".{}", path.field_ident));
            }
        }

        output.path = output.path.replace("{{Unnamed}}.", "");
        output
    }

    /// Represent the PII fields in a format that will be used in the final output.
    fn from_btreeset(pii_types: BTreeSet<FieldsWithAttribute>) -> Vec<Self> {
        let mut output_vec = vec![];
        for pii in pii_types {
            output_vec.push(Output::new(pii));
        }
        output_vec.sort_by(|a, b| a.path.cmp(&b.path));

        output_vec
    }
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

    use crate::item_collector::{AstItemCollector, TypesAndScopedPaths};

    use super::*;

    const RUST_TEST_CRATE: &str = "../../tests/test_pii_docs";

    fn get_types_and_use_statements() -> TypesAndScopedPaths {
        let rust_crate = PathBuf::from_slash(RUST_TEST_CRATE);
        let rust_file_paths = find_rs_files(&rust_crate);
        AstItemCollector::collect(&rust_file_paths).unwrap()
    }

    // On windows the assert fails because of how file paths are different there.
    #[cfg(not(target_os = "windows"))]
    #[test]
    fn test_find_rs_files() {
        let rust_crate = PathBuf::from_slash(RUST_TEST_CRATE);
        let mut rust_file_paths = find_rs_files(&rust_crate);
        rust_file_paths.sort_unstable();
        insta::assert_debug_snapshot!(rust_file_paths);
    }

    #[test]
    fn test_single_type() {
        let types_and_use_statements = get_types_and_use_statements();

        let pii_types = types_and_use_statements
            .find_pii_fields(Some("test_pii_docs::SubStruct"), &vec!["true".to_string()])
            .unwrap();

        let output = Output::from_btreeset(pii_types);
        insta::assert_debug_snapshot!(output);
    }

    #[test]
    fn test_scoped_paths() {
        let types_and_use_statements = get_types_and_use_statements();

        let TypesAndScopedPaths { scoped_paths, .. } = types_and_use_statements;
        insta::assert_debug_snapshot!(scoped_paths);
    }

    #[test]
    fn test_pii_true() {
        let types_and_use_statements = get_types_and_use_statements();

        let pii_types = types_and_use_statements
            .find_pii_fields(None, &vec!["true".to_string()])
            .unwrap();

        let output = Output::from_btreeset(pii_types);
        insta::assert_debug_snapshot!(output);
    }

    #[test]
    fn test_pii_false() {
        let types_and_use_statements = get_types_and_use_statements();

        let pii_types = types_and_use_statements
            .find_pii_fields(None, &vec!["false".to_string()])
            .unwrap();

        let output = Output::from_btreeset(pii_types);
        insta::assert_debug_snapshot!(output);
    }

    #[test]
    fn test_pii_all() {
        let types_and_use_statements = get_types_and_use_statements();

        let pii_types = types_and_use_statements
            .find_pii_fields(
                None,
                &vec!["true".to_string(), "false".to_string(), "maybe".to_string()],
            )
            .unwrap();

        let output = Output::from_btreeset(pii_types);
        insta::assert_debug_snapshot!(output);
    }

    #[test]
    fn test_pii_retain_additional_properties_truth_table()
    /*
    Fields should be chosen if there is a pii match, and either retain = "true", or there's no
    "additional_properties" attribute.
    Logic: ((pii match) & (retain = "true" | !additional_properties))

    truth table:

    +-----------+-----------------+----------  ------------+----------+
    | pii match | retain = "true" | !additional_properties | selected |
    +-----------+----------------------+-------------------+----------+
    | True      | True            | True                   | True     |
    | True      | True            | False                  | True     |
    | True      | False           | True                   | False    |
    | True      | False           | False                  | True     |
    | False     | True            | True                   | False    |
    | False     | True            | False                  | False    |
    | False     | False           | True                   | False    |
    | False     | False           | False                  | False    |
    +-----------+-----------------+------------------------+----------+

     */
    {
        let types_and_use_statements = get_types_and_use_statements();

        let pii_types = types_and_use_statements
            .find_pii_fields(None, &vec!["truth_table_test".to_string()])
            .unwrap();

        let output = Output::from_btreeset(pii_types);
        insta::assert_debug_snapshot!(output);
    }
}
