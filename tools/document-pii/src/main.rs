#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::hash::Hash;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use clap::{command, Parser, ValueEnum};
use serde::Serialize;
use syn::punctuated::Punctuated;
use syn::{visit::Visit, ItemEnum};
use syn::{Attribute, Field, ItemStruct, Meta, Type, UseTree};
use walkdir::WalkDir;

/// Iterates over the rust files to collect all the types and use_statements, which is later
/// used for recursively looking for pii_fields afterwards.
struct FileSyntaxVisitor {
    module_path: String,
    /// Maps from the full path of a type to its AST node.
    all_types: HashMap<String, EnumOrStruct>,
    /// Maps from a module_path to all the use_statements and path of local items.
    use_statements: HashMap<String, BTreeSet<String>>,
}

impl FileSyntaxVisitor {
    fn insert_use_statements(&mut self, use_statements: Vec<String>) {
        self.use_statements
            .entry(self.module_path.clone())
            .or_default()
            .extend(use_statements);
    }

    /// Gets both a mapping of the full type to a type and its actual AST node, and also the
    /// use_statements in its module, which is needed to fetch the types that it referes to in its
    /// fields.
    #[allow(clippy::type_complexity)]
    fn get_types_and_use_statements(
        paths: &Vec<PathBuf>,
    ) -> anyhow::Result<(
        HashMap<String, EnumOrStruct>,
        HashMap<String, BTreeSet<String>>,
    )> {
        let mut node_map = HashMap::new();
        let mut use_statements = HashMap::new();
        for path in paths {
            let mut visitor = Self {
                module_path: rust_file_to_use_path(path.as_path()),
                use_statements: HashMap::new(),
                all_types: HashMap::new(),
            };
            let file_content = fs::read_to_string(path.as_path())?;
            let syntax_tree: syn::File = syn::parse_file(&file_content)?;
            visitor.visit_file(&syntax_tree);
            node_map.extend(visitor.all_types);
            use_statements.extend(visitor.use_statements);
        }
        Ok((node_map, use_statements))
    }
}

/// Iterates through all the rust files in order to make the `full-type-path -> AST node' map
/// and the 'module-path -> use-statements' map. These are needed for the 'find_pii_fields' function.
impl<'ast> Visit<'ast> for FileSyntaxVisitor {
    fn visit_item_struct(&mut self, node: &'ast ItemStruct) {
        let struct_name = format!("{}::{}", self.module_path, node.ident);
        self.insert_use_statements(vec![struct_name.clone()]);
        self.all_types
            .insert(struct_name, EnumOrStruct::Struct(node.clone()));
    }

    fn visit_item_enum(&mut self, node: &'ast ItemEnum) {
        let enum_name = format!("{}::{}", self.module_path, node.ident);
        self.insert_use_statements(vec![enum_name.clone()]);
        self.all_types
            .insert(enum_name, EnumOrStruct::Enum(node.clone()));
    }

    fn visit_item_use(&mut self, i: &'ast syn::ItemUse) {
        let use_statements = usetree_to_paths(&i.tree, &self.module_path)
            .iter()
            .filter(|s| s.contains("relay"))
            .cloned()
            .collect();

        self.insert_use_statements(use_statements);
    }
}

/// The name of a field along with its type. Used for the path to a pii-field.
#[derive(Ord, PartialOrd, PartialEq, Eq, Hash, Clone, Debug, Default)]
struct TypeAndField {
    // Full path of a type. E.g. relay_common::protocol::Event, rather than just 'Event'.
    qualified_type_name: String,
    field_ident: String,
}

/// Gets all the .rs files in a given rust crate/workspace.
fn find_rs_files(dir: &PathBuf) -> Vec<std::path::PathBuf> {
    let walker = WalkDir::new(dir).into_iter();
    let mut rs_files = Vec::new();

    for entry in walker.filter_map(walkdir::Result::ok) {
        if !entry.path().to_string_lossy().contains("/src/") {
            continue;
        }
        if entry.file_type().is_file() && entry.path().extension().map_or(false, |ext| ext == "rs")
        {
            rs_files.push(entry.into_path());
        }
    }
    rs_files
}

/// Structs and Enums are the only items we iterate over here.
#[derive(Debug, Clone)]
enum EnumOrStruct {
    Struct(ItemStruct),
    Enum(ItemEnum),
}

/// This is the visitor that actually generates the pii_types, it has a lot of associated data
/// because it using the Visit trait from syn-crate means I cannot add data as arguments.
/// The 'pii_types' field can be regarded as the output.
struct TypeVisitor<'a> {
    module_path: String,
    current_type: String,
    node_map: &'a HashMap<String, EnumOrStruct>,
    use_statements: &'a HashMap<String, BTreeSet<String>>,
    pii_values: &'a Vec<String>,
    current_path: &'a mut Vec<TypeAndField>,
    pii_types: &'a mut BTreeSet<Vec<TypeAndField>>,
}

impl<'a> TypeVisitor<'a> {
    /// Takes a Field and visit the types that it consist of if we have
    /// the full path to it in self.use_statements.
    fn visit_field_types(&mut self, node: &Field) {
        let local_paths = self.use_statements.get(&self.module_path).unwrap().clone();

        let mut field_types = vec![];
        get_field_types(&node.ty, &mut field_types);

        for field_type in &field_types {
            for use_path in &local_paths {
                if use_path.split("::").last().unwrap() == field_type.trim() {
                    let enum_or_struct = { self.node_map.get(use_path).cloned() };

                    if let Some(enum_or_struct) = enum_or_struct {
                        match enum_or_struct {
                            EnumOrStruct::Enum(itemenum) => {
                                let current_type = self.current_type.clone();
                                let module_path = self.module_path.clone();
                                self.module_path = use_path.rsplit_once("::").unwrap().0.to_owned();
                                self.visit_item_enum(&itemenum.clone());
                                self.module_path = module_path;
                                self.current_type = current_type;
                            }
                            EnumOrStruct::Struct(itemstruct) => {
                                let current_type = self.current_type.clone();
                                let module_path = self.module_path.clone();
                                self.module_path = use_path.rsplit_once("::").unwrap().0.to_owned();
                                self.visit_item_struct(&itemstruct.clone());
                                self.module_path = module_path;
                                self.current_type = current_type;
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<'ast> Visit<'ast> for TypeVisitor<'_> {
    fn visit_item_struct(&mut self, node: &'ast ItemStruct) {
        self.current_type = node.ident.to_string();

        if !self // This should stop any infinite recursion
            .current_path
            .iter()
            .any(|x| x.qualified_type_name == self.current_type)
        {
            for field in node.fields.iter() {
                self.visit_field(field);
            }
        }
    }

    fn visit_item_enum(&mut self, node: &'ast ItemEnum) {
        self.current_type = node.ident.to_string();
        if !self
            .current_path
            .iter()
            .any(|x| x.qualified_type_name == self.current_type)
        {
            for variant in node.variants.iter() {
                for field in variant.fields.iter() {
                    self.visit_field(field);
                }
            }
        }
    }

    fn visit_field(&mut self, node: &'ast Field) {
        // Every time we visit a field, we have to append the field to the current_path, it gets
        // popped in the end of this function. This is done so that we can store the full path
        // whenever the field matches a correct PII value.
        self.current_path.push(TypeAndField {
            qualified_type_name: self.current_type.clone(),
            field_ident: node
                .clone()
                .ident
                .map(|x| x.to_string())
                .unwrap_or_else(|| "{{Unnamed}}".to_string()),
        });

        if has_pii_value(self.pii_values, node) {
            self.pii_types.insert(self.current_path.clone());
        }

        self.visit_field_types(node);

        self.current_path.pop();
    }
}

/// if you have a field such as `foo: Foo<Bar<Baz>>` this function can take the type of the field
/// and return a vector of the types like: ["Foo", "Bar", "Baz"].
fn get_field_types(ty: &Type, segments: &mut Vec<String>) {
    match ty {
        Type::Path(type_path) => {
            let mut path_iter = type_path.path.segments.iter();
            let first_segment = path_iter.next();

            if let Some(first_segment) = first_segment {
                let mut ident = first_segment.ident.to_string();

                let args = &first_segment.arguments;
                if let syn::PathArguments::AngleBracketed(angle_bracketed) = args {
                    for generic_arg in angle_bracketed.args.iter() {
                        match generic_arg {
                            syn::GenericArgument::Type(ty) => {
                                get_field_types(ty, segments);
                            }
                            _ => continue,
                        }
                    }
                }

                if let Some(second_segment) = path_iter.next() {
                    ident.push_str("::");
                    ident.push_str(&second_segment.ident.to_string());
                    segments.push(ident);
                } else {
                    segments.push(ident);
                }
            }
        }
        _ => {
            use quote::ToTokens;
            let mut tokens = proc_macro2::TokenStream::new();
            ty.to_tokens(&mut tokens);
            segments.push(tokens.to_string());
        }
    }
}

/// First flattens the UseTree and then normalizing the paths.
fn usetree_to_paths(use_tree: &UseTree, module_path: &str) -> Vec<String> {
    let crate_root = module_path.split_once("::").map_or(module_path, |s| s.0);
    let paths = flatten_use_tree(
        syn::Path {
            leading_colon: None,
            segments: Punctuated::new(),
        },
        use_tree,
    );

    let mut retvec = vec![];
    for path in paths {
        let mut path = path
            .replace(' ', "")
            .replace('-', "_")
            .replace("crate::", &format!("{}::", crate_root));

        if path.contains("super::") {
            let parent_module = {
                let mut parts = module_path.split("::").collect::<Vec<_>>();
                parts.pop();
                parts.join("::")
            };
            path = path.replace("super::", &parent_module);
        }

        retvec.push(path);
    }
    retvec
}

/// Flattens a usetree. For example: use relay_general::protocol::{Foo, Bar,
/// Baz} into [relay_general::protocol::Foo, relay_general::protocol::Bar, relay_general::protocol::Baz]
fn flatten_use_tree(mut leading_path: syn::Path, use_tree: &UseTree) -> Vec<String> {
    match use_tree {
        UseTree::Path(use_path) => {
            leading_path.segments.push(use_path.ident.clone().into());
            flatten_use_tree(leading_path, &use_path.tree)
        }
        UseTree::Name(use_name) => {
            leading_path.segments.push(use_name.ident.clone().into());
            vec![quote::quote!(#leading_path).to_string()]
        }
        UseTree::Group(use_group) => {
            let mut paths = Vec::new();
            for item in &use_group.items {
                paths.extend(flatten_use_tree(leading_path.clone(), item));
            }
            paths
        }

        UseTree::Rename(use_rename) => {
            leading_path.segments.push(use_rename.rename.clone().into());
            vec![quote::quote!(#leading_path).to_string()]
        }
        _ => vec![quote::quote!(#leading_path).to_string()],
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
        let rust_file_paths = {
            let path = self
                .path
                .clone()
                .unwrap_or_else(|| std::env::current_dir().unwrap());

            find_rs_files(&path)
        };

        // Before we can iterate over the pii fields properly, we make a mapping between all
        // paths to types and their AST node.
        let (all_types, use_statements) =
            FileSyntaxVisitor::get_types_and_use_statements(&rust_file_paths)?;

        let pii_types = find_pii_fields(
            self.item.as_deref(),
            &all_types,
            &use_statements,
            &self.pii_values,
        );

        let output_vec = get_pii_fields_output(pii_types, "".to_string());

        match self.output {
            Some(ref path) => self.write_pii(File::create(path)?, &output_vec)?,
            None => self.write_pii(std::io::stdout(), &output_vec)?,
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, ValueEnum, Default)]
pub enum SchemaFormat {
    #[default]
    Json,
    Yaml,
}

/// Takes in the path to a rust file and gives back the way you'd refer to it in a use-statement.
/// e.g. "/Users/tor/prog/rust/relay/relay-general/src/protocol/types.rs -> relay_general::protocol"
fn rust_file_to_use_path(file_path: &Path) -> String {
    let crate_name = {
        let file_str = file_path.as_os_str().to_str().unwrap();
        let src_index = file_str.find("/src/").unwrap();
        let back_index = file_str[..src_index].rfind('/').unwrap() + 1;
        file_str.split_at(src_index).0.split_at(back_index).1
    };
    let parent_dir = file_path.parent().unwrap();
    let file_stem = file_path.file_stem().unwrap().to_string_lossy().to_string();

    let is_module = is_file_module(file_path);

    let mut module_path = parent_dir
        .components()
        .map(|part| part.as_os_str().to_string_lossy().to_string())
        .filter(|part| part != "src")
        .collect::<Vec<_>>();

    if is_module {
        module_path.push(file_stem);
    }

    let mut use_path = module_path.join("::");
    use_path = use_path
        .split_at(use_path.find(crate_name).unwrap())
        .1
        .replace('-', "_");

    use_path
}

/// Checks if a file is a module, which is needed to convert a file path to a module path
fn is_file_module(file_path: &Path) -> bool {
    let parent_dir = file_path.parent().unwrap();
    let file_stem = file_path.file_stem().unwrap().to_string_lossy().to_string();

    let mod_rs_path = parent_dir.join("mod.rs");
    if mod_rs_path.exists() {
        let mod_rs_file = fs::File::open(mod_rs_path).unwrap();
        let reader = std::io::BufReader::new(mod_rs_file);

        for line in reader.lines() {
            let line = line.unwrap();
            if line.trim().starts_with("pub mod") && line.contains(&file_stem) {
                return true;
            }
        }
    }

    for entry in fs::read_dir(parent_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "rs") && path != *file_path {
            let file = fs::File::open(path).unwrap();
            let reader = std::io::BufReader::new(file);

            for line in reader.lines() {
                let line = line.unwrap();
                if line.trim().starts_with("pub mod") && line.contains(&file_stem) {
                    return true;
                }
            }
        }
    }
    false
}

/// Checks if an attribute is equal to a specified name and value.
fn has_attr_value(attr: &Attribute, name: &str, value: &str) -> bool {
    if let Meta::List(meta_list) = &attr.meta {
        if meta_list.path.is_ident("metastructure") {
            let mut iter = meta_list.tokens.clone().into_iter();
            while let Some(token) = iter.next() {
                if let proc_macro2::TokenTree::Ident(ident) = &token {
                    if ident == name {
                        if let Some(proc_macro2::TokenTree::Punct(punct)) = iter.next() {
                            if punct.as_char() == '=' {
                                if let Some(proc_macro2::TokenTree::Literal(lit_val)) = iter.next()
                                {
                                    let mut lit_val = lit_val.to_string();
                                    lit_val.pop(); // remove superfluous quotes. "\"true\"" -> "true"
                                    lit_val.remove(0);
                                    if lit_val == value {
                                        return true;
                                    }
                                }
                            }
                        }
                    };
                }
            }
        }
    }
    false
}

/// Checks if a field has the metastructure "pii" and if it does, if it is equal to any
/// of the values that the user defines.
fn has_pii_value(pii_values: &Vec<String>, field: &Field) -> bool {
    for attr in &field.attrs {
        for pii_value in pii_values {
            if has_attr_value(attr, "pii", pii_value) {
                return true;
            }
        }
    }
    false
}

/// Finds all pii-fields of either a single type if provided, or of all the defined types in the
/// rust project.
fn find_pii_fields(
    item: Option<&str>,
    all_types: &HashMap<String, EnumOrStruct>,
    use_statements: &HashMap<String, BTreeSet<String>>,
    pii_values: &Vec<String>,
) -> BTreeSet<Vec<TypeAndField>> {
    let mut pii_types = BTreeSet::new();
    let mut current_path = vec![];
    match item {
        Some(path) => {
            if let Some(structorenum) = { all_types.get(path).cloned() } {
                let module_path = path.rsplit_once("::").unwrap().0.to_owned();
                let theitem = structorenum;
                let mut visitor = TypeVisitor {
                    module_path,
                    current_type: String::new(),
                    node_map: all_types,
                    use_statements,
                    pii_values,
                    pii_types: &mut pii_types,
                    current_path: &mut current_path,
                };
                match theitem {
                    EnumOrStruct::Struct(itemstruct) => visitor.visit_item_struct(&itemstruct),
                    EnumOrStruct::Enum(itemenum) => visitor.visit_item_enum(&itemenum),
                };
            } else {
                panic!("Please provide a fully qualified path to a struct or an enum. E.g. 'relay_general::protocol::Event'");
            }
        }
        None => {
            for (key, value) in all_types.iter() {
                let module_path = key.rsplit_once("::").unwrap().0.to_owned();
                let mut visitor = TypeVisitor {
                    module_path,
                    current_type: String::new(),
                    node_map: all_types,
                    use_statements,
                    pii_values,
                    pii_types: &mut pii_types,
                    current_path: &mut current_path,
                };
                match value {
                    EnumOrStruct::Struct(itemstruct) => visitor.visit_item_struct(itemstruct),
                    EnumOrStruct::Enum(itemenum) => visitor.visit_item_enum(itemenum),
                };
            }
        }
    }
    pii_types
}

/// Converts the content of `PII_TYPES` into a `Vec<Pii>` which is the format that is pushed to
/// the sentry-docs.
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

/// Represents the output of a field which has the correct Pii value.
#[derive(Debug, Serialize, Default)]
struct Pii {
    path: String,
}

// Due to global variables, have to both run the tests sequentially as well as clearing the globals
#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    const RUST_CRATE: &str = "../../tests/test_pii_docs";

    #[test]
    fn test_use_statements() {
        let rust_crate = PathBuf::from_str(RUST_CRATE).unwrap();

        let rust_file_paths = find_rs_files(&rust_crate);
        let (_, use_statements) =
            FileSyntaxVisitor::get_types_and_use_statements(&rust_file_paths).unwrap();
        insta::assert_debug_snapshot!(use_statements);
    }

    #[test]
    fn test_pii_true() {
        let rust_crate = PathBuf::from_str(RUST_CRATE).unwrap();

        let rust_file_paths = find_rs_files(&rust_crate);
        let (all_types, use_statements) =
            FileSyntaxVisitor::get_types_and_use_statements(&rust_file_paths).unwrap();

        let pii_types =
            find_pii_fields(None, &all_types, &use_statements, &vec!["true".to_string()]);

        let output = get_pii_fields_output(pii_types, "".into());
        insta::assert_debug_snapshot!(output);
    }

    #[test]
    fn test_pii_false() {
        let rust_crate = PathBuf::from_str(RUST_CRATE).unwrap();

        let rust_file_paths = find_rs_files(&rust_crate);
        let (all_types, use_statements) =
            FileSyntaxVisitor::get_types_and_use_statements(&rust_file_paths).unwrap();

        let pii_types = find_pii_fields(
            None,
            &all_types,
            &use_statements,
            &vec!["false".to_string()],
        );

        let output = get_pii_fields_output(pii_types, "".into());
        insta::assert_debug_snapshot!(output);
    }

    #[test]
    fn test_pii_all() {
        let rust_crate = PathBuf::from_str(RUST_CRATE).unwrap();

        let rust_file_paths = find_rs_files(&rust_crate);
        let (all_types, use_statements) =
            FileSyntaxVisitor::get_types_and_use_statements(&rust_file_paths).unwrap();

        let pii_types = find_pii_fields(
            None,
            &all_types,
            &use_statements,
            &vec!["true".to_string(), "false".to_string(), "maybe".to_string()],
        );

        let output = get_pii_fields_output(pii_types, "".into());
        insta::assert_debug_snapshot!(output);
    }
}
