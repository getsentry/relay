#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use clap::{App, Arg, ArgMatches};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::hash::Hash;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use syn::punctuated::Punctuated;
use syn::{visit::Visit, ItemEnum};
use syn::{Attribute, Field, ItemStruct, Lit, Meta, MetaNameValue, Type, UseTree, Variant};
use walkdir::WalkDir;

// unfortunately many global vars, since i cant add extra arguments to trait methods
lazy_static::lazy_static! {
    // When iterating recursively, this keeps track of the current path
    static ref CURRENT_PATH: Mutex<Vec<TypeAndField>> = Mutex::new(vec![]);
    // a set of CURRENT_PATH whenever it hits a pii=true field
    static ref PII_TYPES: Mutex<HashSet<Vec<TypeAndField>>> = Mutex::new(HashSet::new());
    // All the structs and enums, where the key is the full path. The string in the tuple represents
    // the module path to the item. Now that I think of it, should be able to just get it from the key.
    static ref ALL_TYPES: Mutex<HashMap<String, EnumOrStruct>> = Mutex::new(HashMap::new());
    /// The values that we wanna match on, (true, false, maybe)
    static ref PII_VALUES: Mutex<Vec<String>> = Mutex::new(vec![]);
    /// All the use statements in a given module.
    static ref USE_STATEMENTS: Mutex<HashMap<String, HashSet<String>>> = Mutex::new(HashMap::new());
}

/// It's only purpose is to populate ALL_TYPES and USE_STATEMENTS
struct FileSyntaxVisitor {
    module_path: String,
}

impl FileSyntaxVisitor {
    fn insert_use_statements(&self, use_statements: Vec<String>) {
        USE_STATEMENTS
            .lock()
            .unwrap()
            .entry(self.module_path.clone())
            .or_default()
            .extend(use_statements);
    }

    fn fill_pii_hashmaps(paths: &Vec<PathBuf>) {
        for path in paths {
            // first iterate through all rust files to create the hashmap
            let mut hashmap_filler = FileSyntaxVisitor {
                module_path: rust_file_to_use_path(path.as_path()),
            };
            let file_content = fs::read_to_string(path.as_path()).unwrap();
            dbg!(&file_content);
            let syntax_tree: syn::File = syn::parse_file(&file_content).unwrap();
            hashmap_filler.visit_file(&syntax_tree);
        }
    }
}

impl<'ast> Visit<'ast> for FileSyntaxVisitor {
    fn visit_item_struct(&mut self, node: &'ast ItemStruct) {
        // These are pretty common and we know they dont contain PII so might as well not include
        // them to save time.
        for common_value in ["Annotated", "Value"] {
            if node.ident == common_value {
                return;
            }
        }

        let struct_name = format!("{}::{}", self.module_path, node.ident);
        self.insert_use_statements(vec![struct_name.clone()]);
        ALL_TYPES
            .lock()
            .unwrap()
            .insert(struct_name, EnumOrStruct::Struct(node.clone()));
    }

    fn visit_item_enum(&mut self, node: &'ast ItemEnum) {
        let enum_name = format!("{}::{}", self.module_path, node.ident);
        self.insert_use_statements(vec![enum_name.clone()]);
        ALL_TYPES
            .lock()
            .unwrap()
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

#[derive(PartialEq, Eq, Hash, Clone, Debug, Default)]
struct TypeAndField {
    type_name: String, //shoud be full path
    field_name: String,
}

fn find_rs_files(dir: &str) -> Vec<std::path::PathBuf> {
    let walker = WalkDir::new(dir).into_iter();
    let mut rs_files = Vec::new();

    for entry in walker.filter_map(walkdir::Result::ok) {
        if !entry.clone().path().to_string_lossy().contains("/src/") {
            continue;
        }
        if entry.file_type().is_file() && entry.path().extension().map_or(false, |ext| ext == "rs")
        {
            rs_files.push(entry.into_path());
        }
    }
    rs_files
}

#[derive(Debug, Clone)]
enum EnumOrStruct {
    Struct(ItemStruct),
    Enum(ItemEnum),
}

// Need this to wrap lazy_statics in a mutex. everything is single-threaded so it shouldn't matter.
unsafe impl Send for EnumOrStruct {}

// In practice this is used as a way to keep state between the different methods on the visitor
// trait, as the predefined trait stops me from adding the parameters I need.
#[derive(Default)]
struct TypeVisitor {
    module_path: String,
    current_type: String,
}

impl<'ast> Visit<'ast> for TypeVisitor {
    fn visit_item_struct(&mut self, node: &'ast ItemStruct) {
        self.current_type = node.ident.to_string();
        // This should stop any infinite recursion
        if !CURRENT_PATH
            .lock()
            .unwrap()
            .iter()
            .any(|x| x.type_name == self.current_type)
        {
            for field in node.fields.iter() {
                self.visit_field(field);
            }
        }
    }

    fn visit_item_enum(&mut self, node: &'ast ItemEnum) {
        self.current_type = node.ident.to_string();
        if !CURRENT_PATH
            .lock()
            .unwrap()
            .iter()
            .any(|x| x.type_name == self.current_type)
        {
            for variant in node.variants.iter() {
                for field in variant.fields.iter() {
                    self.visit_field(field);
                }
            }
        }
    }

    fn visit_variant(&mut self, node: &'ast Variant) {
        for field in node.fields.iter() {
            self.visit_field(field);
        }
    }

    fn visit_field(&mut self, node: &'ast Field) {
        CURRENT_PATH.lock().unwrap().push(TypeAndField {
            type_name: self.current_type.clone(),
            field_name: node
                .clone()
                .ident
                .map(|x| x.to_string())
                .unwrap_or_else(|| "{{Unnamed}}".to_string()),
        });

        if has_pii_value(node) {
            PII_TYPES
                .lock()
                .unwrap()
                .insert(CURRENT_PATH.lock().unwrap().clone());
        }

        let field_types = type_to_string(&node.ty);

        let local_paths = USE_STATEMENTS
            .lock()
            .unwrap()
            .get(&self.module_path)
            .unwrap()
            .clone();

        for field_type in &field_types {
            for use_path in &local_paths {
                if use_path.split("::").last().unwrap() == field_type.trim() {
                    let enum_or_struct = {
                        let guard = ALL_TYPES.lock().unwrap();
                        guard.get(use_path).cloned()
                    };

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
        (CURRENT_PATH.lock().unwrap().pop());
    }
}

fn type_to_string(ty: &Type) -> Vec<String> {
    let mut segments: Vec<String> = Vec::new();
    type_to_string_helper(ty, &mut segments);
    segments
}

fn type_to_string_helper(ty: &Type, segments: &mut Vec<String>) {
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
                                type_to_string_helper(ty, segments);
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

fn usetree_to_paths(use_tree: &UseTree, module_path: &str) -> Vec<String> {
    let crate_root = module_path.split_once("::").map_or(module_path, |s| s.0);
    // Split into two functions because use_tree_to_path uses recursion.
    let paths = use_tree_to_path(
        syn::Path {
            leading_colon: None,
            segments: Punctuated::new(),
        },
        use_tree,
    );
    let mut retvec = vec![];
    for path in paths.split(',') {
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

/// Converts a use tree to individual paths, for example: use relay_general::protocol::{Foo, Bar,
/// Baz} into [relay_general::protocol::Foo, relay_general::protocol::Bar, relay_general::protocol::Baz]
fn use_tree_to_path(mut leading_path: syn::Path, use_tree: &UseTree) -> String {
    match use_tree {
        UseTree::Path(use_path) => {
            leading_path.segments.push(use_path.ident.clone().into());
            use_tree_to_path(leading_path, &use_path.tree)
        }
        UseTree::Name(use_name) => {
            leading_path.segments.push(use_name.ident.clone().into());
            quote::quote!(#leading_path).to_string()
        }
        UseTree::Group(use_group) => {
            let mut paths = Vec::new();
            for item in &use_group.items {
                paths.push(use_tree_to_path(leading_path.clone(), item));
            }
            paths.join(", ")
        }

        UseTree::Rename(use_rename) => {
            leading_path.segments.push(use_rename.rename.clone().into());
            quote::quote!(#leading_path).to_string()
        }
        _ => quote::quote!(#leading_path).to_string(),
    }
}

fn get_argmatches() -> ArgMatches {
    let current_dir = {
        let current_dir = std::env::current_dir().unwrap();
        current_dir.to_string_lossy().to_string()
    };

    App::new("PII attribute finder")
        .about("Tests PII paths in Rust structs and enums")
        .arg(
            Arg::new("path")
                .short('p')
                .long("path")
                .default_value(&current_dir)
                .value_name("PATH")
                .help("Path to your crate/workspace")
                .takes_value(true),
        )
        .arg(
            Arg::new("item")
                .short('i')
                .long("item")
                .value_name("ITEM")
                .help("Path to the struct or enum to test (e.g., relay_protocol::common::event)")
                .takes_value(true),
        )
        .arg(
            Arg::new("unnamed")
                .long("unnamed")
                .value_name("UNNAMED")
                .default_value("")
                .help("Placeholder for unnamed fields")
                .takes_value(true),
        )
        .arg(
            Arg::new("pii")
                .long("pii")
                .value_name("PII")
                .multiple_values(true)
                .default_values(&["true"])
                .help("Which pii values should it find? true/false/maybe")
                .takes_value(true),
        )
        .get_matches()
}

fn main() {
    let matches = get_argmatches();
    let chosen_dir = matches.value_of("path").unwrap();

    if !Path::new(&format!("{}/Cargo.toml", &chosen_dir)).exists() {
        panic!("Cargo.toml not found. Either run this script from a rust crate/workspace, or pass in a valid path with the -p flag");
    }
    let pii_values = matches
        .values_of("pii")
        .unwrap()
        .map(|s| s.to_string())
        .collect();

    let item: Option<&str> = matches.value_of("item");

    run(chosen_dir, pii_values, item);
    let unnamed_replace = {
        let mut unnamed_replace = matches.value_of("unnamed").unwrap().to_string();
        if !unnamed_replace.is_empty() {
            unnamed_replace.push('.');
        };
        unnamed_replace
    };
    let output_vec = get_pretty_pii_field_format(unnamed_replace);

    for x in &output_vec {
        println!("{x}");
    }

    dbg!(output_vec.len());
}

fn run(rust_crate: &str, pii_values: Vec<String>, item: Option<&str>) {
    let paths = find_rs_files(rust_crate);
    dbg!(&paths);
    FileSyntaxVisitor::fill_pii_hashmaps(&paths);
    {
        let mut guard = PII_VALUES.lock().unwrap();
        guard.clear();
        guard.extend(pii_values);
    }
    find_pii_fields(item);
}

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

fn has_attr_value(attr: &Attribute, name: &str, value: &str) -> bool {
    if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
        if meta_list.path.is_ident("metastructure") {
            for nested_meta in meta_list.nested {
                if let syn::NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, lit, .. })) =
                    nested_meta
                {
                    if path.is_ident(name) {
                        if let Lit::Str(lit_str) = lit {
                            return lit_str.value() == value;
                        }
                    }
                }
            }
        }
    }
    false
}

fn has_pii_value(field: &Field) -> bool {
    for attr in &field.attrs {
        for pii_value in PII_VALUES.lock().unwrap().iter() {
            if has_attr_value(attr, "pii", pii_value) {
                return true;
            }
        }
    }
    false
}

fn find_pii_fields(item: Option<&str>) {
    let mut count = 0;
    match item {
        Some(path) => {
            if let Some(structorenum) = {
                let guard = ALL_TYPES.lock().unwrap();
                guard.get(path).cloned()
            } {
                let module_path = path.rsplit_once("::").unwrap().0.to_owned();
                let theitem = structorenum;
                let mut visitor = TypeVisitor {
                    module_path,
                    ..Default::default()
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
            let all_types = ALL_TYPES.lock().unwrap().clone();
            for (key, value) in all_types.iter() {
                count += 1;
                println!("{}/{}", count, all_types.len());
                let module_path = key.rsplit_once("::").unwrap().0.to_owned();
                let mut visitor = TypeVisitor {
                    module_path,
                    ..Default::default()
                };
                match value {
                    EnumOrStruct::Struct(itemstruct) => visitor.visit_item_struct(itemstruct),
                    EnumOrStruct::Enum(itemenum) => visitor.visit_item_enum(itemenum),
                };
            }
        }
    }
}

fn get_pretty_pii_field_format(unnamed_replace: String) -> Vec<String> {
    let mut output_vec = vec![];
    for pii in PII_TYPES.lock().unwrap().iter() {
        let mut output = String::new();
        output.push_str(&pii[0].type_name);

        for path in pii {
            output.push_str(&format!(".{}", path.field_name));
        }

        output = output.replace("{{Unnamed}}.", &unnamed_replace);
        output_vec.push(output);
    }
    output_vec.sort();
    output_vec
}

#[cfg(test)]
mod tests {
    use super::*;
    const RUST_CRATE: &str = "/Users/tor/prog/rust/relay/tests/test_pii_docs";

    #[test]
    fn test_use_statements() {
        run(RUST_CRATE, vec![], None);
        let use_statements = USE_STATEMENTS.lock().unwrap().clone();
        insta::assert_debug_snapshot!(use_statements);
    }

    #[test]
    fn test_pii_true() {
        run(RUST_CRATE, vec!["true".into()], None);
        let output = get_pretty_pii_field_format("".into());

        insta::assert_debug_snapshot!(output);
    }

    #[test]
    fn test_pii_false() {
        run(RUST_CRATE, vec!["false".into()], None);
        let output = get_pretty_pii_field_format("".into());
        dbg!(&output);
        insta::assert_debug_snapshot!(output);
    }

    #[test]
    fn test_pii_all() {
        run(
            RUST_CRATE,
            vec!["true".into(), "maybe".into(), "false".into()],
            None,
        );
        let output = get_pretty_pii_field_format("".into());
        dbg!(&output);

        insta::assert_debug_snapshot!(output);
    }
}
