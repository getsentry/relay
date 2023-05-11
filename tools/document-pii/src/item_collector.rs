//! Collecting full paths to items and AST-nodes of types
//!
//! This module will iterate over all the rust files given and collect all of the full path names
//! and the actual AST-node for the types defined in those paths. This is later needed for finding
//! PII fields recursively.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, DirEntry};
use std::io::BufRead;
use std::path::{Path, PathBuf};

use anyhow::anyhow;
use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::{ItemEnum, ItemStruct, UseTree};

use crate::EnumOrStruct;

pub struct TypesAndUseStatements {
    // Maps the path name of an item to its actual AST node.
    pub all_types: HashMap<String, EnumOrStruct>,
    // Maps the paths in scope to different modules. For use in constructing the full path
    // of an item from its type name.
    pub paths_in_scope: BTreeMap<String, BTreeSet<String>>,
}

/// Iterates over the rust files to collect all the types and use_statements, which is later
/// used for recursively looking for pii_fields afterwards.
#[derive(Default)]
pub struct AstItemCollector {
    module_path: String,
    /// Maps from the full path of a type to its AST node.
    all_types: HashMap<String, EnumOrStruct>,
    /// Maps from a module_path to all the use_statements and path of local items.
    use_statements: BTreeMap<String, BTreeSet<String>>,
}

impl AstItemCollector {
    fn insert_use_statements(&mut self, use_statements: Vec<String>) {
        self.use_statements
            .entry(self.module_path.clone())
            .or_default()
            .extend(use_statements);
    }

    /// Gets both a mapping of the full type to a type and its actual AST node, and also the
    /// use_statements in its module, which is needed to fetch the types that it referes to in its
    /// fields.
    pub fn get_types_and_use_statements(
        paths: &[PathBuf],
    ) -> anyhow::Result<TypesAndUseStatements> {
        let mut visitor = Self::default();

        visitor.visit_files(paths)?;

        Ok(TypesAndUseStatements {
            all_types: visitor.all_types,
            paths_in_scope: visitor.use_statements,
        })
    }

    fn visit_files(&mut self, paths: &[PathBuf]) -> anyhow::Result<()> {
        for path in paths {
            self.module_path = module_name_from_file(path)?;

            let syntax_tree: syn::File = {
                let file_content = fs::read_to_string(path.as_path())?;
                syn::parse_file(&file_content)?
            };

            self.visit_file(&syntax_tree);
        }
        Ok(())
    }
}

/// The types and use statements items collected from the rust files
impl<'ast> Visit<'ast> for AstItemCollector {
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

fn normalize_type_path(mut path: String, crate_root: &str, module_path: &str) -> String {
    path = path
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
    path
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

    paths
        .into_iter()
        .map(|path| normalize_type_path(path, crate_root, module_path))
        .collect()
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
        // Currently this script can't handle glob imports, which, we shouldn't use anyway.
        UseTree::Glob(_) => vec![quote::quote!(#leading_path).to_string()],
    }
}

fn crate_name_from_file(file_path: &Path) -> anyhow::Result<String> {
    let file_str = file_path
        .as_os_str()
        .to_str()
        .ok_or_else(|| anyhow!("Invalid file path: {}", file_path.display()))?;

    let src_index = file_str
        .find("/src/")
        .or_else(|| file_str.find("\\src\\"))
        .ok_or_else(|| {
            anyhow!(
                "Invalid file path (missing '/src/' or '\\src\\'): {}",
                file_path.display()
            )
        })?;

    let back_index = file_str[..src_index]
        .rfind('/')
        .or_else(|| file_str[..src_index].rfind('\\'))
        .ok_or_else(|| {
            anyhow!(
                "Invalid file path (missing separator before '/src/' or '\\src\\'): {}",
                file_path.display()
            )
        })?
        + 1;

    Ok(file_str
        .split_at(src_index)
        .0
        .split_at(back_index)
        .1
        .replace('-', "_"))
}

fn add_file_stem_to_module_path(
    file_path: &Path,
    module_path: &mut Vec<String>,
) -> anyhow::Result<()> {
    let file_stem = file_path
        .file_stem()
        .ok_or_else(|| {
            anyhow!(
                "Invalid file path (unable to find file stem): {}",
                file_path.display()
            )
        })?
        .to_string_lossy()
        .into_owned();

    module_path.push(file_stem);
    Ok(())
}

fn split_path_at_crate_name(module_path: &[String], file_path: &Path) -> anyhow::Result<String> {
    // Build the final use-path by joining the module path components
    // with "::" and removing the '-' character if any, since Rust module
    // names replace '-' with '_'.
    let use_path = module_path.join("::");
    let crate_name = crate_name_from_file(file_path)?;

    let index = use_path.find(&crate_name).ok_or_else(|| {
        anyhow!(
            "Failed to find crate name '{}' in use path '{}'",
            crate_name,
            use_path
        )
    })?;
    let use_path = use_path.split_at(index).1.to_string();

    Ok(use_path)
}

/// Takes in the path to a Rust file and returns the path as you'd refer to it in a use-statement.
/// e.g. "/Users/tor/prog/rust/relay/relay-general/src/protocol/types.rs" -> "relay_general::protocol"
fn module_name_from_file(file_path: &Path) -> anyhow::Result<String> {
    let mut module_path = file_path
        .parent()
        .ok_or_else(|| {
            anyhow!(
                "Invalid file path (unable to find parent directory): {}",
                file_path.display()
            )
        })?
        .components()
        .map(|part| part.as_os_str().to_string_lossy().replace('-', "_"))
        .filter(|part| part != "src")
        .collect::<Vec<String>>();

    if is_file_module(file_path)? {
        add_file_stem_to_module_path(file_path, &mut module_path)?;
    }

    let use_path = split_path_at_crate_name(&module_path, file_path)?;

    Ok(use_path)
}

fn is_file_declared_from_mod_file(parent_dir: &Path, file_stem: &str) -> anyhow::Result<bool> {
    let mod_rs_path = parent_dir.join("mod.rs");
    if !mod_rs_path.exists() {
        return Ok(false);
    }
    // If "mod.rs" exists, we need to check if it declares the file in question as a module.
    // The declaration line would start with "pub mod" and contain the file stem.
    let mod_rs_file = fs::File::open(mod_rs_path)?;
    let reader = std::io::BufReader::new(mod_rs_file);

    for line in reader.lines() {
        let line = line?;
        if line.trim().starts_with("pub mod") && line.contains(file_stem) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn is_file_declared_from_other_file(
    entry: &DirEntry,
    file_stem: &str,
    file_path: &Path,
) -> anyhow::Result<bool> {
    let path = entry.path();

    if path.is_file() && path.extension().map_or(false, |ext| ext == "rs") && path != *file_path {
        // Read the file and search for the same declaration pattern: "pub mod" and file stem.
        let file = fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            if line.trim().starts_with("pub mod") && line.contains(file_stem) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

// Checks if a file is a Rust module.
fn is_file_module(file_path: &Path) -> anyhow::Result<bool> {
    let parent_dir = file_path
        .parent()
        .ok_or_else(|| anyhow!("Invalid file path: {}", file_path.display()))?;
    let file_stem = file_path
        .file_stem()
        .ok_or_else(|| anyhow!("Invalid file path: {}", file_path.display()))?
        .to_string_lossy()
        .into_owned();

    if is_file_declared_from_mod_file(parent_dir, &file_stem)? {
        return Ok(true);
    }

    for entry in fs::read_dir(parent_dir)? {
        let entry = entry?;
        if is_file_declared_from_other_file(&entry, &file_stem, file_path)? {
            return Ok(true);
        }
    }

    Ok(false)
}
