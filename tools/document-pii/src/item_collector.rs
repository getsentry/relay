//! Contains the helper types and functions which help to iterate over all the given rust files,
//! collect all of the full path names and the actual AST-node for the types defined in those paths.
//! This is later needed for finding PII fields recursively.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, DirEntry};
use std::io::BufRead;
use std::path::{Path, PathBuf};

use anyhow::anyhow;
use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::{ItemEnum, ItemStruct, UseTree};

use crate::pii_finder::{FieldsWithAttribute, PiiFinder};
use crate::EnumOrStruct;

pub struct TypesAndScopedPaths {
    // Maps the path name of an item to its actual AST node.
    pub all_types: HashMap<String, EnumOrStruct>,
    // Maps the paths in scope to different modules. For use in constructing the full path
    // of an item from its type name.
    pub scoped_paths: BTreeMap<String, BTreeSet<String>>,
}

impl TypesAndScopedPaths {
    pub fn find_pii_fields(
        &self,
        type_path: Option<&str>,
        pii_values: &Vec<String>,
    ) -> anyhow::Result<BTreeSet<FieldsWithAttribute>> {
        let fields = match type_path {
            // If user provides path to an item, find PII_fields under this item in particular.
            Some(path) => self.find_pii_fields_of_type(path),
            // If no item is provided, find PII fields of all types in crate/workspace.
            None => self.find_pii_fields_of_all_types(),
        }?;

        Ok(fields
            .into_iter()
            .filter(|pii| {
                pii.has_attribute("pii", Some(pii_values))
                    && (pii.has_attribute("retain", Some(&vec!["true".to_string()]))
                        || !pii.has_attribute("additional_properties", None))
            })
            .collect())
    }

    /// Finds all the PII fields recursively of a given type.
    fn find_pii_fields_of_type(
        &self,
        type_path: &str,
    ) -> anyhow::Result<BTreeSet<FieldsWithAttribute>> {
        let mut visitor = PiiFinder::new(type_path, &self.all_types, &self.scoped_paths)?;

        let value = &self
            .all_types
            .get(type_path)
            .ok_or_else(|| anyhow!("Unable to find item with following path: {}", type_path))?;

        match value {
            EnumOrStruct::Struct(itemstruct) => visitor.visit_item_struct(itemstruct),
            EnumOrStruct::Enum(itemenum) => visitor.visit_item_enum(itemenum),
        };
        Ok(visitor.pii_types)
    }

    /// Finds all the PII fields recursively of all the types in the rust crate/workspace.
    fn find_pii_fields_of_all_types(&self) -> anyhow::Result<BTreeSet<FieldsWithAttribute>> {
        let mut pii_types = BTreeSet::new();

        for type_path in self.all_types.keys() {
            pii_types.extend(self.find_pii_fields_of_type(type_path)?);
        }

        Ok(pii_types)
    }
}

/// The types and use statements items collected from the rust files.
#[derive(Default)]
pub struct AstItemCollector {
    module_path: String,
    /// Maps from the full path of a type to its AST node.
    all_types: HashMap<String, EnumOrStruct>,
    /// Maps from a module_path to all the types that are in the module's scope.
    scoped_paths: BTreeMap<String, BTreeSet<String>>,
}

impl AstItemCollector {
    fn insert_scoped_paths(&mut self, use_statements: Vec<String>) {
        self.scoped_paths
            .entry(self.module_path.clone())
            .or_default()
            .extend(use_statements);
    }

    /// Gets both a mapping of the full path to a type and its actual AST node, and also the
    /// use_statements in its module, which is needed to fetch the types that it referes to in its
    /// fields.
    pub fn collect(paths: &[PathBuf]) -> anyhow::Result<TypesAndScopedPaths> {
        let mut visitor = Self::default();

        visitor.visit_files(paths)?;

        Ok(TypesAndScopedPaths {
            all_types: visitor.all_types,
            scoped_paths: visitor.scoped_paths,
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

impl<'ast> Visit<'ast> for AstItemCollector {
    fn visit_item_struct(&mut self, node: &'ast ItemStruct) {
        let struct_name = format!("{}::{}", self.module_path, node.ident);
        self.insert_scoped_paths(vec![struct_name.clone()]);
        self.all_types
            .insert(struct_name, EnumOrStruct::Struct(node.clone()));
    }

    fn visit_item_enum(&mut self, node: &'ast ItemEnum) {
        let enum_name = format!("{}::{}", self.module_path, node.ident);
        self.insert_scoped_paths(vec![enum_name.clone()]);
        self.all_types
            .insert(enum_name, EnumOrStruct::Enum(node.clone()));
    }

    fn visit_item_use(&mut self, i: &'ast syn::ItemUse) {
        let use_statements = usetree_to_paths(&i.tree, &self.module_path)
            .iter()
            .filter(|s| s.contains("relay"))
            .cloned()
            .collect();

        self.insert_scoped_paths(use_statements);
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

/// Flattens a usetree.
///
/// For example: `use protocol::{Foo, Bar, Baz}` into `[protocol::Foo, protocol::Bar,
/// protocol::Baz]`.
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
    // We know the crate_name is located like home/foo/bar/crate_name/src/...
    // We therefore first find the index of the '/' to the left of src, then we find the index
    // of the '/' to the left of that, and the crate_name will be whats between those indexes.
    let file_str = file_path.to_string_lossy();

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
        .to_string())
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

/// Takes in the path to a Rust file and returns the path as you'd refer to it in a use-statement.
///
/// e.g. `"relay/relay-event_schema/src/protocol/types.rs"` -> `"relay_event_schema::protocol"`.
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
        .map(|part| part.as_os_str().to_string_lossy().into_owned())
        .filter(|part| part != "src")
        .collect::<Vec<String>>();

    if is_file_module(file_path)? {
        add_file_stem_to_module_path(file_path, &mut module_path)?;
    }

    let crate_name = crate_name_from_file(file_path).unwrap();

    // Removes all the folders before the crate name, and concatenates to a string.
    Ok(module_path
        .iter()
        .position(|s| s == &crate_name)
        .map(|index| &module_path[index..])
        .ok_or_else(|| anyhow!("Couldn't find crate name {}.", crate_name))?
        .join("::")
        .replace('-', "_"))
}

fn is_file_declared_from_mod_file(parent_dir: &Path, file_stem: &str) -> anyhow::Result<bool> {
    let mod_rs_path = parent_dir.join("mod.rs");
    if !mod_rs_path.exists() {
        return Ok(false);
    }
    // If "mod.rs" exists, we need to check if it declares the file in question as a module.
    // The declaration line would start with "pub mod" and contain the file stem.
    let mod_rs_file: fs::File = fs::File::open(mod_rs_path)?;
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
        .to_string_lossy();

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
