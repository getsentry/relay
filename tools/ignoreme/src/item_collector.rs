use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::BufRead;
use std::path::{Path, PathBuf};

use crate::EnumOrStruct;

use anyhow::anyhow;
use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::{ItemEnum, ItemStruct, UseTree};

/// Iterates over the rust files to collect all the types and use_statements, which is later
/// used for recursively looking for pii_fields afterwards.
pub struct ItemCollector {
    module_path: String,
    /// Maps from the full path of a type to its AST node.
    all_types: HashMap<String, EnumOrStruct>,
    /// Maps from a module_path to all the use_statements and path of local items.
    use_statements: BTreeMap<String, BTreeSet<String>>,
}

impl ItemCollector {
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
    pub fn get_types_and_use_statements(
        paths: &Vec<PathBuf>,
    ) -> anyhow::Result<(
        HashMap<String, EnumOrStruct>,
        BTreeMap<String, BTreeSet<String>>,
    )> {
        let mut visitor = Self {
            module_path: String::new(),
            use_statements: BTreeMap::new(),
            all_types: HashMap::new(),
        };
        for path in paths {
            visitor.module_path = rust_file_to_use_path(path)?;
            let file_content = fs::read_to_string(path.as_path())?;
            let syntax_tree: syn::File = syn::parse_file(&file_content)?;
            visitor.visit_file(&syntax_tree);
        }
        Ok((visitor.all_types, visitor.use_statements))
    }
}

/// Iterates through all the rust files in order to make the `full-type-path -> AST node' map
/// and the 'module-module_pathath -> use-statements' map. These are needed for the 'find_pii_fields' function.

impl<'ast> Visit<'ast> for ItemCollector {
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

/// Takes in the path to a rust file and gives back the way you'd refer to it in a use-statement.
/// e.g. "/Users/tor/prog/rust/relay/relay-general/src/protocol/types.rs -> relay_general::protocol"
fn rust_file_to_use_path(file_path: &Path) -> anyhow::Result<String> {
    let crate_name = {
        let file_str = file_path
            .as_os_str()
            .to_str()
            .ok_or_else(|| anyhow!("Invalid file path: {}", file_path.display()))?;
        let src_index = file_str.find("/src/").ok_or_else(|| {
            anyhow!(
                "Invalid file path (missing '/src/'): {}",
                file_path.display()
            )
        })?;
        let back_index = file_str[..src_index].rfind('/').ok_or_else(|| {
            anyhow!(
                "Invalid file path (missing separator before '/src/'): {}",
                file_path.display()
            )
        })? + 1;
        file_str.split_at(src_index).0.split_at(back_index).1
    };
    let parent_dir = file_path.parent().ok_or_else(|| {
        anyhow!(
            "Invalid file path (unable to find parent directory): {}",
            file_path.display()
        )
    })?;
    let file_stem = file_path
        .file_stem()
        .ok_or_else(|| {
            anyhow!(
                "Invalid file path (unable to find file stem): {}",
                file_path.display()
            )
        })?
        .to_string_lossy()
        .to_string();

    let is_module = is_file_module(file_path)?;

    let mut module_path = parent_dir
        .components()
        .map(|part| part.as_os_str().to_string_lossy().to_string())
        .filter(|part| part != "src")
        .collect::<Vec<_>>();

    if is_module {
        module_path.push(file_stem);
    }

    let use_path = {
        let use_path = module_path.join("::");
        let index = use_path.find(crate_name).ok_or_else(|| {
            anyhow!(
                "Failed to find crate name '{}' in use path '{}'",
                crate_name,
                use_path
            )
        })?;
        use_path.split_at(index).1.replace('-', "_")
    };

    Ok(use_path)
}

/// Checks if a file is a module, which is needed to convert a file path to a module path
fn is_file_module(file_path: &Path) -> anyhow::Result<bool> {
    let parent_dir = file_path
        .parent()
        .ok_or_else(|| anyhow!("Invalid file path: {}", file_path.display()))?;
    let file_stem = file_path
        .file_stem()
        .ok_or_else(|| anyhow!("Invaild file path: {}", file_path.display()))?
        .to_string_lossy()
        .to_string();

    let mod_rs_path = parent_dir.join("mod.rs");
    if mod_rs_path.exists() {
        let mod_rs_file = fs::File::open(mod_rs_path)?;
        let reader = std::io::BufReader::new(mod_rs_file);

        for line in reader.lines() {
            let line = line?;
            if line.trim().starts_with("pub mod") && line.contains(&file_stem) {
                return Ok(true);
            }
        }
    }

    for entry in fs::read_dir(parent_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "rs") && path != *file_path {
            let file = fs::File::open(path)?;
            let reader = std::io::BufReader::new(file);

            for line in reader.lines() {
                let line = line?;
                if line.trim().starts_with("pub mod") && line.contains(&file_stem) {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}
