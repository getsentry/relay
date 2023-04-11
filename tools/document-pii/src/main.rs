#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::fs;
use syn::{Attribute, Field, Item, ItemStruct, Lit, Meta, MetaNameValue, Type};

use walkdir::WalkDir;

fn find_rs_files(dir: &str) -> Vec<std::path::PathBuf> {
    let walker = WalkDir::new(dir).into_iter();
    let mut rs_files = Vec::new();

    for entry in walker.filter_map(walkdir::Result::ok) {
        if entry.clone().path().to_string_lossy().contains("target/") {
            continue;
        }
        if entry.file_type().is_file() && entry.path().extension().unwrap_or_default() == "rs" {
            rs_files.push(entry.into_path());
        }
    }

    rs_files
}

use std::env;
use std::path::{Path, PathBuf};

use syn::{visit::Visit, ItemEnum};

struct TypeVisitor {
    use_statements: Vec<syn::ItemUse>,
    module_path: String,
}

impl<'ast> Visit<'ast> for TypeVisitor {
    fn visit_item_struct(&mut self, node: &'ast ItemStruct) {
        println!("{}::{}", self.module_path, node.ident);
        for field in node.fields.iter() {
            self.visit_field(field);
        }

        syn::visit::visit_item_struct(self, node);
    }

    fn visit_item_enum(&mut self, node: &'ast ItemEnum) {
        println!("{}::{}", self.module_path, node.ident);
        syn::visit::visit_item_enum(self, node);
    }
    fn visit_field(&mut self, node: &'ast Field) {
        if node.ident.as_ref().is_none() {
            return;
        }
        println!("  Field: {}", node.ident.as_ref().unwrap());
        println!("    Type: {}", type_to_string(&node.ty));
    }
    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        self.use_statements.push(node.clone());
        syn::visit::visit_item_use(self, node);
    }
}

fn type_to_string(ty: &Type) -> String {
    match ty {
        Type::Path(type_path) => {
            let segments = type_path
                .path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>()
                .join("::");

            format!("/::{}", segments)
        }
        _ => {
            use quote::ToTokens;
            let mut tokens = proc_macro2::TokenStream::new();
            ty.to_tokens(&mut tokens);
            tokens.to_string()
        }
    }
}

fn process_rust_file(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let file_content = fs::read_to_string(file_path)?;

    let syntax_tree: syn::File = syn::parse_file(&file_content)?;
    let module_path = file_path
        .with_extension("")
        .iter()
        .map(|part| part.to_string_lossy())
        .collect::<Vec<_>>()
        .join("::");

    let mut visitor = TypeVisitor {
        module_path: module_path.clone(),
        use_statements: vec![],
    };

    visitor.visit_file(&syntax_tree);
    for x in visitor.use_statements {}

    Ok(())
}

fn main() {
    let paths = find_rs_files("/Users/tor/prog/rust/relay/");
    for path in paths {
        process_rust_file(path.as_path());
    }
}
