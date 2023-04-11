#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::fs;
use syn::punctuated::Punctuated;
use syn::{Field, Item, ItemStruct, Lit, Meta, MetaNameValue, Type, UsePath, UseTree, Variant};

use walkdir::WalkDir;

use lazy_static::lazy_static;
use std::cell::RefCell;

// global var since i cant add extra arguments to trait methods
static mut VISITED_TYPES: Vec<String> = Vec::new();

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
    current_path: Vec<String>,
}

impl<'ast> Visit<'ast> for TypeVisitor {
    fn visit_item_struct(&mut self, node: &'ast ItemStruct) {
        unsafe { VISITED_TYPES.clear() }
        println!("{}::{}", self.module_path, node.ident);
        for field in node.fields.iter() {
            self.visit_field(field);
        }

        syn::visit::visit_item_struct(self, node);
    }

    fn visit_item_enum(&mut self, node: &'ast ItemEnum) {
        unsafe { VISITED_TYPES.clear() }
        println!("{}::{}", self.module_path, node.ident);
        for variant in node.variants.iter() {
            for field in variant.fields.iter() {
                self.visit_field(field);
            }
        }
        syn::visit::visit_item_enum(self, node);
    }
    fn visit_variant(&mut self, node: &'ast Variant) {
        //println!("  Variant: {}", node.ident);
        for field in node.fields.iter() {
            self.visit_field(field);
        }
    }

    fn visit_field(&mut self, node: &'ast Field) {
        if node.ident.as_ref().is_none() {
            return;
        }
        //println!("  Field: {}", node.ident.as_ref().unwrap());
        let mut x = type_to_string(&node.ty);
        let types: Vec<String> = x.split(",").map(|s| s.to_owned()).collect();
        for x in types {
            for bro in &self.use_statements {
                for wtf in use_tree_to_paths(&bro.tree) {
                    if wtf.ends_with(&x.trim()) && unsafe { !VISITED_TYPES.contains(&wtf) } {
                        println!("   Type: {}", wtf);
                        unsafe {
                            VISITED_TYPES.push(wtf.clone());
                        }
                    }
                }
            }
        }
    }

    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        if should_keep_itemuse(node) {
            self.use_statements.push(node.clone());
        }
        syn::visit::visit_item_use(self, node);
    }
}

fn should_keep_itemuse(node: &syn::ItemUse) -> bool {
    let x = use_tree_to_path(
        syn::Path {
            leading_colon: None,
            segments: Punctuated::new(),
        },
        &node.tree,
    );

    let y = x.split(',');
    for x in y {
        let x = x.trim().replace(' ', "");
        if x.starts_with("relay") || x.starts_with("crate") {
            return true;
        }
    }
    false
}

fn footype_to_string(ty: &Type) -> String {
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

fn type_to_string(ty: &Type) -> String {
    let mut segments: Vec<String> = Vec::new();
    process_type(ty, &mut segments);
    segments.join(", ")
    //format!("/::{}", segments.join(", "))
}

fn process_type(ty: &Type, segments: &mut Vec<String>) {
    match ty {
        Type::Path(type_path) => {
            for segment in type_path.path.segments.iter() {
                let ident = segment.ident.to_string();
                segments.push(ident);

                let args = &segment.arguments;
                match args {
                    syn::PathArguments::AngleBracketed(angle_bracketed) => {
                        for generic_arg in angle_bracketed.args.iter() {
                            match generic_arg {
                                syn::GenericArgument::Type(ty) => {
                                    process_type(ty, segments);
                                }
                                _ => continue,
                            }
                        }
                    }
                    _ => {}
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
        current_path: vec![],
    };

    visitor.visit_file(&syntax_tree);
    for itemuse in visitor.use_statements {
        for thepath in use_tree_to_paths(&itemuse.tree) {
            //dbg!(thepath);
        }
    }
    Ok(())
}

fn use_tree_to_paths(use_tree: &UseTree) -> Vec<String> {
    let x = use_tree_to_path(
        syn::Path {
            leading_colon: None,
            segments: Punctuated::new(),
        },
        use_tree,
    );
    let mut retvec = vec![];
    for y in x.split(',') {
        retvec.push(y.replace(" ", ""));
    }
    retvec
}

fn use_tree_to_path(mut leading_path: syn::Path, use_tree: &UseTree) -> String {
    match use_tree {
        UseTree::Path(use_path) => {
            leading_path.segments.push(use_path.ident.clone().into());
            use_tree_to_path(leading_path, &*use_path.tree)
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

fn main() {
    let paths = find_rs_files("/Users/tor/prog/rust/relay/");
    for path in paths {
        process_rust_file(path.as_path());
    }
}
