#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::collections::{HashMap, HashSet};
use std::fs;
use std::hash::Hash;
use std::io::BufRead;
use std::sync::Mutex;
//use syn::meta::ParseNestedMeta;
use syn::parse::ParseStream;
use syn::punctuated::Punctuated;
use syn::token::Enum;
use syn::{
    Attribute, Field, Item, ItemStruct, Lit, Meta, MetaList, MetaNameValue, Type, UsePath, UseTree,
    Variant,
};

use walkdir::WalkDir;

use lazy_static::lazy_static;
use std::cell::RefCell;

// global var since i cant add extra arguments to trait methods
static mut VISITED_TYPES: Vec<String> = Vec::new();

lazy_static::lazy_static! {
    static ref PII_TYPES: Mutex<HashSet<Vec<TypeAndField>>> = Mutex::new(HashSet::new());
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
struct TypeAndField {
    type_name: String,
    field_name: String,
}

impl TypeAndField {
    fn flat_rep(vec: &Vec<Self>) -> String {
        if vec.is_empty() {
            return "".to_string();
        }
        let mut mystring = String::new();

        mystring.push_str(&vec[0].type_name);

        for x in &vec[1..] {
            mystring.push_str(&format!(".{}", x.field_name));
        }
        mystring
    }
}

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

#[derive(Debug, Clone)]
enum EnumOrStruct {
    ItemStruct(ItemStruct),
    ItemEnum(ItemEnum),
}

use std::env;
use std::path::{Path, PathBuf};

use syn::{visit::Visit, ItemEnum};

#[derive(Default)]
struct TypeVisitor {
    use_statements: Vec<syn::ItemUse>,
    module_path: String,
    current_path: Vec<String>,
    output_path: Vec<TypeAndField>,
    all_types: HashMap<String, EnumOrStruct>,
    first_iter: bool,
}

impl TypeVisitor {
    pub fn get_crate_root(&self) -> String {
        let x: Vec<&str> = self.module_path.split("relay::").collect();
        let x = if let Some(x) = x[1].split_once("::") {
            x.0
        } else {
            x[1]
        };
        x.to_owned()
    }
}

impl<'ast> Visit<'ast> for TypeVisitor {
    fn visit_item_struct(&mut self, node: &'ast ItemStruct) {
        unsafe { VISITED_TYPES.clear() }
        let struct_name = format!("{}::{}", self.module_path, node.ident);
        // dbg!(&struct_name);
        if !self.first_iter {
            for field in node.fields.iter() {
                self.visit_field(field);
            }
        } else {
            let x = get_item_path_from_full(struct_name.clone());
            let x = x.replace("src::", "");
            let x = x.replace("-", "_");
            self.all_types
                .insert(x, EnumOrStruct::ItemStruct(node.clone()));
        }

        syn::visit::visit_item_struct(self, node);
    }

    fn visit_item_enum(&mut self, node: &'ast ItemEnum) {
        unsafe { VISITED_TYPES.clear() }
        let enum_name = format!("{}::{}", self.module_path, node.ident);
        if !self.first_iter {
            for variant in node.variants.iter() {
                for field in variant.fields.iter() {
                    self.visit_field(field);
                }
            }
        } else {
            let x = get_item_path_from_full(enum_name.clone());
            let x = x.replace("src::", "");
            let x = x.replace("-", "_");
            self.all_types
                .insert(x, EnumOrStruct::ItemEnum(node.clone()));
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
        if self.first_iter {
            return;
        }
        if self.current_path.len() > 3 {
            //dbg!(&self.current_path);
            //panic!();
        }

        let is_pii = has_pii_true(node);

        // println!("  Field: {}", node.ident.as_ref().unwrap());
        //dbg!(&node.ty);
        let field_types = type_to_string(&node.ty);
        if is_pii {
            //dbg!(&field_types);
            //    dbg!(&self.module_path);
        }
        for field_type in field_types {
            let current_module = self
                .module_path
                .clone()
                .split_once("relay::")
                .unwrap()
                .1
                .to_string();
            let type_in_current_path =
                format!("{}::{}", current_module, field_type).replace("-", "_");
            if type_in_current_path.contains("Query") {
                // dbg!(&type_in_current_path);
                //  panic!();
            }

            // if the field type is defined in the same module and not with any use statements
            if let Some(y) = self.all_types.get(&type_in_current_path) {
                if !self.current_path.contains(&type_in_current_path) {
                    self.current_path.push(type_in_current_path.clone());
                    self.output_path.push(TypeAndField {
                        type_name: field_type.clone(),
                        field_name: node
                            .clone()
                            .ident
                            .map(|x| x.to_string())
                            .unwrap_or_else(|| "{{Anon}}".to_string()),
                    });

                    if is_pii {
                        PII_TYPES.lock().unwrap().insert(self.output_path.clone());
                    }
                    //dbg!(&y.ident.to_string());
                    match y {
                        EnumOrStruct::ItemStruct(itemstruct) => {
                            self.visit_item_struct(&itemstruct.clone())
                        }
                        EnumOrStruct::ItemEnum(itemenum) => self.visit_item_enum(&itemenum.clone()),
                    }
                    //dbg!(&self.current_path);
                    self.current_path.pop();
                    self.output_path.pop();
                }

                //panic!();
                return;
            }

            for use_statement in self.use_statements.clone() {
                for use_path in use_tree_to_paths(&use_statement.tree) {
                    if is_pii {
                        //            dbg!(&use_path);
                    }
                    let use_path_ident = use_path.split("::").last();
                    if let Some(use_path_ident) = use_path_ident {
                        if is_pii {
                            //dbg!(&use_path_ident, &field_type);
                        }
                        if use_path_ident.trim() == field_type.trim()
                            && unsafe { !VISITED_TYPES.contains(&use_path) }
                        {
                            let normalized_use_path = use_path
                                .replace("crate::", &format!("{}::", self.get_crate_root()));
                            let normalized_use_path = normalized_use_path.replace("-", "_");

                            if is_pii {
                                //        dbg!(&normalized_use_path);
                            }
                            if let Some(y) = self.all_types.get(&normalized_use_path) {
                                if !self.current_path.contains(&type_in_current_path) {
                                    self.current_path.push(type_in_current_path.clone());

                                    self.output_path.push(TypeAndField {
                                        type_name: field_type.clone(),
                                        field_name: node
                                            .clone()
                                            .ident
                                            .map(|x| x.to_string())
                                            .unwrap_or_else(|| "{{Anon}}".to_string()),
                                    });

                                    if is_pii {
                                        PII_TYPES.lock().unwrap().insert(self.output_path.clone());
                                    }
                                    //dbg!(&self.current_path);
                                    match y {
                                        EnumOrStruct::ItemStruct(itemstruct) => {
                                            self.visit_item_struct(&itemstruct.clone())
                                        }
                                        EnumOrStruct::ItemEnum(itemenum) => {
                                            self.visit_item_enum(&itemenum.clone())
                                        }
                                    }
                                    self.current_path.pop();
                                    self.output_path.pop();
                                }
                                //  dbg!(&use_path_ident, &field_type);
                                //  dbg!(&use_path);
                                //dbg!(&x);
                                //self.visit_item_struct(&y.clone());
                            } else {
                            }
                            //dbg!(self.all_types.get(&x));
                            unsafe {
                                VISITED_TYPES.push(field_type.clone());
                            }
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

fn type_to_string(ty: &Type) -> Vec<String> {
    let mut segments: Vec<String> = Vec::new();
    process_type(ty, &mut segments);
    segments
    //  segments.join(", ")
    //format!("/::{}", segments.join(", "))
}

fn process_type(ty: &Type, segments: &mut Vec<String>) {
    match ty {
        Type::Path(type_path) => {
            let mut path_iter = type_path.path.segments.iter();
            let first_segment = path_iter.next();

            if let Some(first_segment) = first_segment {
                let mut ident = first_segment.ident.to_string();

                let args = &first_segment.arguments;
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

fn process_rust_file(
    file_path: &Path,
    all_types: HashMap<String, EnumOrStruct>,
    first_iter: bool,
) -> Result<HashMap<String, EnumOrStruct>, Box<dyn std::error::Error>> {
    let file_content = fs::read_to_string(file_path)?;

    let syntax_tree: syn::File = syn::parse_file(&file_content)?;
    let module_path = rust_file_to_use_path(file_path).unwrap();

    let mut visitor = TypeVisitor {
        module_path: module_path.clone(),
        all_types,
        first_iter,
        ..Default::default()
    };

    visitor.visit_file(&syntax_tree);
    Ok(visitor.all_types)
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
        let y = y.replace(" ", "");
        let y = y.replace("-", "_");
        retvec.push(y);
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

pub fn get_item_path_from_full(full: String) -> String {
    full.split_once("::relay::").unwrap().1.to_owned()
}

fn main() {
    let paths = find_rs_files("/Users/tor/prog/rust/relay/");
    let mut all_types: HashMap<String, EnumOrStruct> = HashMap::new();
    for path in paths.clone() {
        all_types = process_rust_file(path.as_path(), all_types.clone(), true).unwrap();
    }
    for path in paths {
        process_rust_file(path.as_path(), all_types.clone(), false).unwrap();
    }
    //dbg!(all_types);

    let mut pii_types = PII_TYPES.lock().unwrap().clone();
    pii_types.retain(|vec| vec.len() > 1);

    dbg!(pii_types.len());

    let mut to_remove = HashSet::new();

    for vec in pii_types.iter() {
        let string_vec: Vec<String> = vec
            .iter()
            .map(|type_and_field| type_and_field.type_name.clone())
            .collect();
        for other_vec in pii_types.iter() {
            let other_string_vec: Vec<String> = other_vec
                .iter()
                .map(|type_and_field| type_and_field.type_name.clone())
                .collect();
            if other_string_vec != string_vec && is_subset(&string_vec, &other_string_vec) {
                to_remove.insert(vec.clone());
                break;
            }
        }
    }

    for vec in to_remove {
        pii_types.remove(&vec);
    }

    let piivec: HashSet<String> = pii_types.iter().map(TypeAndField::flat_rep).collect();
    let mut piivec: Vec<String> = piivec.into_iter().collect();
    piivec.sort();
    dbg!(piivec.len());

    for pii in piivec {
        println!("{pii}");
    }

    for key in all_types.keys() {
        if key.contains("Query") {
            dbg!(key);
        }
    }
}

//relay_server::actors::upstream::UpstreamRelay
//relay_server::actors::upstream::UpstreamRelayService

fn rust_file_to_use_path(file_path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let parent_dir = file_path.parent().unwrap();
    let file_stem = file_path.file_stem().unwrap().to_string_lossy().to_string();

    //let mod_rs_path = parent_dir.join("mod.rs");
    let is_module = is_file_module(file_path)?;

    let mut module_path = parent_dir
        .components()
        .map(|part| part.as_os_str().to_string_lossy().to_string())
        .filter(|part| part != "src")
        .collect::<Vec<_>>();

    if is_module {
        module_path.push(file_stem);
    }

    let mut use_path = module_path.join("::");
    use_path = use_path.split_once("rust::").unwrap().1.into();
    use_path = format!("rust::{}", use_path);

    Ok(use_path)
}

fn is_file_module(file_path: &Path) -> Result<bool, Box<dyn std::error::Error>> {
    let parent_dir = file_path.parent().unwrap();
    let file_stem = file_path.file_stem().unwrap().to_string_lossy().to_string();

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

fn get_attr_value(attr: &Attribute, name: &str) -> Option<bool> {
    if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
        if meta_list.path.is_ident("metastructure") {
            for nested_meta in meta_list.nested {
                if let syn::NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, lit, .. })) =
                    nested_meta
                {
                    if path.is_ident(name) {
                        if let Lit::Str(lit_str) = lit {
                            return Some(lit_str.value() == "true");
                        }
                    }
                }
            }
        }
    }
    None
}

fn has_pii_true(field: &Field) -> bool {
    for attr in &field.attrs {
        if let Some(value) = get_attr_value(attr, "pii") {
            return value;
        }
    }
    false
}

fn is_subset<T: PartialEq>(subset: &[T], superset: &[T]) -> bool {
    let mut superset_iter = superset.iter();
    for item in subset {
        if superset_iter.any(|x| x == item) {
            continue;
        } else {
            return false;
        }
    }
    true
}
