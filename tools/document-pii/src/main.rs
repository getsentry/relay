#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::fs;
use syn::{Attribute, Field, ItemStruct, Lit, Meta, MetaNameValue};

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

fn extract_pii_fields(source: &str) -> syn::Result<Vec<String>> {
    let ast = syn::parse_file(source)?;
    let mut pii_fields = Vec::new();

    for item in ast.items {
        match item {
            syn::Item::Struct(ItemStruct { ident, fields, .. }) => {
                for field in fields {
                    if has_pii_true(&field) {
                        pii_fields.push(format!("{}.{}", ident, field.ident.unwrap()));
                    }
                }
            }
            syn::Item::Enum(_) => {}
            _ => {}
        }
    }

    Ok(pii_fields)
}

fn print_error(error: &anyhow::Error) {
    eprintln!("Error: {error}");

    let mut cause = error.source();
    while let Some(ref e) = cause {
        eprintln!("  caused by: {e}");
        cause = e.source();
    }
}

use walkdir::WalkDir;

fn find_rs_files(dir: &str) -> Vec<std::path::PathBuf> {
    let walker = WalkDir::new(dir).into_iter();
    let mut rs_files = Vec::new();

    for entry in walker.filter_map(Result::ok) {
        if entry.file_type().is_file() && entry.path().extension().unwrap_or_default() == "rs" {
            rs_files.push(entry.into_path());
        }
    }

    rs_files
}

fn main() {
    let paths = find_rs_files("/Users/tor/prog/rust/relay/");
    let mut myvec = vec![];
    for path in paths {
        let file_content = fs::read_to_string(path).unwrap();

        myvec.extend(extract_pii_fields(&file_content).unwrap());
    }
    for x in myvec {
        dbg!(x);
    }
}
