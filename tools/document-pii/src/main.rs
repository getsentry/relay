#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::fs;
use syn::__private::quote::format_ident;
use syn::spanned::Spanned;
use syn::{
    Attribute, Field, Fields, FieldsUnnamed, Item, ItemEnum, ItemStruct, Lit, Meta, MetaNameValue,
};

fn extract_pii_fields(source: &str) -> syn::Result<Vec<String>> {
    let ast = syn::parse_file(source)?;
    let mut pii_fields = Vec::new();

    for item in ast.items {
        match item {
            syn::Item::Struct(ItemStruct { ident, fields, .. }) => {
                for field in fields {
                    if has_pii_true(&field.attrs) {
                        if let Some(iden) = field.ident {
                            pii_fields.push(format!("struct: {}.{}", ident, iden));
                        } else {
                            pii_fields.push(format!("struct: {}.anon", ident));
                        }
                    }
                }
            }
            syn::Item::Enum(ItemEnum {
                ident, variants, ..
            }) => {
                println!("checking enum {}", ident);
                for variant in variants {
                    if has_pii_true(&variant.attrs) {
                        pii_fields.push(format!("enum: {}.{}", ident, variant.ident));
                    }
                }
            }
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

    for path in paths {
        let source_code = fs::read_to_string(path).unwrap();

        let ast = syn::parse_file(&source_code).unwrap();
        let mut paths = Vec::new();
        paths = traverse_items(&ast.items, "", paths);

        for path in paths {
            println!("{}", path);
        }
    }

    /*
    let mut myvec = vec![];
    for path in paths {
        let file_content = fs::read_to_string(path).unwrap();

        myvec.extend(extract_pii_fields(&file_content).unwrap());
    }
    for x in myvec {
        dbg!(x);
    }
    */
}

fn extract_inner_type(ty: &syn::Type) -> Option<&syn::Type> {
    match ty {
        syn::Type::Path(tp) => {
            if let Some(segment) = tp.path.segments.last() {
                if let syn::PathArguments::AngleBracketed(angle_args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner_ty)) = angle_args.args.last() {
                        return Some(inner_ty);
                    }
                }
            }
        }
        _ => (),
    }
    None
}

fn traverse_items(items: &[Item], path: &str, mut paths: Vec<String>) -> Vec<String> {
    for item in items {
        match item {
            Item::Struct(item_struct) => {
                let new_path = format!("{}@{}", path, item_struct.ident);
                let fields_as_items = fields_to_items(&item_struct.fields);

                for field in &item_struct.fields {
                    if has_pii_true(&field.attrs) {
                        if let Some(field_ident) = &field.ident {
                            paths.push(format!("{}.{}", new_path, field_ident));
                        }
                    }
                }

                paths = traverse_items(&fields_as_items, &new_path, paths);

                for field in &item_struct.fields {
                    let nested_items = get_nested_types(&field.ty);
                    paths = traverse_items(
                        &nested_items,
                        &format!("{}.{}", new_path, field.ident.as_ref().unwrap()),
                        paths,
                    );
                }
            }
            Item::Enum(item_enum) => {
                let new_path = format!("{}@{}", path, item_enum.ident);
                for variant in &item_enum.variants {
                    let variant_path = format!("{}@{}", new_path, variant.ident);
                    let fields_as_items = fields_to_items(&variant.fields);

                    for field in &variant.fields {
                        if has_pii_true(&field.attrs) {
                            if let Some(field_ident) = &field.ident {
                                paths.push(format!("{}.{}", variant_path, field_ident));
                            }
                        }
                    }

                    paths = traverse_items(&fields_as_items, &variant_path, paths);

                    for field in &variant.fields {
                        let nested_items = get_nested_types(&field.ty);
                        paths = traverse_items(
                            &nested_items,
                            &format!("{}.{}", variant_path, field.ident.as_ref().unwrap()),
                            paths,
                        );
                    }
                }
            }
            _ => (),
        }
    }
    paths
}

fn get_nested_types(ty: &syn::Type) -> Vec<Item> {
    let mut nested_items = Vec::new();
    if let Some(inner_ty) = extract_inner_type(ty) {
        let item_struct = ItemStruct {
            attrs: vec![],
            ident: format_ident!("_inner"),
            vis: syn::Visibility::Inherited,
            struct_token: syn::token::Struct {
                span: inner_ty.span(),
            },
            generics: syn::Generics::default(),
            fields: Fields::Unit,
            semi_token: None,
        };
        let inner_item = Item::Struct(item_struct);
        nested_items.push(inner_item);
        nested_items.extend(get_nested_types(inner_ty));
    }
    nested_items
}

fn fields_to_items(fields: &Fields) -> Vec<Item> {
    let mut items = Vec::new();
    match fields {
        Fields::Named(named_fields) => {
            for field in &named_fields.named {
                if let Some(ident) = &field.ident {
                    let item_struct = ItemStruct {
                        attrs: vec![],
                        ident: ident.clone(),
                        vis: syn::Visibility::Inherited,
                        struct_token: syn::token::Struct { span: field.span() },
                        generics: syn::Generics::default(),
                        fields: Fields::Unit,
                        semi_token: None,
                    };
                    items.push(Item::Struct(item_struct));
                }

                if let Some(inner_ty) = extract_inner_type(&field.ty) {
                    let item_struct = ItemStruct {
                        attrs: vec![],
                        ident: field.ident.clone().unwrap(),
                        vis: syn::Visibility::Inherited,
                        struct_token: syn::token::Struct { span: field.span() },
                        generics: syn::Generics::default(),
                        fields: Fields::Unit,
                        semi_token: None,
                    };
                    let inner_item = Item::Struct(item_struct);
                    let inner_fields = fields_to_items(&Fields::Unnamed(FieldsUnnamed {
                        paren_token: syn::token::Paren {
                            span: inner_ty.span(),
                        },
                        unnamed: syn::punctuated::Punctuated::from_iter([Field {
                            attrs: vec![],
                            vis: syn::Visibility::Inherited,
                            ident: None,
                            colon_token: None,
                            ty: (*inner_ty).clone(),
                        }]),
                    }));
                    items.push(inner_item);
                    items.extend(inner_fields);
                }
            }
        }
        Fields::Unnamed(unnamed_fields) => {
            for (index, field) in unnamed_fields.unnamed.iter().enumerate() {
                let ident = format_ident!("_{}", index);
                let item_struct = ItemStruct {
                    attrs: vec![],
                    ident,
                    vis: syn::Visibility::Inherited,
                    struct_token: syn::token::Struct { span: field.span() },
                    generics: syn::Generics::default(),
                    fields: Fields::Unit,
                    semi_token: None,
                };
                items.push(Item::Struct(item_struct));
            }
        }
        Fields::Unit => (),
    }
    items
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
                            let val = lit_str.value();
                            return Some(val == "true" || val == "maybe");
                        }
                    }
                }
            }
        }
    }
    None
}

fn has_pii_true(attrs: &Vec<Attribute>) -> bool {
    for attr in attrs {
        if let Some(value) = get_attr_value(attr, "pii") {
            return value;
        }
    }
    false
}
