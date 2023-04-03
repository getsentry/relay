#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

/*

I want to print out all the fields and enum variants with pii = true in my rust project, with the full path.

the full path means that, for example, if struct A has a field of struct B, and struct B has a field
named "foo_field" of type struct C, and this field is pii = true, it should print the following:

A.B.foo_field

notice it didn't print out A.B.C, the last component should be the field name, while the previous
components should the name of types.

the main plan to do this is as following:

- traverse all the types defined in the rust files
- keep track of the `current` full path
- first the full path is just the name of whatever type is iterated over
- for each type, iterate over all the fields. if a field is pii=true, then print out path+field_name
- for each field, regardless of if it's pii=true or not, start iterating through its fields in the
same manner, so that it will be a recursive process.
- when it comes to iterating recursively, keep the following things in mind:
    - make a new path variable, which is the current path + the name of type you go into, such as: A.B
    - don't iterate through the type if the type is already in the current path, as to avoid
    self referencing infinite loops.
    - if the type has generic instansiations, such as type Foo instiantiated with Bar, like Foo<Bar>,
    then it should sequentially recurse into both Foo and Bar. reminder if youre on struct A, it should
    first go to A.Foo, and then A.Bar, NOT A.Foo.Bar. there should be a function that returns a
    vector of all types of a field_name as such: vec![Foo, Bar].


if any of this is not clear, let me know.



 */

use std::collections::{HashMap, HashSet};
use std::io::{BufReader, Read};
use std::path::PathBuf;

use syn::{
    Data, DataStruct, DeriveInput, Field, Fields, File, Item, ItemMod, TypeParamBound, TypePath,
    Visibility,
};

use syn::{parse_quote, Type};

use toml::value;
use walkdir::WalkDir;

fn find_rs_files_and_crate_names(dir: &str) -> Vec<(PathBuf, String)> {
    let mut result = Vec::new();
    let walker = WalkDir::new(dir).into_iter();

    for entry in walker
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
    {
        if entry.path().file_name().unwrap_or_default() == "Cargo.toml" {
            let crate_name = read_crate_name_from_cargo_toml(entry.path());
            if let Some(crate_name) = crate_name {
                let parent_dir = entry.path().parent().unwrap();
                for rs_entry in WalkDir::new(parent_dir).into_iter().filter_map(Result::ok) {
                    if rs_entry.file_type().is_file()
                        && rs_entry.path().extension().unwrap_or_default() == "rs"
                    {
                        result.push((rs_entry.into_path(), crate_name.clone()));
                    }
                }
            }
        }
    }

    result
}

fn read_crate_name_from_cargo_toml(path: &std::path::Path) -> Option<String> {
    let mut file = std::fs::File::open(path).ok()?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).ok()?;
    let value: toml::Value = toml::from_str(&contents).ok()?;
    let table = value.as_table()?;
    let package = table.get("package")?.as_table()?;
    let name = package.get("name")?.as_str()?;
    Some(name.to_owned())
}

fn main() {
    let paths = find_rs_files_and_crate_names("/Users/tor/prog/rust/relay/");
    for (path, crate_name) in paths {
        let file_content = std::fs::read_to_string(path).unwrap();

        let ast = syn::parse_file(&file_content).unwrap();
        let items = ast.items;

        let mut module_path = vec![];
        let mut visited = HashMap::new();
        traverse_items(&items, &crate_name, &mut module_path, &mut visited);
    }
}

fn create_type_map(file: &File, crate_name: &str) -> HashMap<String, Item> {
    let mut type_map = HashMap::new();
    traverse_items(&file.items, crate_name, &mut Vec::new(), &mut type_map);
    type_map
}

fn traverse_items(
    items: &[Item],
    crate_name: &str,
    module_path: &mut Vec<String>,
    type_map: &mut HashMap<String, Item>,
) {
    for item in items {
        match item {
            Item::Struct(item_struct) => {
                if let Visibility::Public(_) = item_struct.vis {
                    let type_name = item_struct.ident.to_string();
                    let full_path =
                        format!("{}::{}::{}", crate_name, module_path.join("::"), type_name);
                    type_map.insert(full_path, item.clone());
                }
            }
            Item::Enum(item_enum) => {
                if let Visibility::Public(_) = item_enum.vis {
                    let type_name = item_enum.ident.to_string();
                    let full_path =
                        format!("{}::{}::{}", crate_name, module_path.join("::"), type_name);
                    type_map.insert(full_path, item.clone());
                }
            }
            Item::Mod(item_mod) => {
                if let ItemMod {
                    content: Some((_, mod_items)),
                    ident,
                    ..
                } = item_mod
                {
                    module_path.push(ident.to_string());
                    traverse_items(mod_items, crate_name, module_path, type_map);
                    module_path.pop();
                }
            }
            // Add other public item types as needed, e.g., ItemType (type aliases)
            _ => {}
        }
    }
}

fn traverse_struct(path: &str, fields: &Fields, visited: &mut HashSet<String>) {
    for field in fields {
        // If the field is pii=true, print the current path + field_name
        // ...

        // Recurse into the field's type
        let new_path = format!("{}.{}", path, field.ident.unwrap());
        traverse_type(&new_path, &field.ty, visited);
    }
}

fn traverse_enum(
    path: &str,
    variants: &syn::punctuated::Punctuated<syn::Variant, syn::token::Comma>,
    visited: &mut HashSet<String>,
) {
    for variant in variants {
        traverse_struct(path, &variant.fields, visited);
    }
}

fn traverse_type(path: &str, ty: &syn::Type, visited: &mut HashSet<String>) {
    match ty {
        Type::Path(TypePath { path, .. }) => {
            let ident = &path.segments.last().unwrap().ident;

            if visited.contains(&ident.to_string()) {
                return;
            }
            visited.insert(ident.to_string());

            let new_path = format!("{}.{}", path, ident);
            // Recurse into the type by finding the corresponding syn::Item and calling traverse_items()
            // ...

            visited.remove(&ident.to_string());
        } // Other match arms for different node types
          // ...
    }
}

fn find_pii_fields(path: &str, node: &syn::Type, visited: &mut HashSet<String>) {
    match node {
        Type::Path(TypePath { path, .. }) => {
            let ident = &path.segments.last().unwrap().ident;

            if visited.contains(&ident.to_string()) {
                return;
            }
            visited.insert(ident.to_string());

            // Get the struct or enum definition corresponding to `ident` and iterate through its fields
            // ...

            // For each field, if it has `pii = true`, print the current path + field_name
            // ...

            // Recurse into the field's type
            let new_path = format!("{}.{}", path, ident);
            find_pii_fields(&new_path, &field.ty, visited);

            // Process generic arguments
            if let Some(angles) = path.segments.last().unwrap().arguments {
                process_generic_arguments(&new_path, &angles, visited);
            }

            visited.remove(&ident.to_string());
        }
        Type::Paren(syn::TypeParen { elem, .. }) => {
            find_pii_fields(path, &*elem, visited);
        }
        Type::Group(syn::TypeGroup { elem, .. }) => {
            find_pii_fields(path, &*elem, visited);
        }
        Type::Tuple(syn::TypeTuple { elems, .. }) => {
            for elem in elems {
                find_pii_fields(path, elem, visited);
            }
        }
        Type::TraitObject(syn::TypeTraitObject { bounds, .. }) => {
            for bound in bounds {
                if let TypeParamBound::Trait(syn::TraitBound { path, .. }) = bound {
                    let ident = &path.segments.last().unwrap().ident;
                    let new_path = format!("{}.{}", path, ident);
                    // Recurse into the trait's associated types (if any)
                    // ...
                }
            }
        }
        Type::ImplTrait(syn::TypeImplTrait { bounds, .. }) => {
            for bound in bounds {
                if let TypeParamBound::Trait(syn::TraitBound { path, .. }) = bound {
                    let ident = &path.segments.last().unwrap().ident;
                    let new_path = format!("{}.{}", path, ident);
                    // Recurse into the trait's associated types (if any)
                    // ...
                }
            }
        }
        Type::Reference(syn::TypeReference { elem, .. }) => {
            find_pii_fields(path, &*elem, visited);
        }
        Type::Ptr(syn::TypePtr { elem, .. }) => {
            find_pii_fields(path, &*elem, visited);
        } // Other node types can be added here if needed
    }
}

fn process_generic_arguments(path: &str, args: &syn::PathArguments, visited: &mut HashSet<String>) {
    if let syn::PathArguments::AngleBracketed(ref bracketed) = args {
        for arg in &bracketed.args {
            if let syn::GenericArgument::Type(ref ty) = arg {
                find_pii_fields(path, ty, visited)
            }
            // You might also want to handle other generic argument types, like Lifetime, Const, etc.
            // For example:
            // if let syn::GenericArgument::Lifetime(ref lifetime) = arg {
            //     // Process lifetimes if necessary
            // }
            // if let syn::GenericArgument::Const(ref constant) = arg {
            //     // Process const generics if necessary
            // }
        }
    }
}
