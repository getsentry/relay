use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::EnumOrStruct;

use anyhow::anyhow;
use syn::visit::Visit;
use syn::{Attribute, Field, ItemEnum, ItemStruct, Meta, Type};

/// The name of a field along with its type. Used for the path to a pii-field.
#[derive(Ord, PartialOrd, PartialEq, Eq, Hash, Clone, Debug, Default)]
pub struct TypeAndField {
    // Full path of a type. E.g. relay_common::protocol::Event, rather than just 'Event'.
    pub qualified_type_name: String,
    pub field_ident: String,
}

/// This is the visitor that actually generates the pii_types, it has a lot of associated data
/// because it using the Visit trait from syn-crate means I cannot add data as arguments.
/// The 'pii_types' field can be regarded as the output.
pub struct PiiFinder<'a> {
    pub module_path: String,
    pub current_type: String,
    pub all_types: &'a HashMap<String, EnumOrStruct>,
    pub use_statements: &'a BTreeMap<String, BTreeSet<String>>,
    pub pii_values: &'a Vec<String>,
    pub current_path: &'a mut Vec<TypeAndField>,
    pub pii_types: &'a mut BTreeSet<Vec<TypeAndField>>, // output
}

impl<'a> PiiFinder<'a> {
    /// Takes a Field and visit the types that it consist of if we have
    /// the full path to it in self.use_statements.
    fn visit_field_types(&mut self, node: &Field) {
        let local_paths = self.use_statements.get(&self.module_path).unwrap().clone();

        let mut field_types = vec![];
        get_field_types(&node.ty, &mut field_types);

        for field_type in &field_types {
            for use_path in &local_paths {
                if use_path.split("::").last().unwrap() == field_type.trim() {
                    let enum_or_struct = { self.all_types.get(use_path).cloned() };

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
    }
}

impl<'ast> Visit<'ast> for PiiFinder<'_> {
    fn visit_item_struct(&mut self, node: &'ast ItemStruct) {
        self.current_type = node.ident.to_string();

        if !self // This should stop any infinite recursion
            .current_path
            .iter()
            .any(|x| x.qualified_type_name == self.current_type)
        {
            for field in node.fields.iter() {
                self.visit_field(field);
            }
        }
    }

    fn visit_item_enum(&mut self, node: &'ast ItemEnum) {
        self.current_type = node.ident.to_string();
        if !self
            .current_path
            .iter()
            .any(|x| x.qualified_type_name == self.current_type)
        {
            for variant in node.variants.iter() {
                for field in variant.fields.iter() {
                    self.visit_field(field);
                }
            }
        }
    }

    fn visit_field(&mut self, node: &'ast Field) {
        // Every time we visit a field, we have to append the field to the current_path, it gets
        // popped in the end of this function. This is done so that we can store the full path
        // whenever the field matches a correct PII value.
        self.current_path.push(TypeAndField {
            qualified_type_name: self.current_type.clone(),
            field_ident: node
                .clone()
                .ident
                .map(|x| x.to_string())
                .unwrap_or_else(|| "{{Unnamed}}".to_string()),
        });

        if has_pii_value(self.pii_values, node) {
            self.pii_types.insert(self.current_path.clone());
        }

        self.visit_field_types(node);

        self.current_path.pop();
    }
}

/// if you have a field such as `foo: Foo<Bar<Baz>>` this function can take the type of the field
/// and return a vector of the types like: ["Foo", "Bar", "Baz"].
fn get_field_types(ty: &Type, segments: &mut Vec<String>) {
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
                                get_field_types(ty, segments);
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

/// Checks if an attribute is equal to a specified name and value.
fn has_attr_value(attr: &Attribute, name: &str, value: &str) -> bool {
    if let Meta::List(meta_list) = &attr.meta {
        if meta_list.path.is_ident("metastructure") {
            let mut iter = meta_list.tokens.clone().into_iter();
            while let Some(token) = iter.next() {
                if let proc_macro2::TokenTree::Ident(ident) = &token {
                    if ident == name {
                        if let Some(proc_macro2::TokenTree::Punct(punct)) = iter.next() {
                            if punct.as_char() == '=' {
                                if let Some(proc_macro2::TokenTree::Literal(lit_val)) = iter.next()
                                {
                                    let mut lit_val = lit_val.to_string();
                                    lit_val.pop(); // remove superfluous quotes. "\"true\"" -> "true"
                                    lit_val.remove(0);
                                    if lit_val == value {
                                        return true;
                                    }
                                }
                            }
                        }
                    };
                }
            }
        }
    }
    false
}

/// Checks if a field has the metastructure "pii" and if it does, if it is equal to any
/// of the values that the user defines.
fn has_pii_value(pii_values: &Vec<String>, field: &Field) -> bool {
    for attr in &field.attrs {
        for pii_value in pii_values {
            if has_attr_value(attr, "pii", pii_value) {
                return true;
            }
        }
    }
    false
}

/// Finds all pii-fields of either a single type if provided, or of all the defined types in the
/// rust project.
pub fn find_pii_fields(
    item: Option<&str>,
    all_types: &HashMap<String, EnumOrStruct>,
    use_statements: &BTreeMap<String, BTreeSet<String>>,
    pii_values: &Vec<String>,
) -> anyhow::Result<BTreeSet<Vec<TypeAndField>>> {
    let mut pii_types = BTreeSet::new();
    let mut current_path = vec![];
    match item {
        Some(path) => {
            if let Some(structorenum) = { all_types.get(path).cloned() } {
                let module_path = path
                    .rsplit_once("::")
                    .ok_or_else(|| anyhow!("invalid module path: {}", path))?
                    .0
                    .to_owned();
                let theitem = structorenum;
                let mut visitor = PiiFinder {
                    module_path,
                    current_type: String::new(),
                    all_types,
                    use_statements,
                    pii_values,
                    pii_types: &mut pii_types,
                    current_path: &mut current_path,
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
            for (key, value) in all_types.iter() {
                let module_path = key
                    .rsplit_once("::")
                    .ok_or_else(|| anyhow!("invalid module path: {}", key))?
                    .0
                    .to_owned();
                let mut visitor = PiiFinder {
                    module_path,
                    current_type: String::new(),
                    all_types,
                    use_statements,
                    pii_values,
                    pii_types: &mut pii_types,
                    current_path: &mut current_path,
                };
                match value {
                    EnumOrStruct::Struct(itemstruct) => visitor.visit_item_struct(itemstruct),
                    EnumOrStruct::Enum(itemenum) => visitor.visit_item_enum(itemenum),
                };
            }
        }
    }
    Ok(pii_types)
}
