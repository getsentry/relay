use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::anyhow;
use proc_macro2::TokenTree;
use syn::visit::Visit;
use syn::{Attribute, Field, ItemEnum, ItemStruct, Meta, Type};

use crate::EnumOrStruct;

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

/// Finds the full path to the given types by comparing them to the types in the scope.
fn get_matching_use_paths<'a>(
    field_types: &'a BTreeSet<String>,
    local_paths: &'a BTreeSet<String>,
) -> Vec<&'a String> {
    local_paths
        .iter()
        .filter(|use_path| {
            let last_use_path = use_path.split("::").last().unwrap().trim();
            field_types
                .iter()
                .any(|field_type| field_type.trim() == last_use_path)
        })
        .collect()
}

impl<'a> PiiFinder<'a> {
    /// Takes a Field and visit the types that it consist of if we have
    /// the full path to it in self.use_statements.
    fn visit_field_types(&mut self, node: &Field) {
        let local_paths = self.use_statements.get(&self.module_path).unwrap().clone();

        let mut field_types = BTreeSet::new();
        get_field_types(&node.ty, &mut field_types);

        let use_paths = get_matching_use_paths(&field_types, &local_paths);
        for use_path in use_paths {
            if let Some(enum_or_struct) = self.all_types.get(use_path).cloned() {
                // Theses values will be changed when recursing, so we save them here so when we
                // return to this function after the match statement, we can set them back.
                let current_type = self.current_type.clone();
                let module_path = self.module_path.clone();
                self.module_path = use_path.rsplit_once("::").unwrap().0.to_owned();

                match enum_or_struct {
                    EnumOrStruct::Struct(itemstruct) => self.visit_item_struct(&itemstruct),
                    EnumOrStruct::Enum(itemenum) => self.visit_item_enum(&itemenum),
                }

                self.module_path = module_path;
                self.current_type = current_type;
            }
        }
    }

    /// Checks if the type we are on has already been visited, this is to avoid infinite recursion.
    fn is_current_type_already_visited(&self) -> bool {
        self.current_path
            .iter()
            .any(|ty| ty.qualified_type_name == self.current_type)
    }
}

impl<'ast> Visit<'ast> for PiiFinder<'_> {
    fn visit_item_struct(&mut self, node: &'ast ItemStruct) {
        self.current_type = node.ident.to_string();
        if !self.is_current_type_already_visited() {
            for field in node.fields.iter() {
                self.visit_field(field);
            }
        }
    }

    fn visit_item_enum(&mut self, node: &'ast ItemEnum) {
        self.current_type = node.ident.to_string();
        if !self.is_current_type_already_visited() {
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

        // Here we make like a 'snapshot' of the current path whenever we encounter a field with the
        // correct PII-value.
        if has_pii_value(self.pii_values, node) {
            self.pii_types.insert(self.current_path.clone());
        }

        // Recursively diving into the types of the field to look for more PII-fields.
        self.visit_field_types(node);

        self.current_path.pop();
    }
}

/// if you have a field such as `foo: Foo<Bar<Baz>>` this function can take the type of the field
/// and return a vector of the types like: ["Foo", "Bar", "Baz"].
fn get_field_types(ty: &Type, segments: &mut BTreeSet<String>) {
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
                    segments.insert(ident);
                } else {
                    segments.insert(ident);
                }
            }
        }
        _ => {
            use quote::ToTokens;
            let mut tokens = proc_macro2::TokenStream::new();
            ty.to_tokens(&mut tokens);
            segments.insert(tokens.to_string());
        }
    }
}

/// Checks if an attribute is equal to a specified name and value.
fn has_attr_value(attr: &Attribute, ident: &str, name: &str, value: &str) -> bool {
    let meta_list = match &attr.meta {
        Meta::List(meta_list) => meta_list,
        _ => return false,
    };

    // Checks name of attribute, E.g. 'metastructure'
    if !meta_list.path.is_ident(ident) {
        return false;
    }

    // Looks for the pattern '{name} = "{value}"'

    let mut iter = meta_list.tokens.clone().into_iter();

    if !iter.any(|token| matches!(token, TokenTree::Ident(ident) if ident == name)) {
        return false;
    };

    if !iter.any(|token| matches!(token, TokenTree::Punct(punct) if punct.as_char() == '=')) {
        return false;
    };

    iter.any(
        |token| matches!(token, TokenTree::Literal(lit_val) if lit_val.to_string() == format!("\"{value}\"")),
    )
}

/// Checks if a field has the metastructure "pii" and if it does, if it is equal to any
/// of the values that the user defines.
fn has_pii_value(pii_values: &[String], field: &Field) -> bool {
    field.attrs.iter().any(|attribute| {
        pii_values
            .iter()
            .any(|pii_value| has_attr_value(attribute, "metastructure", "pii", pii_value))
    })
}

/// Finds all the pii fields recursively of a given type.
pub fn find_pii_fields_of_type(
    path: &str,
    all_types: &HashMap<String, EnumOrStruct>,
    use_statements: &BTreeMap<String, BTreeSet<String>>,
    pii_values: &Vec<String>,
) -> anyhow::Result<BTreeSet<Vec<TypeAndField>>> {
    let value = all_types
        .get(path)
        .ok_or_else(|| anyhow!("Unable to find item with following path: {}", path))?;

    // This is where we collect all the pii-fields.
    let mut pii_types: BTreeSet<Vec<TypeAndField>> = BTreeSet::new();

    // When we recursively dive into the fields of an item, we have to keep track of where we are
    // at a given time.
    let mut current_path: Vec<TypeAndField> = vec![];

    // Module path of a type is the full path up to the type itself.
    // E.g. relay_general::protocol::Event -> relay_general::protocol
    let module_path = path
        .rsplit_once("::")
        .ok_or_else(|| anyhow!("invalid module path: {}", path))?
        .0
        .to_owned();

    // Keeps track of all states during recursion, and implements the 'visitor'-trait responsible
    // for recursion.
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
    Ok(pii_types)
}

/// Finds all the pii fields recursively of all the types in the rust crate/workspace.
pub fn find_pii_fields_of_all_types(
    all_types: &HashMap<String, EnumOrStruct>,
    use_statements: &BTreeMap<String, BTreeSet<String>>,
    pii_values: &Vec<String>,
) -> anyhow::Result<BTreeSet<Vec<TypeAndField>>> {
    let mut pii_types = BTreeSet::new();

    for path in all_types.keys() {
        pii_types.extend(find_pii_fields_of_type(
            path,
            all_types,
            use_statements,
            pii_values,
        )?);
    }

    Ok(pii_types)
}
