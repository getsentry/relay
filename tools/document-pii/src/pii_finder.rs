//! This module takes the types and paths from 'item_collector' module, and will recursively find
//! all the fields with the specified PII-value, and save those fields with the full path.
//!
//! E.g. `MyStruct.sub_struct.mystery_field.bitcoin_wallet_key`, meaning you can find the full path
//! from the top-level type all the way down to wherever the field with the correct PII-value resides.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::anyhow;
use proc_macro2::TokenTree;
use syn::visit::Visit;
use syn::{Attribute, Field, ItemEnum, ItemStruct, Meta, Path, Type, TypePath};

use crate::EnumOrStruct;

/// The name of a field along with its type. Used for the path to a PII-field.
#[derive(Ord, PartialOrd, PartialEq, Eq, Hash, Clone, Debug, Default)]
pub struct TypeAndField {
    // Full path of a type. E.g. relay_common::protocol::Event, rather than just 'Event'.
    pub qualified_type_name: String,
    pub field_ident: String,
}

#[derive(Ord, PartialOrd, PartialEq, Eq, Hash, Clone, Debug, Default)]
pub struct FieldsWithAttribute {
    pub type_and_fields: Vec<TypeAndField>,
    pub attributes: BTreeMap<String, Option<String>>,
}

impl FieldsWithAttribute {
    pub fn has_attribute(&self, key: &str, expected_values: Option<&Vec<String>>) -> bool {
        let actual_value = match self.attributes.get(key) {
            Some(value) => value,
            None => return false,
        };

        match (expected_values, actual_value) {
            (None, None) => true,
            (Some(expected_values), Some(actual_value)) => expected_values
                .iter()
                .any(|expected_value| expected_value == actual_value),
            (_, _) => false,
        }
    }
}

fn get_type_paths_from_type(ty: &Type, type_paths: &mut Vec<TypePath>) {
    match ty {
        Type::Path(path) => type_paths.push(path.clone()),
        Type::Reference(reference) => get_type_paths_from_type(&reference.elem, type_paths),
        Type::Array(arr) => get_type_paths_from_type(&arr.elem, type_paths),
        Type::BareFn(bare_fn) => bare_fn
            .inputs
            .iter()
            .for_each(|ty| get_type_paths_from_type(&ty.ty, type_paths)),
        Type::Group(group) => get_type_paths_from_type(&group.elem, type_paths),
        Type::Paren(paren) => get_type_paths_from_type(&paren.elem, type_paths),
        Type::Ptr(ptr) => get_type_paths_from_type(&ptr.elem, type_paths),
        Type::Slice(slice) => get_type_paths_from_type(&slice.elem, type_paths),
        Type::Tuple(tuple) => tuple
            .elems
            .iter()
            .for_each(|ty| get_type_paths_from_type(ty, type_paths)),
        Type::Verbatim(_)
        | Type::TraitObject(_)
        | Type::ImplTrait(_)
        | Type::Infer(_)
        | Type::Macro(_)
        | Type::Never(_) => {}
        _ => {}
    }
}

/// This is the visitor that actually generates the pii_types, it has a lot of associated data
/// because it using the Visit trait from syn-crate means I cannot add data as arguments.
/// The 'pii_types' field can be regarded as the output.
pub struct PiiFinder<'a> {
    /// Module path of a type is the full path up to the type itself.
    ///
    /// Example: `relay_event_schema::protocol::Event` -> `relay_event_schema::protocol`
    pub module_path: String,
    pub current_type: String,
    pub all_types: &'a HashMap<String, EnumOrStruct>,
    // The full paths of rust types either defined in the module or brought in to scope with a use-statement.
    pub scoped_paths: &'a BTreeMap<String, BTreeSet<String>>,
    pub current_path: Vec<TypeAndField>,
    pub pii_types: BTreeSet<FieldsWithAttribute>, // output
}

impl<'a> PiiFinder<'a> {
    pub fn new(
        path: &str,
        all_types: &'a HashMap<String, EnumOrStruct>,
        scoped_paths: &'a BTreeMap<String, BTreeSet<String>>,
    ) -> anyhow::Result<Self> {
        let module_path = path
            .rsplit_once("::")
            .ok_or_else(|| anyhow!("invalid module path: {}", path))?
            .0
            .to_owned();

        Ok(Self {
            module_path,
            current_type: String::new(),
            all_types,
            scoped_paths,
            current_path: vec![],
            pii_types: BTreeSet::new(),
        })
    }

    fn visit_type_path(&mut self, path: &TypePath) {
        let scoped_paths = self.scoped_paths.get(&self.module_path).unwrap().clone();

        let mut field_types = BTreeSet::new();
        get_field_types(&path.path, &mut field_types);

        let use_paths = get_matching_scoped_paths(&field_types, &scoped_paths);
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

    fn visit_field_types(&mut self, ty: &Type) {
        let mut type_paths = vec![];
        get_type_paths_from_type(ty, &mut type_paths);

        for path in type_paths {
            self.visit_type_path(&path);
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

        let mut all_attributes = BTreeMap::new();
        for attr in &node.attrs {
            if let Some(mut attributes) = get_attributes(attr, "metastructure") {
                all_attributes.append(&mut attributes);
            }
        }

        if !all_attributes.is_empty() {
            self.pii_types.insert(FieldsWithAttribute {
                type_and_fields: self.current_path.clone(),
                attributes: all_attributes,
            });
        }

        // Recursively diving into the types of the field to look for more PII-fields.
        self.visit_field_types(&node.ty);

        self.current_path.pop();
    }
}

/// Finds the full path to the given types by comparing them to the types in the scope.
fn get_matching_scoped_paths<'a>(
    field_types: &'a BTreeSet<String>,
    scoped_paths: &'a BTreeSet<String>,
) -> Vec<&'a String> {
    scoped_paths
        .iter()
        .filter(|use_path| {
            let last_use_path = use_path.split("::").last().unwrap().trim();
            field_types
                .iter()
                .any(|field_type| field_type.trim() == last_use_path)
        })
        .collect()
}

/// This function extracts the type names from a complex type and stores them in a BTreeSet.
/// It's designed to handle nested generic types, such as `Foo<Bar<Baz>>`, and return ["Foo", "Bar", "Baz"].
fn get_field_types(path: &Path, segments: &mut BTreeSet<String>) {
    // Iterating over path segments allows us to handle complex, possibly nested types
    let mut path_iter = path.segments.iter();
    if let Some(first_segment) = path_iter.next() {
        let mut ident = first_segment.ident.to_string();

        // Recursion on AngleBracketed args is necessary for nested generic types
        if let syn::PathArguments::AngleBracketed(angle_bracketed) = &first_segment.arguments {
            for generic_arg in angle_bracketed.args.iter() {
                if let syn::GenericArgument::Type(Type::Path(path)) = generic_arg {
                    get_field_types(&path.path, segments);
                }
            }
        }

        // Namespace resolution: if a second segment exists, it's part of the first type's namespace
        if let Some(second_segment) = path_iter.next() {
            ident.push_str("::");
            ident.push_str(&second_segment.ident.to_string());
        }
        segments.insert(ident);
    }
}

/// Collects all the attributes from a given field.
fn get_attributes(attr: &Attribute, ident: &str) -> Option<BTreeMap<String, Option<String>>> {
    let meta_list = match &attr.meta {
        Meta::List(meta_list) => meta_list,
        _ => return None,
    };

    // Checks name of attribute, E.g. 'metastructure'
    if !meta_list.path.is_ident(ident) {
        return None;
    }

    let mut attributes = BTreeMap::<String, Option<String>>::new();

    let mut ident = String::new();
    let mut literal = None;
    for token in meta_list.tokens.clone().into_iter() {
        match token {
            TokenTree::Ident(new_ident) => {
                if !ident.is_empty() {
                    attributes.insert(ident.clone(), literal.clone());
                }
                ident = new_ident.to_string();
                literal = None;
            }
            TokenTree::Literal(lit) => {
                let mut as_string = lit.to_string();

                // remove quotes
                as_string.remove(0);
                as_string.pop();

                literal = Some(as_string);
            }
            TokenTree::Group(_) | TokenTree::Punct(_) => {}
        }
    }

    if !ident.is_empty() {
        attributes.insert(ident, literal);
    }

    Some(attributes)
}
