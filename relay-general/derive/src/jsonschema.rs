use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse_quote, Attribute, Lit, Meta, MetaNameValue, Visibility};

use crate::parse_field_attributes;

/// Take an attribute set from the original struct and:
///
/// 1. Filter out all attibutes but `schemars` and `doc`.
/// 2. Replace #[doc = "foo"] with #[schemars(description = "foo")]. While schemars already uses
///    docstrings for its jsonschema description by default, it applies line-wrapping logic that
///    destroys markdown. Explicitly setting description bypasses that.
///
fn transform_attributes(attrs: &mut Vec<Attribute>) {
    let mut description = String::new();

    attrs.retain(|attr| {
        if attr.path.is_ident("doc") {
            if let Ok(Meta::NameValue(MetaNameValue {
                lit: Lit::Str(s), ..
            })) = attr.parse_meta()
            {
                if !description.is_empty() {
                    description.push('\n');
                }
                description.push_str(&s.value());
                return false;
            }
        }

        attr.path.is_ident("schemars")
    });

    if !description.is_empty() {
        attrs.push(parse_quote!(#[schemars(description = #description)]));
    }
}

pub fn derive_jsonschema(mut s: synstructure::Structure<'_>) -> TokenStream {
    let _ = s.add_bounds(synstructure::AddBounds::Generics);

    let mut arms = quote!();

    for variant in s.variants() {
        let mut fields = quote!();

        let mut is_tuple_struct = false;

        for (index, bi) in variant.bindings().iter().enumerate() {
            let field_attrs = parse_field_attributes(index, &bi.ast(), &mut is_tuple_struct);
            let name = field_attrs.field_name;

            fields = quote! {
                #fields
                #[schemars(rename = #name)]
            };

            if !field_attrs.required.unwrap_or(false) {
                fields = quote!(#fields #[schemars(default = "__schemars_null")]);
            }

            if field_attrs.additional_properties || field_attrs.omit_from_schema {
                fields = quote!(#fields #[schemars(skip)]);
            }

            let mut ast = bi.ast().clone();
            ast.vis = Visibility::Inherited;
            transform_attributes(&mut ast.attrs);
            fields = quote!(#fields #ast,);
        }

        let ident = variant.ast().ident;

        let arm = if is_tuple_struct {
            quote!( #ident( #fields ) )
        } else {
            quote!( #ident { #fields } )
        };

        arms = quote! {
            #arms
            #arm,
        };
    }

    let ident = &s.ast().ident;
    let mut attrs = s.ast().attrs.clone();
    transform_attributes(&mut attrs);

    s.gen_impl(quote! {
        // Massive hack to tell schemars that fields are nullable. Causes it to emit {"default":
        // null} even though Option<()> is not a valid instance of T.
        fn __schemars_null() -> Option<()> {
            None
        }

        #[automatically_derived]
        gen impl schemars::JsonSchema for @Self {
            fn schema_name() -> String {
                stringify!(#ident).to_owned()
            }

            fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
                #[derive(schemars::JsonSchema)]
                #[cfg_attr(feature = "jsonschema", schemars(untagged))]
                #[cfg_attr(feature = "jsonschema", schemars(deny_unknown_fields))]
                #(#attrs)*
                enum Helper {
                    #arms
                }

                Helper::json_schema(gen)
            }
        }
    })
}
