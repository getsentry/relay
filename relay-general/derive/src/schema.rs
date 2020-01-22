use proc_macro2::{Span, TokenStream};
use quote::quote;

use syn::{Ident, Lit, Meta};

use crate::{is_newtype, parse_field_name_from_field_attributes};

#[derive(Default, Eq, PartialEq)]
struct Attrs {
    required: bool,
    nonempty: bool,
    trim_whitespace: bool,
    match_regex: Option<String>,
}

fn parse_attributes(bi_ast: &syn::Field) -> Attrs {
    let mut rv = Attrs::default();

    for attr in &bi_ast.attrs {
        let meta = match attr.interpret_meta() {
            Some(meta) => meta,
            None => continue,
        };

        match &meta.name().to_string()[..] {
            "required" => rv.required = true,
            "nonempty" => rv.nonempty = true,
            "trim_whitespace" => rv.trim_whitespace = true,
            "match_regex" => {
                let name_value = match meta {
                    Meta::NameValue(x) => x,
                    _ => panic!("Invalid usage of match_regex, need NameValue"),
                };

                match name_value.lit {
                    Lit::Str(litstr) => {
                        rv.match_regex = Some(litstr.value());
                    }
                    _ => panic!("Invalid usage of match_regex, need string as value"),
                }
            }
            _ => {}
        }
    }

    rv
}

pub fn derive_schema(mut s: synstructure::Structure<'_>) -> TokenStream {
    s.add_bounds(synstructure::AddBounds::None);

    let mut lazy_statics = quote!();
    let mut variant_i = 0;

    let arms = s.each_variant(|variant| {
        if is_newtype(variant) {
            let inner_ident = &variant.bindings()[0].binding;
            return quote!(crate::store::schema::SchemaValidated::get_attrs(#inner_ident));
        }

        let mut required = quote!();
        let mut nonempty = quote!();
        let mut trim_whitespace = quote!();
        let mut match_regex = quote!();
        let mut has_match_regex = false;

        for (i, bi) in variant.bindings().iter().enumerate() {
            let attrs = parse_attributes(bi.ast());
            if attrs == Default::default() {
                continue;
            }

            let path_item = parse_field_name_from_field_attributes(bi.ast(), i).1;

            if attrs.required {
                required = quote!(#path_item, #required);
            }

            if attrs.nonempty {
                nonempty = quote!(#path_item, #nonempty);
            }

            if attrs.trim_whitespace {
                trim_whitespace = quote!(#path_item, #trim_whitespace);
            }

            if let Some(regex) = attrs.match_regex {
                match_regex = quote!((#path_item, ::regex::Regex::new(#regex).unwrap()), #match_regex);
                has_match_regex = true;
            }
        }

        let regex_static_expr = if has_match_regex {
            let name = Ident::new(&format!("MATCH_REGEX_{}", variant_i), Span::call_site());
            variant_i += 1;

            lazy_statics = quote! {
                static ref #name: Vec<(crate::processor::PathItem<'static>, ::regex::Regex)> = vec![#match_regex];
                #lazy_statics
            };
            quote!(&*#name)
        } else {
            quote!(&[])
        };

        quote! {
            crate::store::schema::SchemaAttrsMap {
                required: &[#required],
                nonempty: &[#nonempty],
                trim_whitespace: &[#trim_whitespace],
                match_regex: #regex_static_expr,
            }
        }
    });

    s.gen_impl(quote! {
        #[automatically_derived]
        gen impl crate::store::schema::SchemaValidated for @Self {
            fn get_attrs(&self) -> crate::store::schema::SchemaAttrsMap {
                ::lazy_static::lazy_static! {
                    #lazy_statics
                }

                match *self {
                    #arms
                }
            }
        }
    })
}
