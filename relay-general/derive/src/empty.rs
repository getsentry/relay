use proc_macro2::TokenStream;
use quote::quote;

use crate::{is_newtype, parse_field_attributes};

pub fn derive_empty(mut s: synstructure::Structure<'_>) -> TokenStream {
    let _ = s.add_bounds(synstructure::AddBounds::Generics);

    let is_empty_arms = s.each_variant(|variant| {
        let mut is_tuple_struct = false;
        let mut cond = quote!(true);
        for (index, bi) in variant.bindings().iter().enumerate() {
            let field_attrs = parse_field_attributes(index, bi.ast(), &mut is_tuple_struct);
            let ident = &bi.binding;
            if field_attrs.additional_properties {
                cond = quote! {
                    #cond && #bi.values().all(crate::types::Empty::is_empty)
                };
            } else {
                cond = quote! {
                    #cond && crate::types::Empty::is_empty(#ident)
                };
            }
        }

        cond
    });

    let is_deep_empty_arms = s.each_variant(|variant| {
        if is_newtype(variant) {
            let ident = &variant.bindings()[0].binding;
            return quote! {
                crate::types::Empty::is_deep_empty(#ident)
            };
        }

        let mut cond = quote!(true);
        let mut is_tuple_struct = false;
        for (index, bi) in variant.bindings().iter().enumerate() {
            let field_attrs = parse_field_attributes(index, bi.ast(), &mut is_tuple_struct);
            let ident = &bi.binding;
            let skip_serialization_attr = field_attrs.skip_serialization.as_tokens();

            if field_attrs.additional_properties {
                cond = quote! {
                    #cond && #bi.values().all(|v| v.skip_serialization(#skip_serialization_attr))
                };
            } else {
                cond = quote! {
                    #cond && #ident.skip_serialization(#skip_serialization_attr)
                };
            }
        }

        cond
    });

    s.gen_impl(quote! {
        #[automatically_derived]
        gen impl crate::types::Empty for @Self {
            fn is_empty(&self) -> bool {
                match *self {
                    #is_empty_arms
                }
            }

            fn is_deep_empty(&self) -> bool {
                match *self {
                    #is_deep_empty_arms
                }
            }
        }
    })
}
