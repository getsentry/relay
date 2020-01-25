use proc_macro2::TokenStream;
use quote::quote;

use syn::{Lit, LitBool, Meta};

use crate::{is_newtype, parse_field_name_from_field_attributes};

fn parse_attributes(bi_ast: &syn::Field) -> Option<bool> {
    for attr in &bi_ast.attrs {
        let meta = match attr.interpret_meta() {
            Some(meta) => meta,
            None => continue,
        };

        if meta.name() != "should_strip_pii" {
            continue;
        }

        let name_value = match meta {
            Meta::NameValue(x) => x,
            _ => panic!("Invalid usage of should_strip_pii, need NameValue"),
        };

        match name_value.lit {
            Lit::Bool(LitBool { value, .. }) => return Some(value),
            _ => panic!("Invalid usage of should_strip_pii, need bool as value"),
        };
    }

    None
}

pub fn derive_pii(mut s: synstructure::Structure<'_>) -> TokenStream {
    s.add_bounds(synstructure::AddBounds::None);

    let arms = s.each_variant(|variant| {
        if is_newtype(variant) {
            let inner_ident = &variant.bindings()[0].binding;
            return quote!(crate::processor::Attributes::get_attrs(#inner_ident));
        }

        let mut whitelist = quote!();
        let mut blacklist = quote!();

        for (i, bi) in variant.bindings().iter().enumerate() {
            if let Some(val) = parse_attributes(bi.ast()) {
                let field_name = parse_field_name_from_field_attributes(bi.ast(), i).1;

                if val {
                    whitelist = quote!(#field_name, #whitelist);
                } else {
                    blacklist = quote!(#field_name, #blacklist);
                }
            }
        }

        quote! {
            crate::pii::PiiAttrs {
                whitelist: &[#whitelist],
                blacklist: &[#blacklist],
            }
        }
    });

    s.gen_impl(quote! {
        #[automatically_derived]
        gen impl crate::processor::Attributes<crate::pii::PiiAttrs> for @Self {
            fn get_attrs(&self) -> crate::pii::PiiAttrs {
                match *self {
                    #arms
                }
            }
        }
    })
}
