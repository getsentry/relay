use proc_macro2::TokenStream;
use quote::quote;

use syn::{Lit, LitBool, Meta};

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
        let mut whitelist = quote!();
        let mut blacklist = quote!();

        for bi in variant.bindings() {
            if let Some(val) = parse_attributes(bi.ast()) {
                let field_name: String = bi
                    .ast()
                    .ident
                    .as_ref()
                    .expect("Cannot define should_strip_pii on unnamed fields")
                    .to_string();

                if val {
                    whitelist = quote!(#field_name, #whitelist);
                } else {
                    blacklist = quote!(#field_name, #blacklist);
                }
            }
        }

        quote! {
            crate::pii::PiiAttrsMap {
                whitelist: &[#whitelist],
                blacklist: &[#blacklist],
            }
        }
    });

    s.gen_impl(quote! {
        #[automatically_derived]
        gen impl crate::pii::PiiStrippable for @Self {
            fn get_attrs(&self) -> crate::pii::PiiAttrsMap {
                match *self {
                    #arms
                }
            }
        }
    })
}
