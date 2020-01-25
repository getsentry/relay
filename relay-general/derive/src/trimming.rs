use proc_macro2::TokenStream;
use quote::quote;

use syn::{Lit, Meta};

use crate::{is_newtype, parse_field_name_from_field_attributes};

#[derive(Default)]
struct Attrs {
    max_chars: Option<TokenStream>,
    bag_size: Option<TokenStream>,
    bag_size_inner: Option<TokenStream>,
}

fn parse_bag_size(name: &str) -> TokenStream {
    match name {
        "small" => quote!(crate::processor::BagSize::Small),
        "medium" => quote!(crate::processor::BagSize::Medium),
        "large" => quote!(crate::processor::BagSize::Large),
        "larger" => quote!(crate::processor::BagSize::Larger),
        "massive" => quote!(crate::processor::BagSize::Massive),
        _ => panic!("invalid bag_size variant '{}'", name),
    }
}

fn parse_max_chars(name: &str) -> TokenStream {
    match name {
        "logger" => quote!(crate::processor::MaxChars::Logger),
        "hash" => quote!(crate::processor::MaxChars::Hash),
        "enumlike" => quote!(crate::processor::MaxChars::EnumLike),
        "summary" => quote!(crate::processor::MaxChars::Summary),
        "message" => quote!(crate::processor::MaxChars::Message),
        "symbol" => quote!(crate::processor::MaxChars::Symbol),
        "path" => quote!(crate::processor::MaxChars::Path),
        "short_path" => quote!(crate::processor::MaxChars::ShortPath),
        "email" => quote!(crate::processor::MaxChars::Email),
        "culprit" => quote!(crate::processor::MaxChars::Culprit),
        "tag_key" => quote!(crate::processor::MaxChars::TagKey),
        "tag_value" => quote!(crate::processor::MaxChars::TagValue),
        "environment" => quote!(crate::processor::MaxChars::Environment),
        _ => panic!("invalid max_chars variant '{}'", name),
    }
}

fn parse_attributes(bi_ast: &syn::Field) -> Attrs {
    let mut rv = Attrs::default();

    for attr in &bi_ast.attrs {
        let meta = match attr.parse_meta() {
            Ok(meta) => meta,
            Err(_) => continue,
        };

        let ident = match meta.path().get_ident() {
            Some(x) => x,
            None => continue,
        };

        match ident.to_string().as_str() {
            "max_chars" | "bag_size" | "bag_size_inner" => {}
            _ => continue,
        }

        let name_value = match meta {
            Meta::NameValue(ref x) => x,
            _ => panic!("Invalid usage of derive(TrimmingAttributes), need NameValue"),
        };

        let value = match name_value.lit {
            Lit::Str(ref litstr) => litstr.value(),
            _ => panic!("Invalid usage of derive(TrimmingAttributes), need string as value"),
        };

        match ident.to_string().as_str() {
            "max_chars" => rv.max_chars = Some(parse_max_chars(&value)),
            "bag_size" => rv.bag_size = Some(parse_bag_size(&value)),
            "bag_size_inner" => rv.bag_size_inner = Some(parse_bag_size(&value)),
            _ => unreachable!(),
        }
    }

    rv
}

pub fn derive_trimming(mut s: synstructure::Structure<'_>) -> TokenStream {
    s.add_bounds(synstructure::AddBounds::None);

    let arms = s.each_variant(|variant| {
        if is_newtype(variant) {
            let inner_ident = &variant.bindings()[0].binding;
            return quote!(crate::processor::Attributes::get_attrs(#inner_ident));
        }

        let mut bag_size = quote!();
        let mut bag_size_inner = quote!();
        let mut max_chars = quote!();

        for (i, bi) in variant.bindings().iter().enumerate() {
            let attrs = parse_attributes(bi.ast());

            let path_item = parse_field_name_from_field_attributes(bi.ast(), i).1;

            if let Some(value) = attrs.max_chars {
                max_chars = quote!((#path_item, #value), #max_chars);
            }

            if let Some(value) = attrs.bag_size {
                bag_size = quote!((#path_item, #value), #bag_size);
            }

            if let Some(value) = attrs.bag_size_inner {
                bag_size_inner = quote!((#path_item, #value), #bag_size_inner);
            }
        }

        quote! {
            crate::store::trimming::TrimmingAttrs {
                bag_size: &[#bag_size],
                bag_size_inner: &[#bag_size_inner],
                max_chars: &[#max_chars],
            }
        }
    });

    s.gen_impl(quote! {
        #[automatically_derived]
        gen impl crate::processor::Attributes<crate::store::trimming::TrimmingAttrs> for @Self {
            fn get_attrs(&self) -> crate::store::trimming::TrimmingAttrs {
                match *self {
                    #arms
                }
            }
        }
    })
}
