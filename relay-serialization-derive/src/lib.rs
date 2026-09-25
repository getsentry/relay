//! A macro to add deserialization bounds to a prost message.
use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{DeriveInput, LitStr, PathArguments, Type, parse_macro_input};

#[proc_macro_derive(RuntimeDescription)]
pub fn derive(s: TokenStream) -> TokenStream {
    let input = parse_macro_input!(s as DeriveInput);

    let mut nested_v = vec![];

    match input.data {
        syn::Data::Struct(data_struct) => {
            if let syn::Fields::Named(fields_named) = data_struct.fields {
                for field in fields_named.named.iter() {
                    for attr in &field.attrs {
                        if attr.path().is_ident("prost")
                            && let Err(value) = collect_tags_and_types(&mut nested_v, field, attr)
                        {
                            return value;
                        }
                    }
                }
            }
        }
        syn::Data::Enum(data_enum) => {
            for variant in data_enum.variants.iter() {
                for attr in &variant.attrs {
                    if attr.path().is_ident("prost")
                        && let Some(first_field) = variant.fields.iter().next()
                        && let Err(value) = collect_tags_and_types(&mut nested_v, first_field, attr)
                    {
                        return value;
                    }
                }
            }
        }
        syn::Data::Union(_) => {
            let span = input.span();
            return quote_spanned! {
                span => compile_error!("unions are unsupported")
            }
            .into();
        }
    }

    let qs = nested_v.into_iter().map(|(typ, tag)| {
        if let TypeKind::Field(tag) = tag {
            quote! {
                ::relay_serialization::prost::Nested::Field(#tag, < #typ as ::relay_serialization::prost::RuntimeDescription>::desc)
            }
        } else {
            quote! {
                ::relay_serialization::prost::Nested::Oneof(< #typ as ::relay_serialization::prost::RuntimeDescription>::desc)
            }
        }
    });

    let typ = &input.ident;
    quote! {
        impl ::relay_serialization::prost::RuntimeDescription for #typ {
            fn desc() -> &'static [::relay_serialization::prost::Nested] {
                &[#(#qs,)*]
            }
        }
    }
    .into()
}

enum TypeKind {
    Field(u32),
    OneOf,
}

fn collect_tags_and_types(
    tags_and_types: &mut Vec<(Type, TypeKind)>,
    field: &syn::Field,
    attr: &syn::Attribute,
) -> Result<(), TokenStream> {
    let mut tag: Option<TypeKind> = None;
    let mut message_type: Option<&Type> = None;

    let result = attr.parse_nested_meta(|meta| {
        // Fun landmine: parse_nested_meta assumes you consume all the parsed--for tags like
        // "#[foo(bar)]", bar is consumed automatically just before this callback is called,
        // but "#[foo(bar = 6)]", the "= 6" value is not, and you MUST consume that in this callback
        // or the parser will try to parse the "next" value in the stream, but the stream
        // won't have advanced correctly, leading to sadness.
        if meta.path.is_ident("tag") {
            let value = meta.value()?;
            let s: LitStr = value.parse()?;
            let tag_val = s
                .value()
                .parse::<u32>()
                .map_err(|_| meta.input.error("error parsing tag"))?;

            tag = TypeKind::Field(tag_val).into();
            return Ok(());
        }

        if meta.path.is_ident("map")
            || meta.path.is_ident("btree_map")
            || meta.path.is_ident("hash_map")
        {
            return Err(meta.input.error("map types are currently unsupported"));
        }

        if meta.path.is_ident("group") {
            return Err(meta.input.error("group types are unsupported"));
        }

        if meta.path.is_ident("message") {
            message_type = innermost_contained_type(&field.ty);
            return Ok(());
        }

        if meta.path.is_ident("oneof") {
            message_type = innermost_contained_type(&field.ty);
            let value = meta.value()?;
            let _: LitStr = value.parse()?; // Consumes the tags

            // Tag = None communicates that this is a one-of.
            tag = Some(TypeKind::OneOf);
            return Ok(());
        }

        // Things we don't care about, but are more than just a tag
        if meta.path.is_ident("bytes")
            || meta.path.is_ident("enumeration")
            || meta.path.is_ident("tags")
            || meta.path.is_ident("packed")
        {
            let value = meta.value()?;
            let _: LitStr = value.parse()?;
            return Ok(());
        }

        // By default, just skip everything else; those should be just tags (and if not, we'll get
        // an error, albeit a confusing one.)
        Ok(())
    });

    if let Err(e) = result {
        return Err(e.to_compile_error().into());
    }

    if let Some(typ) = message_type {
        let Some(tag) = tag else {
            let span = attr.span();
            return Err(quote_spanned! {
                span => compile_error!("missing tag value");
            }
            .into());
        };
        tags_and_types.push((typ.to_owned(), tag));
    }
    Ok(())
}

fn innermost_contained_type(typ: &Type) -> Option<&Type> {
    let Type::Path(p) = typ else {
        return None;
    };

    let last_part = p.path.segments.last()?;

    if let PathArguments::AngleBracketed(inner) = &last_part.arguments {
        let first = inner.args.first()?;
        match first {
            syn::GenericArgument::Type(t) => innermost_contained_type(t),
            _ => None,
        }
    } else {
        Some(typ)
    }
}
