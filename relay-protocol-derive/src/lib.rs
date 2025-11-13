//! Derives for Relay's protocol traits.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![recursion_limit = "256"]

use std::collections::BTreeSet;
use std::str::FromStr;

use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, quote};
use syn::spanned::Spanned;
use syn::{Data, Error, Lit, LitStr};
use synstructure::decl_derive;

mod utils;

use self::utils::SynstructureExt as _;

#[derive(Debug, Clone, Copy)]
enum Trait {
    From,
    To,
}

decl_derive!([Empty, attributes(metastructure)] => derive_empty);
decl_derive!([IntoValue, attributes(metastructure)] => derive_to_value);
decl_derive!([FromValue, attributes(metastructure)] => derive_from_value);

fn derive_empty(mut s: synstructure::Structure<'_>) -> syn::Result<TokenStream> {
    let _ = s.add_bounds(synstructure::AddBounds::Generics);

    let is_empty_arms = s.try_each_variant(|variant| {
        let mut is_tuple_struct = false;
        let mut cond = quote!(true);
        for (index, bi) in variant.bindings().iter().enumerate() {
            let field_attrs = parse_field_attributes(index, bi.ast(), &mut is_tuple_struct)?;
            let ident = &bi.binding;
            if field_attrs.additional_properties {
                cond = quote! {
                    #cond && #bi.values().all(::relay_protocol::Empty::is_empty)
                };
            } else {
                cond = quote! {
                    #cond && ::relay_protocol::Empty::is_empty(#ident)
                };
            }
        }

        Ok(cond)
    })?;

    let is_deep_empty_arms = s.try_each_variant(|variant| {
        if is_newtype_variant(variant) {
            let ident = &variant.bindings()[0].binding;
            return Ok(quote! {
                ::relay_protocol::Empty::is_deep_empty(#ident)
            });
        }

        let mut cond = quote!(true);
        let mut is_tuple_struct = false;
        for (index, bi) in variant.bindings().iter().enumerate() {
            let field_attrs = parse_field_attributes(index, bi.ast(), &mut is_tuple_struct)?;
            let ident = &bi.binding;
            let skip_serialization_attr = field_attrs.skip_serialization.as_tokens();

            if field_attrs.additional_properties {
                cond = quote! {
                    #cond && #bi.values().all(|v| v.skip_serialization(#skip_serialization_attr))
                };
            } else if field_attrs.flatten {
                cond = quote! {
                    #cond && ::relay_protocol::Empty::is_deep_empty(#ident)
                };
            } else {
                cond = quote! {
                    #cond && #ident.skip_serialization(#skip_serialization_attr)
                };
            }
        }

        Ok(cond)
    })?;

    Ok(s.gen_impl(quote! {
        #[automatically_derived]
        gen impl ::relay_protocol::Empty for @Self {
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
    }))
}

fn derive_to_value(s: synstructure::Structure<'_>) -> syn::Result<TokenStream> {
    derive_metastructure(s, Trait::To)
}

fn derive_from_value(s: synstructure::Structure<'_>) -> syn::Result<TokenStream> {
    derive_metastructure(s, Trait::From)
}

fn derive_newtype_metastructure(
    s: &synstructure::Structure<'_>,
    t: Trait,
) -> syn::Result<TokenStream> {
    let type_attrs = parse_type_attributes(s)?;
    if type_attrs.tag_key.is_some() {
        panic!("tag_key not supported on structs");
    }

    let field_attrs = parse_field_attributes(0, s.variants()[0].bindings()[0].ast(), &mut true)?;
    let skip_serialization_attr = field_attrs.skip_serialization.as_tokens();

    let name = &s.ast().ident;

    Ok(match t {
        Trait::From => s.gen_impl(quote! {
            #[automatically_derived]
            gen impl ::relay_protocol::FromValue for @Self {
                fn from_value(
                    __value: ::relay_protocol::Annotated<::relay_protocol::Value>,
                ) -> ::relay_protocol::Annotated<Self> {
                    match ::relay_protocol::FromValue::from_value(__value) {
                        Annotated(Some(__value), __meta) => Annotated(Some(#name(__value)), __meta),
                        Annotated(None, __meta) => Annotated(None, __meta),
                    }
                }
            }
        }),
        Trait::To => s.gen_impl(quote! {
            #[automatically_derived]
            gen impl ::relay_protocol::IntoValue for @Self {
                fn into_value(self) -> ::relay_protocol::Value {
                    ::relay_protocol::IntoValue::into_value(self.0)
                }

                fn serialize_payload<S>(&self, __serializer: S, __behavior: ::relay_protocol::SkipSerialization) -> Result<S::Ok, S::Error>
                where
                    Self: Sized,
                    S: ::serde::ser::Serializer
                {
                    ::relay_protocol::IntoValue::serialize_payload(&self.0, __serializer, #skip_serialization_attr)
                }

                fn extract_child_meta(&self) -> ::relay_protocol::MetaMap
                where
                    Self: Sized,
                {
                    ::relay_protocol::IntoValue::extract_child_meta(&self.0)
                }
            }
        }),
    })
}

fn derive_enum_metastructure(
    s: &synstructure::Structure<'_>,
    t: Trait,
) -> syn::Result<TokenStream> {
    let type_attrs = parse_type_attributes(s)?;

    let type_name = &s.ast().ident;
    let tag_key_str = LitStr::new(
        &type_attrs.tag_key.unwrap_or_else(|| "type".to_owned()),
        Span::call_site(),
    );

    let mut from_value_body = TokenStream::new();
    let mut to_value_body = TokenStream::new();
    let mut serialize_body = TokenStream::new();
    let mut extract_child_meta_body = TokenStream::new();

    for variant in s.variants() {
        let variant_attrs = parse_variant_attributes(variant.ast().attrs)?;
        let variant_name = &variant.ast().ident;
        let tag = variant_attrs
            .tag_override
            .unwrap_or_else(|| variant_name.to_string().to_lowercase());

        if !variant_attrs.fallback_variant {
            let tag = LitStr::new(&tag, Span::call_site());
            (quote! {
                Some(#tag) => {
                    ::relay_protocol::FromValue::from_value(::relay_protocol::Annotated(Some(::relay_protocol::Value::Object(__object)), __meta))
                        .map_value(#type_name::#variant_name)
                }
            }).to_tokens(&mut from_value_body);
            (quote! {
                #type_name::#variant_name(__value) => {
                    let mut __rv = ::relay_protocol::IntoValue::into_value(__value);
                    if let ::relay_protocol::Value::Object(ref mut __object) = __rv {
                        __object.insert(#tag_key_str.to_string(), Annotated::new(::relay_protocol::Value::String(#tag.to_string())));
                    }
                    __rv
                }
            }).to_tokens(&mut to_value_body);
            (quote! {
                #type_name::#variant_name(ref __value) => {
                    let mut __map_ser = ::serde::Serializer::serialize_map(__serializer, None)?;
                    ::relay_protocol::IntoValue::serialize_payload(__value, ::serde::__private228::ser::FlatMapSerializer(&mut __map_ser), __behavior)?;
                    ::serde::ser::SerializeMap::serialize_key(&mut __map_ser, #tag_key_str)?;
                    ::serde::ser::SerializeMap::serialize_value(&mut __map_ser, #tag)?;
                    ::serde::ser::SerializeMap::end(__map_ser)
                }
            }).to_tokens(&mut serialize_body);
        } else {
            (quote! {
                _ => {
                    if let Some(__type) = __type {
                        __object.insert(#tag_key_str.to_string(), __type);
                    }
                    ::relay_protocol::Annotated(Some(#type_name::#variant_name(__object)), __meta)
                }
            })
            .to_tokens(&mut from_value_body);
            (quote! {
                #type_name::#variant_name(__value) => {
                    ::relay_protocol::IntoValue::into_value(__value)
                }
            })
            .to_tokens(&mut to_value_body);
            (quote! {
                #type_name::#variant_name(ref __value) => {
                    ::relay_protocol::IntoValue::serialize_payload(__value, __serializer, __behavior)
                }
            })
            .to_tokens(&mut serialize_body);
        }

        (quote! {
            #type_name::#variant_name(ref __value) => {
                ::relay_protocol::IntoValue::extract_child_meta(__value)
            }
        })
        .to_tokens(&mut extract_child_meta_body);
    }

    Ok(match t {
        Trait::From => {
            s.gen_impl(quote! {
                #[automatically_derived]
                gen impl ::relay_protocol::FromValue for @Self {
                    fn from_value(
                        __value: ::relay_protocol::Annotated<::relay_protocol::Value>,
                    ) -> ::relay_protocol::Annotated<Self> {
                        match ::relay_protocol::Object::<::relay_protocol::Value>::from_value(__value) {
                            ::relay_protocol::Annotated(Some(mut __object), __meta) => {
                                let __type = __object.remove(#tag_key_str);
                                match __type.as_ref().and_then(|__type| __type.0.as_ref()).and_then(Value::as_str) {
                                    #from_value_body
                                }
                            }
                            ::relay_protocol::Annotated(None, __meta) => ::relay_protocol::Annotated(None, __meta)
                        }
                    }
                }
            })
        }
        Trait::To => {
            s.gen_impl(quote! {
                #[automatically_derived]
                gen impl ::relay_protocol::IntoValue for @Self {
                    fn into_value(self) -> ::relay_protocol::Value {
                        match self {
                            #to_value_body
                        }
                    }

                    fn serialize_payload<S>(&self, __serializer: S, __behavior: ::relay_protocol::SkipSerialization) -> Result<S::Ok, S::Error>
                    where
                        S: ::serde::ser::Serializer
                    {
                        match *self {
                            #serialize_body
                        }
                    }

                    fn extract_child_meta(&self) -> ::relay_protocol::MetaMap
                    where
                        Self: Sized,
                    {
                        match *self {
                            #extract_child_meta_body
                        }
                    }
                }
            })
        }
    })
}

fn derive_metastructure(mut s: synstructure::Structure<'_>, t: Trait) -> syn::Result<TokenStream> {
    if is_newtype(&s) {
        return derive_newtype_metastructure(&s, t);
    }

    if let Data::Enum(_) = s.ast().data {
        return derive_enum_metastructure(&s, t);
    }

    let _ = s.add_bounds(synstructure::AddBounds::Generics);

    let variants = s.variants();
    if variants.len() != 1 {
        panic!("Can only derive structs");
    }

    let mut variant = variants[0].clone();
    for binding in variant.bindings_mut() {
        binding.style = synstructure::BindStyle::MoveMut;
    }

    let mut from_value_body = TokenStream::new();
    let mut to_value_body = TokenStream::new();
    let mut serialize_body = TokenStream::new();
    let mut extract_child_meta_body = TokenStream::new();

    let type_attrs = parse_type_attributes(&s)?;
    if type_attrs.tag_key.is_some() {
        panic!("tag_key not supported on structs");
    }

    let mut is_tuple_struct = false;

    let mut field_names = BTreeSet::new();
    for (index, bi) in variant.bindings().iter().enumerate() {
        let field_attrs = parse_field_attributes(index, bi.ast(), &mut is_tuple_struct)?;
        if !field_attrs.additional_properties {
            field_names.insert(field_attrs.field_name.clone());
        }
    }

    for (index, bi) in variant.bindings().iter().enumerate() {
        let field_attrs = parse_field_attributes(index, bi.ast(), &mut is_tuple_struct)?;
        let field_name = field_attrs.field_name.clone();

        let skip_serialization_attr = field_attrs.skip_serialization.as_tokens();

        if field_attrs.additional_properties {
            if is_tuple_struct {
                panic!("additional_properties not allowed in tuple struct");
            }

            (quote! {
                let #bi = ::std::mem::take(__obj).into_iter().map(|(__key, __value)| (__key, ::relay_protocol::FromValue::from_value(__value))).collect();
            }).to_tokens(&mut from_value_body);

            (quote! {
                for (__key, __value) in #bi {
                    // Additional properties may not overwrite existing fields:
                    __map.entry(__key).or_insert(
                        Annotated::map_value(__value, ::relay_protocol::IntoValue::into_value)
                    );
                }
            })
            .to_tokens(&mut to_value_body);

            let field_names = field_names.iter().map(String::as_str).collect::<Vec<_>>();
            assert!(field_names.is_sorted());
            (quote! {
                let __field_names = &[ #( #field_names ),* ];
                for (__key, __value) in #bi.iter() {
                    if !__value.skip_serialization(#skip_serialization_attr) {
                        if __field_names.binary_search(&__key.as_str()).is_ok() {
                            continue;
                        }
                        ::serde::ser::SerializeMap::serialize_key(&mut __map_serializer, __key)?;
                        ::serde::ser::SerializeMap::serialize_value(&mut __map_serializer, &::relay_protocol::SerializePayload(__value, #skip_serialization_attr))?;
                    }
                }
            })
            .to_tokens(&mut serialize_body);

            (quote! {
                for (__key, __value) in #bi.iter() {
                    let __inner_tree = ::relay_protocol::IntoValue::extract_meta_tree(__value);
                    if !__inner_tree.is_empty() {
                        __child_meta.insert(__key.to_string(), __inner_tree);
                    }
                }
            })
            .to_tokens(&mut extract_child_meta_body);
        } else if field_attrs.flatten {
            if is_tuple_struct {
                panic!("flatten not allowed in tuple struct");
            }

            (quote! {
                let #bi = ::relay_protocol::FromObjectRef::from_object_ref(__obj);
            })
            .to_tokens(&mut from_value_body);

            (quote! {
                ::relay_protocol::IntoObjectRef::into_object_ref(#bi, __map);
            })
            .to_tokens(&mut to_value_body);

            (quote! {
                ::relay_protocol::IntoValue::serialize_payload(#bi, ::serde::__private228::ser::FlatMapSerializer(&mut __map_serializer), __behavior)?;
            }).to_tokens(&mut serialize_body);

            (quote! {
                __child_meta.extend(::relay_protocol::IntoValue::extract_child_meta(#bi));
            })
            .to_tokens(&mut extract_child_meta_body);
        } else {
            if is_tuple_struct {
                (quote! {
                    let #bi = __arr.next();
                })
                .to_tokens(&mut from_value_body);
            } else {
                (quote! {
                    let #bi = __obj.remove(#field_name);
                })
                .to_tokens(&mut from_value_body);

                for legacy_alias in &field_attrs.legacy_aliases {
                    let legacy_field_name = LitStr::new(legacy_alias, Span::call_site());
                    (quote! {
                        let #bi = #bi.or(__obj.remove(#legacy_field_name));
                    })
                    .to_tokens(&mut from_value_body);
                }
            }

            (quote! {
                let #bi = ::relay_protocol::FromValue::from_value(#bi.unwrap_or_else(|| ::relay_protocol::Annotated(None, ::relay_protocol::Meta::default())));
            }).to_tokens(&mut from_value_body);

            if is_tuple_struct {
                (quote! {
                    __arr.push(Annotated::map_value(#bi, ::relay_protocol::IntoValue::into_value));
                })
                .to_tokens(&mut to_value_body);
                (quote! {
                    if !#bi.skip_serialization(#skip_serialization_attr) {
                        ::serde::ser::SerializeSeq::serialize_element(&mut __seq_serializer, &::relay_protocol::SerializePayload(#bi, #skip_serialization_attr))?;
                    }
                }).to_tokens(&mut serialize_body);
            } else {
                (quote! {
                    __map.insert(#field_name.to_string(), Annotated::map_value(#bi, ::relay_protocol::IntoValue::into_value));
                }).to_tokens(&mut to_value_body);
                (quote! {
                    if !#bi.skip_serialization(#skip_serialization_attr) {
                        ::serde::ser::SerializeMap::serialize_key(&mut __map_serializer, #field_name)?;
                        ::serde::ser::SerializeMap::serialize_value(&mut __map_serializer, &::relay_protocol::SerializePayload(#bi, #skip_serialization_attr))?;
                    }
                }).to_tokens(&mut serialize_body);
            }

            (quote! {
                let __inner_tree = ::relay_protocol::IntoValue::extract_meta_tree(#bi);
                if !__inner_tree.is_empty() {
                    __child_meta.insert(#field_name.to_string(), __inner_tree);
                }
            })
            .to_tokens(&mut extract_child_meta_body);
        }
    }

    let ast = s.ast();
    let expectation = LitStr::new(&ast.ident.to_string().to_lowercase(), Span::call_site());
    let mut variant = variant.clone();
    for binding in variant.bindings_mut() {
        binding.style = synstructure::BindStyle::Move;
    }
    let to_value_pat = variant.pat();
    let to_structure_assemble_pat = variant.pat();
    for binding in variant.bindings_mut() {
        binding.style = synstructure::BindStyle::Ref;
    }
    let serialize_pat = variant.pat();

    Ok(match t {
        Trait::From => {
            let bindings_count = variant.bindings().len();
            if is_tuple_struct {
                s.gen_impl(quote! {
                    #[automatically_derived]
                    gen impl ::relay_protocol::FromValue for @Self {
                        fn from_value(
                            __value: ::relay_protocol::Annotated<::relay_protocol::Value>,
                        ) -> ::relay_protocol::Annotated<Self> {
                            match __value {
                                ::relay_protocol::Annotated(Some(::relay_protocol::Value::Array(mut __arr)), mut __meta) => {
                                    if __arr.len() != #bindings_count {
                                        __meta.add_error(Error::expected(concat!("a ", stringify!(#bindings_count), "-tuple")));
                                        __meta.set_original_value(Some(__arr));
                                        Annotated(None, __meta)
                                    } else {
                                        let mut __arr = __arr.into_iter();
                                        #from_value_body;
                                        ::relay_protocol::Annotated(Some(#to_structure_assemble_pat), __meta)
                                    }
                                }
                                ::relay_protocol::Annotated(None, __meta) => ::relay_protocol::Annotated(None, __meta),
                                ::relay_protocol::Annotated(Some(__value), mut __meta) => {
                                    __meta.add_error(::relay_protocol::Error::expected(#expectation));
                                    __meta.set_original_value(Some(__value));
                                    ::relay_protocol::Annotated(None, __meta)
                                }
                            }
                        }
                    }
                })
            } else {
                s.gen_impl(quote! {
                    #[automatically_derived]
                    gen impl ::relay_protocol::FromValue for @Self {
                        fn from_value(
                            mut __value: ::relay_protocol::Annotated<::relay_protocol::Value>,
                        ) -> ::relay_protocol::Annotated<Self> {
                            match __value {
                                ::relay_protocol::Annotated(Some(::relay_protocol::Value::Object(ref mut __obj)), __meta) => {
                                    #from_value_body;
                                    ::relay_protocol::Annotated(Some(#to_structure_assemble_pat), __meta)
                                },
                                ::relay_protocol::Annotated(None, __meta) => ::relay_protocol::Annotated(None, __meta),
                                ::relay_protocol::Annotated(Some(__value), mut __meta) => {
                                    __meta.add_error(::relay_protocol::Error::expected(#expectation));
                                    __meta.set_original_value(Some(__value));
                                    ::relay_protocol::Annotated(None, __meta)
                                }
                            }
                        }
                    }

                    #[automatically_derived]
                    gen impl ::relay_protocol::FromObjectRef for @Self {
                        fn from_object_ref(__obj: &mut relay_protocol::Object<::relay_protocol::Value>) -> Self {
                            #from_value_body;
                            #to_structure_assemble_pat
                        }
                    }
                })
            }
        }
        Trait::To => {
            let into_value = if is_tuple_struct {
                quote! {
                    let mut __arr = ::relay_protocol::Array::new();
                    let #to_value_pat = self;
                    #to_value_body;
                    ::relay_protocol::Value::Array(__arr)
                }
            } else {
                quote! {
                    let mut __map_ret = ::relay_protocol::Object::new();
                    let #to_value_pat = self;
                    let mut __map = &mut __map_ret;
                    #to_value_body;
                    ::relay_protocol::Value::Object(__map_ret)
                }
            };

            let serialize_payload = if is_tuple_struct {
                quote! {
                    let mut __seq_serializer = ::serde::ser::Serializer::serialize_seq(__serializer, None)?;
                    #serialize_body;
                    ::serde::ser::SerializeSeq::end(__seq_serializer)
                }
            } else {
                quote! {
                    let mut __map_serializer = ::serde::ser::Serializer::serialize_map(__serializer, None)?;
                    #serialize_body;
                    ::serde::ser::SerializeMap::end(__map_serializer)
                }
            };

            let into_value = s.gen_impl(quote! {
                #[automatically_derived]
                gen impl ::relay_protocol::IntoValue for @Self {
                    fn into_value(self) -> ::relay_protocol::Value {
                        #into_value
                    }

                    fn serialize_payload<S>(&self, __serializer: S, __behavior: ::relay_protocol::SkipSerialization) -> Result<S::Ok, S::Error>
                    where
                        Self: Sized,
                        S: ::serde::ser::Serializer
                    {
                        let #serialize_pat = *self;
                        #serialize_payload
                    }

                    fn extract_child_meta(&self) -> ::relay_protocol::MetaMap
                    where
                        Self: Sized,
                    {
                        let mut __child_meta = ::relay_protocol::MetaMap::new();
                        let #serialize_pat = *self;
                        #extract_child_meta_body;
                        __child_meta
                    }
                }
            });

            let into_object_ref = (!is_tuple_struct).then(|| s.gen_impl(quote! {
                #[automatically_derived]
                gen impl ::relay_protocol::IntoObjectRef for @Self {
                    fn into_object_ref(self, __map: &mut ::relay_protocol::Object<::relay_protocol::Value>) {
                        let #to_value_pat = self;
                        #to_value_body;
                    }
                }
            }));

            quote! {
                #into_value
                #into_object_ref
            }
        }
    })
}

#[derive(Default)]
struct TypeAttrs {
    tag_key: Option<String>,
}

fn parse_type_attributes(s: &synstructure::Structure<'_>) -> syn::Result<TypeAttrs> {
    let mut rv = TypeAttrs::default();

    for attr in &s.ast().attrs {
        if !attr.path().is_ident("metastructure") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            let ident = meta.path.require_ident()?;

            if ident == "tag_key" {
                let s = meta.value()?.parse::<LitStr>()?;
                rv.tag_key = Some(s.value());
            } else {
                // Ignore other attributes used by `ProcessValue` derive macro.
                let _ = meta.value()?.parse::<Lit>()?;
            }

            Ok(())
        })?;
    }

    if rv.tag_key.is_some() && s.variants().len() == 1 {
        // TODO: move into parse_type_attributes
        return Err(Error::new(
            s.ast().span(),
            "tag_key not supported on structs",
        ));
    }

    Ok(rv)
}

#[derive(Default)]
struct FieldAttrs {
    additional_properties: bool,
    field_name: String,
    flatten: bool,
    legacy_aliases: Vec<String>,
    skip_serialization: SkipSerialization,
}

#[derive(Copy, Clone, Default)]
enum SkipSerialization {
    #[default]
    Never,
    Null(bool),
    Empty(bool),
}

impl SkipSerialization {
    fn as_tokens(self) -> TokenStream {
        match self {
            SkipSerialization::Never => quote!(::relay_protocol::SkipSerialization::Never),
            SkipSerialization::Null(deep) => {
                quote!(::relay_protocol::SkipSerialization::Null(#deep))
            }
            SkipSerialization::Empty(deep) => {
                quote!(::relay_protocol::SkipSerialization::Empty(#deep))
            }
        }
    }
}

impl FromStr for SkipSerialization {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, ()> {
        Ok(match s {
            "never" => SkipSerialization::Never,
            "null" => SkipSerialization::Null(false),
            "null_deep" => SkipSerialization::Null(true),
            "empty" => SkipSerialization::Empty(false),
            "empty_deep" => SkipSerialization::Empty(true),
            _ => return Err(()),
        })
    }
}

fn parse_field_attributes(
    index: usize,
    bi_ast: &syn::Field,
    is_tuple_struct: &mut bool,
) -> syn::Result<FieldAttrs> {
    if bi_ast.ident.is_none() {
        *is_tuple_struct = true;
    } else if *is_tuple_struct {
        panic!("invalid tuple struct");
    }

    let mut rv = FieldAttrs::default();
    if !*is_tuple_struct {
        rv.skip_serialization = SkipSerialization::Null(false);
    }
    rv.field_name = bi_ast
        .ident
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| index.to_string());

    for attr in &bi_ast.attrs {
        if !attr.path().is_ident("metastructure") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            let ident = meta.path.require_ident()?;

            if ident == "additional_properties" {
                rv.additional_properties = true;
            } else if ident == "omit_from_schema" {
                // Skip
            } else if ident == "field" {
                rv.field_name = meta.value()?.parse::<LitStr>()?.value();
            } else if ident == "flatten" {
                rv.flatten = true;
            } else if ident == "legacy_alias" {
                rv.legacy_aliases
                    .push(meta.value()?.parse::<LitStr>()?.value());
            } else if ident == "skip_serialization" {
                rv.skip_serialization = meta
                    .value()?
                    .parse::<LitStr>()?
                    .value()
                    .parse()
                    .map_err(|_| meta.error("Unknown value"))?;
            } else {
                // Ignore other attributes used by `ProcessValue` derive macro.
                let _ = meta.value()?.parse::<Lit>()?;
            }

            Ok(())
        })?;
    }

    Ok(rv)
}

#[derive(Default)]
struct VariantAttrs {
    omit_from_schema: bool,
    tag_override: Option<String>,
    fallback_variant: bool,
}

fn parse_variant_attributes(attrs: &[syn::Attribute]) -> syn::Result<VariantAttrs> {
    let mut rv = VariantAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("metastructure") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            let ident = meta.path.require_ident()?;

            if ident == "fallback_variant" {
                rv.tag_override = None;
                rv.fallback_variant = true;
            } else if ident == "omit_from_schema" {
                rv.omit_from_schema = true;
            } else if ident == "tag" {
                rv.tag_override = Some(meta.value()?.parse::<LitStr>()?.value());
            } else {
                // Ignore other attributes used by `ProcessValue` derive macro.
                let _ = meta.value()?.parse::<Lit>()?;
            }

            Ok(())
        })?;
    }

    Ok(rv)
}

fn is_newtype_variant(variant: &synstructure::VariantInfo) -> bool {
    variant.bindings().len() == 1 && variant.bindings()[0].ast().ident.is_none()
}

fn is_newtype(s: &synstructure::Structure<'_>) -> bool {
    if s.variants().len() != 1 {
        // We have more than one variant (e.g. `enum Foo { A, B }`)
        return false;
    }

    if s.variants()[0].bindings().len() != 1 {
        // The single variant has multiple fields
        // e.g. `struct Foo(Bar, Baz)`
        //      `enum Foo { A(X, Y) }`
        return false;
    }

    if s.variants()[0].bindings()[0].ast().ident.is_some() {
        // The variant has a name
        // e.g. `struct Foo { bar: Bar }` instead of `struct Foo(Bar)`
        return false;
    }

    true
}
