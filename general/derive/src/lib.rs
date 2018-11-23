#![recursion_limit = "256"]
extern crate syn;

#[macro_use]
extern crate synstructure;
#[macro_use]
extern crate quote;
extern crate proc_macro2;

use proc_macro2::{Span, TokenStream};
use quote::ToTokens;
use syn::{Data, Ident, Lit, LitBool, LitStr, Meta, MetaNameValue, NestedMeta};

#[derive(Debug, Clone, Copy)]
enum Trait {
    From,
    To,
    Process,
}

decl_derive!([ToValue, attributes(metastructure)] => process_to_value);
decl_derive!([FromValue, attributes(metastructure)] => process_from_value);
decl_derive!([ProcessValue, attributes(metastructure)] => process_process_value);

fn process_to_value(s: synstructure::Structure) -> TokenStream {
    process_metastructure_impl(s, Trait::To)
}

fn process_from_value(s: synstructure::Structure) -> TokenStream {
    process_metastructure_impl(s, Trait::From)
}

fn process_process_value(s: synstructure::Structure) -> TokenStream {
    process_metastructure_impl(s, Trait::Process)
}

fn process_wrapper_struct_derive(
    s: synstructure::Structure,
    t: Trait,
) -> Result<TokenStream, synstructure::Structure> {
    // The next few blocks are for finding out whether the given type is of the form:
    // struct Foo(Bar)  (tuple struct with a single field)

    if s.variants().len() != 1 {
        // We have more than one variant (e.g. `enum Foo { A, B }`)
        return Err(s);
    }

    if s.variants()[0].bindings().len() != 1 {
        // The single variant has multiple fields
        // e.g. `struct Foo(Bar, Baz)`
        //      `enum Foo { A(X, Y) }`
        return Err(s);
    }

    if s.variants()[0].bindings()[0].ast().ident.is_some() {
        // The variant has a name
        // e.g. `struct Foo { bar: Bar }` instead of `struct Foo(Bar)`
        return Err(s);
    }

    let mut skip_serialization_body = TokenStream::new();

    let type_attrs = parse_type_attributes(&s.ast().attrs);
    if type_attrs.tag_key.is_some() {
        panic!("tag_key not supported on structs");
    }

    if type_attrs.never_skip_serialization {
        quote!(false)
    } else {
        quote!(crate::processor::ToValue::skip_serialization(&self.0))
    }.to_tokens(&mut skip_serialization_body);

    let name = &s.ast().ident;

    Ok(match t {
        Trait::From=> {
            s.gen_impl(quote! {
                gen impl crate::processor::FromValue for @Self {
                    #[inline(always)]
                    fn from_value(
                        __value: crate::types::Annotated<crate::types::Value>,
                    ) -> crate::types::Annotated<Self> {
                        match crate::processor::FromValue::from_value(__value) {
                            Annotated(Some(__value), __meta) => Annotated(Some(#name(__value)), __meta),
                            Annotated(None, __meta) => Annotated(None, __meta),
                        }
                    }
                }
            })
        }
        Trait::To=> {
            s.gen_impl(quote! {
                extern crate serde as __serde;

                gen impl crate::processor::ToValue for @Self {
                    #[inline(always)]
                    fn to_value(
                        mut __value: crate::types::Annotated<Self>
                    ) -> crate::types::Annotated<crate::types::Value> {
                        let __value = __value.map_value(|x| x.0);
                        crate::processor::ToValue::to_value(__value)
                    }

                    #[inline(always)]
                    fn serialize_payload<S>(&self, __serializer: S) -> Result<S::Ok, S::Error>
                    where
                        Self: Sized,
                        S: __serde::ser::Serializer
                    {
                        crate::processor::ToValue::serialize_payload(&self.0, __serializer)
                    }

                    #[inline(always)]
                    fn extract_child_meta(&self) -> crate::types::MetaMap
                    where
                        Self: Sized,
                    {
                        crate::processor::ToValue::extract_child_meta(&self.0)
                    }

                    #[inline(always)]
                    fn skip_serialization(&self) -> bool
                    where
                        Self: Sized,
                    {
                        #skip_serialization_body
                    }
                }
            })
        }
        Trait::Process=> {
            s.gen_impl(quote! {
                gen impl crate::processor::ProcessValue for @Self {
                    #[inline(always)]
                    fn process_child_values<P: crate::processor::Processor>(
                        __value: crate::types::Annotated<Self>,
                        __processor: &mut P,
                        __state: crate::processor::ProcessingState
                    ) -> crate::types::Annotated<Self> {
                        let __new_annotated = match __value {
                            crate::types::Annotated(Some(__value), __meta) => {
                                crate::processor::ProcessValue::process_value(
                                    crate::types::Annotated(Some(__value.0), __meta), __processor, __state)
                            }
                            crate::types::Annotated(None, __meta) => crate::types::Annotated(None, __meta)
                        };
                        match __new_annotated {
                            crate::types::Annotated(Some(__value), __meta) => crate::types::Annotated(Some(#name(__value)), __meta),
                            crate::types::Annotated(None, __meta) => crate::types::Annotated(None, __meta)
                        }
                    }
                }
            })
        }
    })
}

#[cfg_attr(feature = "cargo-clippy", allow(cyclomatic_complexity))]
fn process_enum_struct_derive(
    s: synstructure::Structure,
    t: Trait,
) -> Result<TokenStream, synstructure::Structure> {
    if let Data::Enum(_) = s.ast().data {
    } else {
        return Err(s);
    }

    let type_attrs = parse_type_attributes(&s.ast().attrs);

    let type_name = &s.ast().ident;
    let tag_key_str = LitStr::new(
        &type_attrs.tag_key.unwrap_or_else(|| "type".to_string()),
        Span::call_site(),
    );
    let mut from_value_body = TokenStream::new();
    let mut to_value_body = TokenStream::new();
    let mut process_value_body = TokenStream::new();
    let mut serialize_body = TokenStream::new();
    let mut extract_child_meta_body = TokenStream::new();

    let process_value = type_attrs.process_func.map(|func_name| {
        let func_name = Ident::new(&func_name, Span::call_site());
        quote! {
            fn process_value<P: crate::processor::Processor>(
                __value: crate::types::Annotated<Self>,
                __processor: &mut P,
                __state: crate::processor::ProcessingState
            ) -> crate::types::Annotated<Self> {
                __processor.#func_name(__value, __state)
            }
        }
    });

    for variant in s.variants() {
        let variant_attrs = parse_variant_attributes(&variant.ast().attrs);
        let mut variant_name = &variant.ast().ident;
        let tag = variant_attrs
            .tag_override
            .unwrap_or_else(|| variant_name.to_string().to_lowercase());

        if !variant_attrs.fallback_variant {
            let tag = LitStr::new(&tag, Span::call_site());
            (quote! {
                Some(#tag) => {
                    crate::processor::FromValue::from_value(crate::types::Annotated(Some(crate::types::Value::Object(__object)), __meta))
                        .map_value(|__value| #type_name::#variant_name(Box::new(__value)))
                }
            }).to_tokens(&mut from_value_body);
            (quote! {
                crate::types::Annotated(Some(#type_name::#variant_name(__value)), __meta) => {
                    let mut __rv = crate::processor::ToValue::to_value(crate::types::Annotated(Some(__value), __meta));
                    if let crate::types::Annotated(Some(crate::types::Value::Object(ref mut __object)), _) = __rv {
                        __object.insert(#tag_key_str.to_string(), Annotated::new(crate::types::Value::String(#tag.to_string())));
                    }
                    __rv
                }
            }).to_tokens(&mut to_value_body);
            (quote! {
                #type_name::#variant_name(ref __value) => {
                    crate::processor::ToValue::extract_child_meta(__value)
                }
            }).to_tokens(&mut extract_child_meta_body);
            (quote! {
                #type_name::#variant_name(ref __value) => {
                    let mut __map_ser = __serde::Serializer::serialize_map(__serializer, None)?;
                    crate::processor::ToValue::serialize_payload(__value, __serde::private::ser::FlatMapSerializer(&mut __map_ser))?;
                    __serde::ser::SerializeMap::serialize_key(&mut __map_ser, #tag_key_str)?;
                    __serde::ser::SerializeMap::serialize_value(&mut __map_ser, #tag)?;
                    __serde::ser::SerializeMap::end(__map_ser)
                }
            }).to_tokens(&mut serialize_body);
            (quote! {
                crate::types::Annotated(Some(#type_name::#variant_name(__value)), __meta) => {
                    crate::processor::ProcessValue::process_value(crate::types::Annotated(Some(*__value), __meta), __processor, __state)
                        .map_value(|__value| #type_name::#variant_name(Box::new(__value)))
                }
            }).to_tokens(&mut process_value_body);
        } else {
            (quote! {
                _ => {
                    if let Some(__type) = __type {
                        __object.insert(#tag_key_str.to_string(), __type);
                    }
                    crate::types::Annotated(Some(#type_name::#variant_name(__object)), __meta)
                }
            }).to_tokens(&mut from_value_body);
            (quote! {
                crate::types::Annotated(Some(#type_name::#variant_name(__value)), __meta) => {
                    crate::processor::ToValue::to_value(crate::types::Annotated(Some(__value), __meta))
                }
            }).to_tokens(&mut to_value_body);
            (quote! {
                #type_name::#variant_name(ref __value) => {
                    crate::processor::ToValue::extract_child_meta(__value)
                }
            }).to_tokens(&mut extract_child_meta_body);
            (quote! {
                #type_name::#variant_name(ref __value) => {
                    crate::processor::ToValue::serialize_payload(__value, __serializer)
                }
            }).to_tokens(&mut serialize_body);
            (quote! {
                crate::types::Annotated(Some(#type_name::#variant_name(__value)), __meta) => {
                    crate::processor::ProcessValue::process_value(crate::types::Annotated(Some(__value), __meta), __processor, __state)
                        .map_value(#type_name::#variant_name)
                }
            }).to_tokens(&mut process_value_body);
        }
    }

    Ok(match t {
        Trait::From=> {
            s.gen_impl(quote! {
                gen impl crate::processor::FromValue for @Self {
                    fn from_value(
                        __value: crate::types::Annotated<crate::types::Value>,
                    ) -> crate::types::Annotated<Self> {
                        match crate::types::Object::<crate::types::Value>::from_value(__value) {
                            crate::types::Annotated(Some(mut __object), __meta) => {
                                let __type = __object.remove(#tag_key_str);
                                match __type.as_ref().and_then(|__type| __type.0.as_ref()).and_then(|__type| __type.as_str()) {
                                    #from_value_body
                                }
                            }
                            crate::types::Annotated(None, __meta) => crate::types::Annotated(None, __meta)
                        }
                    }
                }
            })
        }
        Trait::To=> {
            s.gen_impl(quote! {
                extern crate serde as __serde;

                gen impl crate::processor::ToValue for @Self {
                    fn to_value(
                        __value: crate::types::Annotated<Self>
                    ) -> crate::types::Annotated<crate::types::Value> {
                        match __value {
                            #to_value_body
                            crate::types::Annotated(None, __meta) => crate::types::Annotated(None, __meta),
                        }
                    }

                    fn serialize_payload<S>(&self, __serializer: S) -> Result<S::Ok, S::Error>
                    where
                        S: __serde::ser::Serializer
                    {
                        match *self {
                            #serialize_body
                        }
                    }

                    fn extract_child_meta(&self) -> crate::types::MetaMap
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
        Trait::Process=> {
            s.gen_impl(quote! {
                gen impl crate::processor::ProcessValue for @Self {
                    fn process_child_values<P: crate::processor::Processor>(
                        __value: crate::types::Annotated<Self>,
                        __processor: &mut P,
                        __state: crate::processor::ProcessingState
                    ) -> crate::types::Annotated<Self> {
                        match __value {
                            #process_value_body
                            crate::types::Annotated(None, __meta) => crate::types::Annotated(None, __meta),
                        }
                    }

                    #process_value
                }
            })
        }
    })
}

#[cfg_attr(feature = "cargo-clippy", allow(cyclomatic_complexity))]
fn process_metastructure_impl(s: synstructure::Structure, t: Trait) -> TokenStream {
    let s = match process_wrapper_struct_derive(s, t) {
        Ok(stream) => return stream,
        Err(s) => s,
    };

    let mut s = match process_enum_struct_derive(s, t) {
        Ok(stream) => return stream,
        Err(s) => s,
    };

    s.add_bounds(synstructure::AddBounds::Generics);

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
    let mut process_value_body = TokenStream::new();
    (quote! {
        use lazy_static::lazy_static;
        use regex::Regex;
    }).to_tokens(&mut process_value_body);
    let mut serialize_body = TokenStream::new();
    let mut extract_child_meta_body = TokenStream::new();
    let mut skip_serialization_body = TokenStream::new();
    let mut tmp_idx = 0;

    let type_attrs = parse_type_attributes(&s.ast().attrs);
    if type_attrs.tag_key.is_some() {
        panic!("tag_key not supported on structs");
    }
    if type_attrs.never_skip_serialization {
        (quote! {
            return false;
        }).to_tokens(&mut skip_serialization_body);
    }

    for bi in variant.bindings() {
        let field_attrs = parse_field_attributes(&bi.ast().attrs);
        let field_name = field_attrs.field_name_override.unwrap_or_else(|| {
            bi.ast()
                .ident
                .as_ref()
                .expect("can not derive struct tuples")
                .to_string()
        });
        let field_name = LitStr::new(&field_name, Span::call_site());

        if field_attrs.additional_properties {
            (quote! {
                let #bi = __obj.into_iter().map(|(__key, __value)| (__key, crate::processor::FromValue::from_value(__value))).collect();
            }).to_tokens(&mut from_value_body);
            (quote! {
                __map.extend(#bi.into_iter().map(|(__key, __value)| (__key, crate::processor::ToValue::to_value(__value))));
            }).to_tokens(&mut to_value_body);
            (quote! {
                let #bi = #bi.into_iter().map(|(__key, __value)| {
                    let __value = crate::processor::ProcessValue::process_value(__value, __processor, __state.enter_borrowed(__key.as_str(), None));
                    (__key, __value)
                }).collect();
            }).to_tokens(&mut process_value_body);
            (quote! {
                for (__key, __value) in #bi.iter() {
                    if !__value.skip_serialization() {
                        __serde::ser::SerializeMap::serialize_key(&mut __map_serializer, __key)?;
                        __serde::ser::SerializeMap::serialize_value(&mut __map_serializer, &crate::processor::SerializePayload(__value))?;
                    }
                }
            }).to_tokens(&mut serialize_body);
            (quote! {
                for (__key, __value) in #bi.iter() {
                    let __inner_tree = crate::processor::ToValue::extract_meta_tree(__value);
                    if !__inner_tree.is_empty() {
                        __child_meta.insert(__key.to_string(), __inner_tree);
                    }
                }
            }).to_tokens(&mut extract_child_meta_body);
        } else {
            let field_attrs_name = Ident::new(
                &format!("__field_attrs_{}", {
                    tmp_idx += 1;
                    tmp_idx
                }),
                Span::call_site(),
            );
            let required_attr = LitBool {
                value: field_attrs.required,
                span: Span::call_site(),
            };

            let nonempty_attr = LitBool {
                value: field_attrs.nonempty,
                span: Span::call_site(),
            };

            let max_chars_attr = field_attrs.max_chars;
            let bag_size_attr = field_attrs.bag_size;
            let pii_kind_attr = field_attrs.pii_kind;

            (quote! {
                let #bi = __obj.remove(#field_name);
            }).to_tokens(&mut from_value_body);

            for legacy_alias in field_attrs.legacy_aliases {
                let legacy_field_name = LitStr::new(&legacy_alias, Span::call_site());
                (quote! {
                    let #bi = #bi.or(__obj.remove(#legacy_field_name));
                }).to_tokens(&mut from_value_body);
            }

            (quote! {
                let #bi = crate::processor::FromValue::from_value(#bi.unwrap_or_else(|| crate::types::Annotated(None, crate::types::Meta::default())));
            }).to_tokens(&mut from_value_body);

            if field_attrs.required {
                (quote! {
                    let mut #bi = #bi;
                    #bi.require_value();
                }).to_tokens(&mut from_value_body);
            }

            let match_regex_attr = if let Some(match_regex) = field_attrs.match_regex {
                quote!(Some(Regex::new(#match_regex).unwrap()))
            } else {
                quote!(None)
            };

            (quote! {
                __map.insert(#field_name.to_string(), crate::processor::ToValue::to_value(#bi));
            }).to_tokens(&mut to_value_body);
            (quote! {
                lazy_static! {
                    static ref #field_attrs_name: crate::processor::FieldAttrs = crate::processor::FieldAttrs {
                        name: Some(#field_name),
                        required: #required_attr,
                        nonempty: #nonempty_attr,
                        match_regex: #match_regex_attr,
                        max_chars: #max_chars_attr,
                        bag_size: #bag_size_attr,
                        pii_kind: #pii_kind_attr,
                    };
                }

                let #bi = crate::processor::ProcessValue::process_value(#bi, __processor, __state.enter_static(#field_name, Some(::std::borrow::Cow::Borrowed(&*#field_attrs_name))));
            }).to_tokens(&mut process_value_body);
            (quote! {
                if !#bi.skip_serialization() {
                    __serde::ser::SerializeMap::serialize_key(&mut __map_serializer, #field_name)?;
                    __serde::ser::SerializeMap::serialize_value(&mut __map_serializer, &crate::processor::SerializePayload(#bi))?;
                }
            }).to_tokens(&mut serialize_body);
            (quote! {
                let __inner_tree = crate::processor::ToValue::extract_meta_tree(#bi);
                if !__inner_tree.is_empty() {
                    __child_meta.insert(#field_name.to_string(), __inner_tree);
                }
            }).to_tokens(&mut extract_child_meta_body);
        }

        (quote! {
            if !#bi.skip_serialization() {
                return false;
            }
        }).to_tokens(&mut skip_serialization_body);
    }

    let ast = s.ast();
    let expectation = LitStr::new(
        &format!("expected {}", ast.ident.to_string().to_lowercase()),
        Span::call_site(),
    );
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

    let process_value = type_attrs.process_func.map(|func_name| {
        let func_name = Ident::new(&func_name, Span::call_site());
        quote! {
            fn process_value<P: crate::processor::Processor>(
                __value: crate::types::Annotated<Self>,
                __processor: &mut P,
                __state: crate::processor::ProcessingState
            ) -> crate::types::Annotated<Self> {
                __processor.#func_name(__value, __state)
            }
        }
    });

    match t {
        Trait::From=> {
            s.gen_impl(quote! {
                gen impl crate::processor::FromValue for @Self {
                    fn from_value(
                        __value: crate::types::Annotated<crate::types::Value>,
                    ) -> crate::types::Annotated<Self> {
                        match __value {
                            crate::types::Annotated(Some(crate::types::Value::Object(mut __obj)), __meta) => {
                                #from_value_body;
                                crate::types::Annotated(Some(#to_structure_assemble_pat), __meta)
                            }
                            crate::types::Annotated(None, __meta) => crate::types::Annotated(None, __meta),
                            crate::types::Annotated(Some(__value), mut __meta) => {
                                __meta.add_unexpected_value_error(#expectation, __value);
                                crate::types::Annotated(None, __meta)
                            }
                        }
                    }
                }
            })
        }
        Trait::To=> {
            s.gen_impl(quote! {
                extern crate serde as __serde;

                gen impl crate::processor::ToValue for @Self {
                    fn to_value(
                        __value: crate::types::Annotated<Self>
                    ) -> crate::types::Annotated<crate::types::Value> {
                        let crate::types::Annotated(__value, __meta) = __value;
                        if let Some(__value) = __value {
                            let mut __map = crate::types::Object::new();
                            let #to_value_pat = __value;
                            #to_value_body;
                            crate::types::Annotated(Some(crate::types::Value::Object(__map)), __meta)
                        } else {
                            crate::types::Annotated(None, __meta)
                        }
                    }

                    fn serialize_payload<S>(&self, __serializer: S) -> Result<S::Ok, S::Error>
                    where
                        Self: Sized,
                        S: __serde::ser::Serializer
                    {
                        let #serialize_pat = *self;
                        let mut __map_serializer = __serde::ser::Serializer::serialize_map(__serializer, None)?;
                        #serialize_body;
                        __serde::ser::SerializeMap::end(__map_serializer)
                    }

                    fn extract_child_meta(&self) -> crate::types::MetaMap
                    where
                        Self: Sized,
                    {
                        let mut __child_meta = crate::types::MetaMap::new();
                        let #serialize_pat = *self;
                        #extract_child_meta_body;
                        __child_meta
                    }

                    fn skip_serialization(&self) -> bool {
                        let #serialize_pat = self;
                        #skip_serialization_body;
                        true
                    }
                }
            })
        }
        Trait::Process=> {
            s.gen_impl(quote! {
                gen impl crate::processor::ProcessValue for @Self {
                    #process_value
                    fn process_child_values<P: crate::processor::Processor>(
                        __value: crate::types::Annotated<Self>,
                        __processor: &mut P,
                        __state: crate::processor::ProcessingState
                    ) -> crate::types::Annotated<Self> {
                        let crate::types::Annotated(__value, __meta) = __value;
                        if let Some(__value) = __value {
                            let #to_value_pat = __value;
                            #process_value_body;
                            crate::types::Annotated(Some(#to_structure_assemble_pat), __meta)
                        } else {
                            crate::types::Annotated(None, __meta)
                        }
                    }
                }
            })
        }
    }
}

fn parse_max_chars(name: &str) -> TokenStream {
    match name {
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
        _ => panic!("invalid max_chars variant '{}'", name),
    }
}

fn parse_bag_size(name: &str) -> TokenStream {
    match name {
        "small" => quote!(crate::processor::BagSize::Small),
        "medium" => quote!(crate::processor::BagSize::Medium),
        "large" => quote!(crate::processor::BagSize::Large),
        _ => panic!("invalid bag_size variant '{}'", name),
    }
}

fn parse_pii_kind(kind: &str) -> TokenStream {
    match kind {
        "freeform" => quote!(crate::processor::PiiKind::Freeform),
        "ip" => quote!(crate::processor::PiiKind::Ip),
        "id" => quote!(crate::processor::PiiKind::Id),
        "username" => quote!(crate::processor::PiiKind::Username),
        "hostname" => quote!(crate::processor::PiiKind::Hostname),
        "sensitive" => quote!(crate::processor::PiiKind::Sensitive),
        "name" => quote!(crate::processor::PiiKind::Name),
        "email" => quote!(crate::processor::PiiKind::Email),
        "location" => quote!(crate::processor::PiiKind::Location),
        "databag" => quote!(crate::processor::PiiKind::Databag),
        _ => panic!("invalid pii_kind variant '{}'", kind),
    }
}

#[derive(Default)]
struct TypeAttrs {
    process_func: Option<String>,
    tag_key: Option<String>,
    never_skip_serialization: bool,
}

fn parse_type_attributes(attrs: &[syn::Attribute]) -> TypeAttrs {
    let mut rv = TypeAttrs::default();

    for attr in attrs {
        let meta = match attr.interpret_meta() {
            Some(meta) => meta,
            None => continue,
        };
        if meta.name() != "metastructure" {
            continue;
        }

        if let Meta::List(metalist) = meta {
            for nested_meta in metalist.nested {
                match nested_meta {
                    NestedMeta::Literal(..) => panic!("unexpected literal attribute"),
                    NestedMeta::Meta(meta) => match meta {
                        Meta::NameValue(MetaNameValue { ident, lit, .. }) => {
                            if ident == "process_func" {
                                match lit {
                                    Lit::Str(litstr) => {
                                        rv.process_func = Some(litstr.value());
                                    }
                                    _ => {
                                        panic!("Got non string literal for field");
                                    }
                                }
                            } else if ident == "tag_key" {
                                match lit {
                                    Lit::Str(litstr) => {
                                        rv.tag_key = Some(litstr.value());
                                    }
                                    _ => {
                                        panic!("Got non string literal for tag_key");
                                    }
                                }
                            } else if ident == "skip_serialization" {
                                match lit {
                                    Lit::Str(litstr) => match litstr.value().as_ref() {
                                        "never" => rv.never_skip_serialization = true,
                                        _ => panic!("Unknown value for skip_serialization"),
                                    },
                                    _ => {
                                        panic!("Got non string literal for skip_serialization");
                                    }
                                }
                            } else {
                                panic!("Unknown attribute")
                            }
                        }
                        _ => panic!("Unsupported attribute"),
                    },
                }
            }
        }
    }

    rv
}

#[derive(Default)]
struct FieldAttrs {
    additional_properties: bool,
    field_name_override: Option<String>,
    required: bool,
    nonempty: bool,
    match_regex: Option<String>,
    max_chars: TokenStream,
    bag_size: TokenStream,
    pii_kind: TokenStream,
    legacy_aliases: Vec<String>,
}

#[cfg_attr(feature = "cargo-clippy", allow(cyclomatic_complexity))]
fn parse_field_attributes(attrs: &[syn::Attribute]) -> FieldAttrs {
    let mut rv = FieldAttrs::default();
    rv.max_chars = quote!(None);
    rv.bag_size = quote!(None);
    rv.pii_kind = quote!(None);

    let mut required = None;

    for attr in attrs {
        let meta = match attr.interpret_meta() {
            Some(meta) => meta,
            None => continue,
        };
        if meta.name() != "metastructure" {
            continue;
        }

        if let Meta::List(metalist) = meta {
            for nested_meta in metalist.nested {
                match nested_meta {
                    NestedMeta::Literal(..) => panic!("unexpected literal attribute"),
                    NestedMeta::Meta(meta) => match meta {
                        Meta::Word(ident) => {
                            if ident == "additional_properties" {
                                rv.additional_properties = true;
                            } else {
                                panic!("Unknown attribute {}", ident);
                            }
                        }
                        Meta::NameValue(MetaNameValue { ident, lit, .. }) => {
                            if ident == "field" {
                                match lit {
                                    Lit::Str(litstr) => {
                                        rv.field_name_override = Some(litstr.value());
                                    }
                                    _ => {
                                        panic!("Got non string literal for field");
                                    }
                                }
                            } else if ident == "required" {
                                match lit {
                                    Lit::Str(litstr) => match litstr.value().as_str() {
                                        "true" => required = Some(true),
                                        "false" => required = Some(false),
                                        other => panic!("Unknown value {}", other),
                                    },
                                    _ => {
                                        panic!("Got non string literal for required");
                                    }
                                }
                            } else if ident == "nonempty" {
                                match lit {
                                    Lit::Str(litstr) => match litstr.value().as_str() {
                                        "true" => rv.nonempty = true,
                                        "false" => rv.nonempty = false,
                                        other => panic!("Unknown value {}", other),
                                    },
                                    _ => {
                                        panic!("Got non string literal for required");
                                    }
                                }
                            } else if ident == "match_regex" {
                                match lit {
                                    Lit::Str(litstr) => {
                                        rv.match_regex = Some(litstr.value().clone())
                                    }
                                    _ => panic!("Got non string literal for match_regex"),
                                }
                            } else if ident == "max_chars" {
                                match lit {
                                    Lit::Str(litstr) => {
                                        let attr = parse_max_chars(litstr.value().as_str());
                                        rv.max_chars = quote!(Some(#attr));
                                    }
                                    _ => {
                                        panic!("Got non string literal for max_chars");
                                    }
                                }
                            } else if ident == "bag_size" {
                                match lit {
                                    Lit::Str(litstr) => {
                                        let attr = parse_bag_size(litstr.value().as_str());
                                        rv.bag_size = quote!(Some(#attr));
                                    }
                                    _ => {
                                        panic!("Got non string literal for bag_size");
                                    }
                                }
                            } else if ident == "pii_kind" {
                                match lit {
                                    Lit::Str(litstr) => {
                                        let attr = parse_pii_kind(litstr.value().as_str());
                                        rv.pii_kind = quote!(Some(#attr));
                                    }
                                    _ => {
                                        panic!("Got non string literal for pii_kind");
                                    }
                                }
                            } else if ident == "legacy_alias" {
                                match lit {
                                    Lit::Str(litstr) => {
                                        rv.legacy_aliases.push(litstr.value());
                                    }
                                    _ => {
                                        panic!("Got non string literal for legacy_alias");
                                    }
                                }
                            }
                        }
                        other => {
                            panic!("Unexpected or bad attribute {}", other.name());
                        }
                    },
                }
            }
        }
    }
    if required.is_none() && rv.nonempty {
        panic!(
            "`required` has to be explicitly set to \"true\" or \"false\" if `nonempty` is used."
        );
    }

    rv.required = required.unwrap_or(false);
    rv
}

#[derive(Default)]
struct VariantAttrs {
    tag_override: Option<String>,
    fallback_variant: bool,
}

fn parse_variant_attributes(attrs: &[syn::Attribute]) -> VariantAttrs {
    let mut rv = VariantAttrs::default();
    for attr in attrs {
        let meta = match attr.interpret_meta() {
            Some(meta) => meta,
            None => continue,
        };
        if meta.name() != "metastructure" {
            continue;
        }

        if let Meta::List(metalist) = meta {
            for nested_meta in metalist.nested {
                match nested_meta {
                    NestedMeta::Literal(..) => panic!("unexpected literal attribute"),
                    NestedMeta::Meta(meta) => match meta {
                        Meta::Word(ident) => {
                            if ident == "fallback_variant" {
                                rv.tag_override = None;
                                rv.fallback_variant = true;
                            } else {
                                panic!("Unknown attribute {}", ident);
                            }
                        }
                        Meta::NameValue(MetaNameValue { ident, lit, .. }) => {
                            if ident == "tag" {
                                match lit {
                                    Lit::Str(litstr) => {
                                        rv.tag_override = Some(litstr.value());
                                    }
                                    _ => {
                                        panic!("Got non string literal for tag");
                                    }
                                }
                            } else {
                                panic!("Unknown key {}", ident);
                            }
                        }
                        other => {
                            panic!("Unexpected or bad attribute {}", other.name());
                        }
                    },
                }
            }
        }
    }
    rv
}
