#![recursion_limit = "256"]
#![allow(clippy::cyclomatic_complexity)]

use std::str::FromStr;

use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};
use syn::{Data, Ident, Lit, LitStr, Meta, MetaNameValue, NestedMeta};
use synstructure::decl_derive;

#[derive(Debug, Clone, Copy)]
enum Trait {
    Empty,
    From,
    To,
    Process,
}

decl_derive!([Empty, attributes(metastructure)] => derive_empty);
decl_derive!([ToValue, attributes(metastructure)] => derive_to_value);
decl_derive!([FromValue, attributes(metastructure)] => derive_from_value);
decl_derive!([ProcessValue, attributes(metastructure)] => derive_process_value);

fn derive_empty(s: synstructure::Structure<'_>) -> TokenStream {
    derive_metastructure(s, Trait::Empty)
}

fn derive_to_value(s: synstructure::Structure<'_>) -> TokenStream {
    derive_metastructure(s, Trait::To)
}

fn derive_from_value(s: synstructure::Structure<'_>) -> TokenStream {
    derive_metastructure(s, Trait::From)
}

fn derive_process_value(s: synstructure::Structure<'_>) -> TokenStream {
    derive_metastructure(s, Trait::Process)
}

fn derive_newtype_metastructure(
    s: synstructure::Structure<'_>,
    t: Trait,
) -> Result<TokenStream, synstructure::Structure<'_>> {
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

    let type_attrs = parse_type_attributes(&s.ast().attrs);
    if type_attrs.tag_key.is_some() {
        panic!("tag_key not supported on structs");
    }
    if type_attrs.process_func.is_some() {
        panic!("process_func not supported on newtype structs");
    }

    let field_attrs = parse_field_attributes(0, &s.variants()[0].bindings()[0].ast(), &mut true);
    let skip_serialization_attr = field_attrs.skip_serialization.as_tokens();
    let field_attrs_tokens = field_attrs.as_tokens(Some(quote!(parent_attrs)));

    let name = &s.ast().ident;

    let value_type = type_attrs
        .value_type
        .map(|value_name| {
            let value_name = Ident::new(&value_name, Span::call_site());
            quote! {
                Some(crate::processor::ValueType::#value_name)
            }
        })
        .unwrap_or_else(|| {
            quote! {
                crate::processor::ProcessValue::value_type(&self.0)
            }
        });

    Ok(match t {
        Trait::Empty => s.gen_impl(quote! {
            #[automatically_derived]
            gen impl crate::types::Empty for @Self {
                #[inline]
                fn is_empty(&self) -> bool {
                    crate::types::Empty::is_empty(&self.0)
                }

                #[inline]
                fn is_deep_empty(&self) -> bool {
                    crate::types::Empty::is_deep_empty(&self.0)
                }
            }
        }),
        Trait::From => s.gen_impl(quote! {
            #[automatically_derived]
            gen impl crate::types::FromValue for @Self {
                fn from_value(
                    __value: crate::types::Annotated<crate::types::Value>,
                ) -> crate::types::Annotated<Self> {
                    match crate::types::FromValue::from_value(__value) {
                        Annotated(Some(__value), __meta) => Annotated(Some(#name(__value)), __meta),
                        Annotated(None, __meta) => Annotated(None, __meta),
                    }
                }
            }
        }),
        Trait::To => s.gen_impl(quote! {
            #[automatically_derived]
            gen impl crate::types::ToValue for @Self {
                fn to_value(self) -> crate::types::Value {
                    crate::types::ToValue::to_value(self.0)
                }

                fn serialize_payload<S>(&self, __serializer: S, __behavior: crate::types::SkipSerialization) -> Result<S::Ok, S::Error>
                where
                    Self: Sized,
                    S: ::serde::ser::Serializer
                {
                    crate::types::ToValue::serialize_payload(&self.0, __serializer, #skip_serialization_attr)
                }

                fn extract_child_meta(&self) -> crate::types::MetaMap
                where
                    Self: Sized,
                {
                    crate::types::ToValue::extract_child_meta(&self.0)
                }
            }
        }),
        Trait::Process => s.gen_impl(quote! {
            #[automatically_derived]
            gen impl crate::processor::ProcessValue for @Self {
                #[inline]
                fn value_type(&self) -> Option<crate::processor::ValueType> {
                    #value_type
                }

                #[inline]
                fn process_value<P>(
                    &mut self,
                    meta: &mut crate::types::Meta,
                    processor: &mut P,
                    state: &crate::processor::ProcessingState<'_>,
                ) -> crate::types::ValueAction
                where
                    P: crate::processor::Processor,
                {
                    let parent_attrs = state.attrs();
                    let attrs = #field_attrs_tokens;
                    let state = state.enter_nothing(
                        Some(::std::borrow::Cow::Owned(attrs))
                    );

                    let mut action = processor.before_process(
                        Some(&self.0),
                        meta,
                        &state
                    );

                    if action == crate::types::ValueAction::Keep {
                        action = crate::processor::ProcessValue::process_value(
                            &mut self.0,
                            meta,
                            processor,
                            &state
                        )
                    }

                    let new_value = match action {
                        crate::types::ValueAction::Keep => Some(&self.0),
                        _=> None
                    };

                    processor.after_process(
                        new_value,
                        meta,
                        &state
                    );

                    action
                }

                #[inline]
                fn process_child_values<P>(
                    &mut self,
                    __processor: &mut P,
                    __state: &crate::processor::ProcessingState<'_>,
                )
                where
                    P: crate::processor::Processor,
                {
                    crate::processor::ProcessValue::process_child_values(
                        &mut self.0,
                        __processor,
                        __state,
                    )
                }
            }
        }),
    })
}

fn derive_enum_metastructure(
    s: synstructure::Structure<'_>,
    t: Trait,
) -> Result<TokenStream, synstructure::Structure<'_>> {
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

    let mut is_empty_body = TokenStream::new();
    let mut is_deep_empty_body = TokenStream::new();
    let mut from_value_body = TokenStream::new();
    let mut to_value_body = TokenStream::new();
    let mut process_value_body = TokenStream::new();
    let mut process_child_values_body = TokenStream::new();
    let mut serialize_body = TokenStream::new();
    let mut extract_child_meta_body = TokenStream::new();

    for variant in s.variants() {
        let variant_attrs = parse_variant_attributes(&variant.ast().attrs);
        let variant_name = &variant.ast().ident;
        let tag = variant_attrs
            .tag_override
            .unwrap_or_else(|| variant_name.to_string().to_lowercase());

        (quote! {
            #type_name::#variant_name(__value) => {
                crate::types::Empty::is_empty(__value)
            }
        })
        .to_tokens(&mut is_empty_body);

        (quote! {
            #type_name::#variant_name(__value) => {
                crate::types::Empty::is_deep_empty(__value)
            }
        })
        .to_tokens(&mut is_deep_empty_body);

        if !variant_attrs.fallback_variant {
            let tag = LitStr::new(&tag, Span::call_site());
            (quote! {
                Some(#tag) => {
                    crate::types::FromValue::from_value(crate::types::Annotated(Some(crate::types::Value::Object(__object)), __meta))
                        .map_value(|__value| #type_name::#variant_name(Box::new(__value)))
                }
            }).to_tokens(&mut from_value_body);
            (quote! {
                #type_name::#variant_name(__value) => {
                    let mut __rv = crate::types::ToValue::to_value(__value);
                    if let crate::types::Value::Object(ref mut __object) = __rv {
                        __object.insert(#tag_key_str.to_string(), Annotated::new(crate::types::Value::String(#tag.to_string())));
                    }
                    __rv
                }
            }).to_tokens(&mut to_value_body);
            (quote! {
                #type_name::#variant_name(ref __value) => {
                    let mut __map_ser = ::serde::Serializer::serialize_map(__serializer, None)?;
                    crate::types::ToValue::serialize_payload(__value, ::serde::private::ser::FlatMapSerializer(&mut __map_ser), __behavior)?;
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
                    crate::types::Annotated(Some(#type_name::#variant_name(__object)), __meta)
                }
            })
            .to_tokens(&mut from_value_body);
            (quote! {
                #type_name::#variant_name(__value) => {
                    crate::types::ToValue::to_value(__value)
                }
            })
            .to_tokens(&mut to_value_body);
            (quote! {
                #type_name::#variant_name(ref __value) => {
                    crate::types::ToValue::serialize_payload(__value, __serializer, __behavior)
                }
            })
            .to_tokens(&mut serialize_body);
        }

        (quote! {
            #type_name::#variant_name(ref __value) => {
                crate::types::ToValue::extract_child_meta(__value)
            }
        })
        .to_tokens(&mut extract_child_meta_body);

        (quote! {
            #type_name::#variant_name(__value) => {
                crate::processor::ProcessValue::process_value(
                    __value,
                    __meta,
                    __processor,
                    __state,
                )
            }
        })
        .to_tokens(&mut process_value_body);

        (quote! {
            #type_name::#variant_name(__value) => {
                crate::processor::ProcessValue::process_child_values(
                    __value,
                    __processor,
                    __state,
                )
            }
        })
        .to_tokens(&mut process_child_values_body);
    }

    Ok(match t {
        Trait::Empty => s.gen_impl(quote! {
            #[automatically_derived]
            gen impl crate::types::Empty for @Self {
                fn is_empty(&self) -> bool {
                    match self {
                        #is_empty_body
                    }
                }

                fn is_deep_empty(&self) -> bool {
                    match self {
                        #is_deep_empty_body
                    }
                }
            }
        }),
        Trait::From => {
            s.gen_impl(quote! {
                #[automatically_derived]
                gen impl crate::types::FromValue for @Self {
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
        Trait::To => {
            s.gen_impl(quote! {
                #[automatically_derived]
                gen impl crate::types::ToValue for @Self {
                    fn to_value(self) -> crate::types::Value {
                        match self {
                            #to_value_body
                        }
                    }

                    fn serialize_payload<S>(&self, __serializer: S, __behavior: crate::types::SkipSerialization) -> Result<S::Ok, S::Error>
                    where
                        S: ::serde::ser::Serializer
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
        Trait::Process => {
            let process_value = type_attrs.process_func.map(|func_name| {
                let func_name = Ident::new(&func_name, Span::call_site());
                quote! {
                    if __result == crate::types::ValueAction::Keep {
                        return __processor.#func_name(self, __meta, __state);
                    }
                }
            }).unwrap_or_else(|| {
                quote! {
                    crate::processor::ProcessValue::process_child_values(
                        self,
                        __processor,
                        __state,
                    );
                }
            });

            s.gen_impl(quote! {
                #[automatically_derived]
                gen impl crate::processor::ProcessValue for @Self {
                    #[inline]
                    fn process_value<P>(
                        &mut self,
                        __meta: &mut crate::types::Meta,
                        __processor: &mut P,
                        __state: &crate::processor::ProcessingState<'_>,
                    ) -> crate::types::ValueAction
                    where
                        P: crate::processor::Processor,
                    {
                        let __result = match self {
                            #process_value_body
                        };

                        #process_value

                        __result
                    }

                    #[inline]
                    fn process_child_values<P>(
                        &mut self,
                        __processor: &mut P,
                        __state: &crate::processor::ProcessingState<'_>,
                    )
                    where
                        P: crate::processor::Processor,
                    {
                        match self {
                            #process_child_values_body
                        }
                    }
                }
            })
        }
    })
}

fn derive_metastructure(s: synstructure::Structure<'_>, t: Trait) -> TokenStream {
    let s = match derive_newtype_metastructure(s, t) {
        Ok(stream) => return stream,
        Err(s) => s,
    };

    let mut s = match derive_enum_metastructure(s, t) {
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

    let mut is_empty_body = TokenStream::new();
    let mut is_deep_empty_body = TokenStream::new();
    let mut from_value_body = TokenStream::new();
    let mut to_value_body = TokenStream::new();
    let mut process_child_values_body = TokenStream::new();
    let mut serialize_body = TokenStream::new();
    let mut extract_child_meta_body = TokenStream::new();

    let type_attrs = parse_type_attributes(&s.ast().attrs);
    if type_attrs.tag_key.is_some() {
        panic!("tag_key not supported on structs");
    }

    let mut is_tuple_struct = false;
    for (index, bi) in variant.bindings().iter().enumerate() {
        let field_attrs = parse_field_attributes(index, &bi.ast(), &mut is_tuple_struct);
        let field_attrs_name = Ident::new(&format!("__field_attrs_{}", index), Span::call_site());
        let field_name = field_attrs.field_name.clone();

        let skip_serialization_attr = field_attrs.skip_serialization.as_tokens();

        if field_attrs.additional_properties {
            if is_tuple_struct {
                panic!("additional_properties not allowed in tuple struct");
            }

            (quote! {
                let #bi = __obj.into_iter().map(|(__key, __value)| (__key, crate::types::FromValue::from_value(__value))).collect();
            }).to_tokens(&mut from_value_body);
            (quote! {
                __map.extend(#bi.into_iter().map(|(__key, __value)| (
                    __key,
                    Annotated::map_value(__value, crate::types::ToValue::to_value)
                )));
            })
            .to_tokens(&mut to_value_body);

            let additional_state = if field_attrs.retain {
                quote! {
                    &__state.enter_nothing(
                        Some(::std::borrow::Cow::Borrowed(crate::processor::FieldAttrs::default_retain()))
                    )
                }
            } else {
                quote! { __state }
            };

            (quote! { __processor.process_other(#bi, #additional_state); })
                .to_tokens(&mut process_child_values_body);

            (quote! {
                for (__key, __value) in #bi.iter() {
                    if !__value.skip_serialization(#skip_serialization_attr) {
                        ::serde::ser::SerializeMap::serialize_key(&mut __map_serializer, __key)?;
                        ::serde::ser::SerializeMap::serialize_value(&mut __map_serializer, &crate::types::SerializePayload(__value, #skip_serialization_attr))?;
                    }
                }
            })
            .to_tokens(&mut serialize_body);
            (quote! {
                for (__key, __value) in #bi.iter() {
                    let __inner_tree = crate::types::ToValue::extract_meta_tree(__value);
                    if !__inner_tree.is_empty() {
                        __child_meta.insert(__key.to_string(), __inner_tree);
                    }
                }
            })
            .to_tokens(&mut extract_child_meta_body);
            (quote! { && #bi.values().all(crate::types::Empty::is_empty) })
                .to_tokens(&mut is_empty_body);
            (quote! { && #bi.values().all(|__v| __v.skip_serialization(#skip_serialization_attr)) })
                .to_tokens(&mut is_deep_empty_body);
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
                    let legacy_field_name = LitStr::new(&legacy_alias, Span::call_site());
                    (quote! {
                        let #bi = #bi.or(__obj.remove(#legacy_field_name));
                    })
                    .to_tokens(&mut from_value_body);
                }
            }

            (quote! {
                let #bi = crate::types::FromValue::from_value(#bi.unwrap_or_else(|| crate::types::Annotated(None, crate::types::Meta::default())));
            }).to_tokens(&mut from_value_body);

            if is_tuple_struct {
                (quote! {
                    __arr.push(Annotated::map_value(#bi, crate::types::ToValue::to_value));
                })
                .to_tokens(&mut to_value_body);
                (quote! {
                    if !#bi.skip_serialization(#skip_serialization_attr) {
                        ::serde::ser::SerializeSeq::serialize_element(&mut __seq_serializer, &crate::types::SerializePayload(#bi, #skip_serialization_attr))?;
                    }
                }).to_tokens(&mut serialize_body);
            } else {
                (quote! {
                    __map.insert(#field_name.to_string(), Annotated::map_value(#bi, crate::types::ToValue::to_value));
                }).to_tokens(&mut to_value_body);
                (quote! {
                    if !#bi.skip_serialization(#skip_serialization_attr) {
                        ::serde::ser::SerializeMap::serialize_key(&mut __map_serializer, #field_name)?;
                        ::serde::ser::SerializeMap::serialize_value(&mut __map_serializer, &crate::types::SerializePayload(#bi, #skip_serialization_attr))?;
                    }
                }).to_tokens(&mut serialize_body);
            }

            (quote! {
                let __inner_tree = crate::types::ToValue::extract_meta_tree(#bi);
                if !__inner_tree.is_empty() {
                    __child_meta.insert(#field_name.to_string(), __inner_tree);
                }
            })
            .to_tokens(&mut extract_child_meta_body);

            let enter_state = if is_tuple_struct {
                quote! {
                    __state.enter_index(
                        #index,
                        Some(::std::borrow::Cow::Borrowed(&*#field_attrs_name)),
                        crate::processor::ValueType::for_field(#bi),
                    )
                }
            } else {
                quote! {
                    __state.enter_static(
                        #field_name,
                        Some(::std::borrow::Cow::Borrowed(&*#field_attrs_name)),
                        crate::processor::ValueType::for_field(#bi),
                    )
                }
            };

            let field_attrs_tokens = field_attrs.as_tokens(None);

            (quote! {
                ::lazy_static::lazy_static! {
                    static ref #field_attrs_name: crate::processor::FieldAttrs =
                        #field_attrs_tokens;
                }

                let #bi = crate::processor::process_value(#bi, __processor, &#enter_state);
            })
            .to_tokens(&mut process_child_values_body);

            (quote! { && crate::types::Empty::is_empty(#bi) }).to_tokens(&mut is_empty_body);
            (quote! { && #bi.skip_serialization(#skip_serialization_attr) })
                .to_tokens(&mut is_deep_empty_body);
        }
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
        binding.style = synstructure::BindStyle::RefMut;
    }
    let process_value_pat = variant.pat();
    for binding in variant.bindings_mut() {
        binding.style = synstructure::BindStyle::Ref;
    }
    let serialize_pat = variant.pat();
    let is_empty_pat = variant.pat();

    match t {
        Trait::Empty => s.gen_impl(quote! {
            #[automatically_derived]
            gen impl crate::types::Empty for @Self {
                fn is_empty(&self) -> bool {
                    let #is_empty_pat = self;
                    true #is_empty_body
                }

                fn is_deep_empty(&self) -> bool {
                    let #is_empty_pat = self;
                    true #is_deep_empty_body
                }
            }
        }),
        Trait::From => {
            let valid_match_arm = if is_tuple_struct {
                quote! {
                    crate::types::Annotated(Some(crate::types::Value::Array(mut __arr)), __meta) => {
                        let __arr = __arr.into_iter();
                        #from_value_body;
                        crate::types::Annotated(Some(#to_structure_assemble_pat), __meta)
                    }
                }
            } else {
                quote! {
                    crate::types::Annotated(Some(crate::types::Value::Object(mut __obj)), __meta) => {
                        #from_value_body;
                        crate::types::Annotated(Some(#to_structure_assemble_pat), __meta)
                    }
                }
            };

            s.gen_impl(quote! {
                #[automatically_derived]
                gen impl crate::types::FromValue for @Self {
                    fn from_value(
                        __value: crate::types::Annotated<crate::types::Value>,
                    ) -> crate::types::Annotated<Self> {
                        match __value {
                            #valid_match_arm
                            crate::types::Annotated(None, __meta) => crate::types::Annotated(None, __meta),
                            crate::types::Annotated(Some(__value), mut __meta) => {
                                __meta.add_error(crate::types::Error::expected(#expectation));
                                __meta.set_original_value(Some(__value));
                                crate::types::Annotated(None, __meta)
                            }
                        }
                    }
                }
            })
        }
        Trait::To => {
            let to_value = if is_tuple_struct {
                quote! {
                    let mut __arr = crate::types::Array::new();
                    let #to_value_pat = self;
                    #to_value_body;
                    crate::types::Value::Array(__arr)
                }
            } else {
                quote! {
                    let mut __map = crate::types::Object::new();
                    let #to_value_pat = self;
                    #to_value_body;
                    crate::types::Value::Object(__map)
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

            s.gen_impl(quote! {
                #[automatically_derived]
                gen impl crate::types::ToValue for @Self {
                    fn to_value(self) -> crate::types::Value {
                        #to_value
                    }

                    fn serialize_payload<S>(&self, __serializer: S, __behavior: crate::types::SkipSerialization) -> Result<S::Ok, S::Error>
                    where
                        Self: Sized,
                        S: ::serde::ser::Serializer
                    {
                        let #serialize_pat = *self;
                        #serialize_payload
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
                }
            })
        }
        Trait::Process => {
            let process_value = type_attrs.process_func.map(|func_name| {
                let func_name = Ident::new(&func_name, Span::call_site());
                quote! {
                    fn process_value<P>(
                        &mut self,
                        __meta: &mut crate::types::Meta,
                        __processor: &mut P,
                        __state: &crate::processor::ProcessingState<'_>,
                    ) -> crate::types::ValueAction
                    where
                        P: crate::processor::Processor,
                    {
                        __processor.#func_name(self, __meta, __state)
                    }
                }
            });

            let value_type = type_attrs.value_type.map(|value_name| {
                let value_name = Ident::new(&value_name, Span::call_site());
                quote! {
                    fn value_type(&self) -> Option<crate::processor::ValueType> {
                        Some(crate::processor::ValueType::#value_name)
                    }
                }
            });

            s.gen_impl(quote! {
                #[automatically_derived]
                gen impl crate::processor::ProcessValue for @Self {
                    #value_type
                    #process_value

                    #[inline]
                    fn process_child_values<P>(
                        &mut self,
                        __processor: &mut P,
                        __state: &crate::processor::ProcessingState<'_>
                    )
                    where
                        P: crate::processor::Processor,
                    {
                        let #process_value_pat = self;
                        #process_child_values_body
                    }
                }
            })
        }
    }
}

fn parse_max_chars(name: &str) -> TokenStream {
    match name {
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

#[derive(Default)]
struct TypeAttrs {
    process_func: Option<String>,
    value_type: Option<String>,
    tag_key: Option<String>,
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
                                        panic!("Got non string literal for process_func");
                                    }
                                }
                            } else if ident == "value_type" {
                                match lit {
                                    Lit::Str(litstr) => {
                                        rv.value_type = Some(litstr.value());
                                    }
                                    _ => {
                                        panic!("Got non string literal for value type");
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
    field_name: String,
    required: Option<bool>,
    nonempty: Option<bool>,
    pii: Option<bool>,
    retain: bool,
    match_regex: Option<String>,
    max_chars: Option<TokenStream>,
    bag_size: Option<TokenStream>,
    legacy_aliases: Vec<String>,
    skip_serialization: SkipSerialization,
}

impl FieldAttrs {
    fn as_tokens(&self, inherit_from_field_attrs: Option<TokenStream>) -> TokenStream {
        let field_name = &self.field_name;

        if self.required.is_none() && self.nonempty.is_some() {
            panic!(
                "`required` has to be explicitly set to \"true\" or \"false\" if `nonempty` is used."
            );
        }
        let required = if let Some(required) = self.required {
            quote!(#required)
        } else if let Some(ref parent_attrs) = inherit_from_field_attrs {
            quote!(#parent_attrs.required)
        } else {
            quote!(false)
        };

        let nonempty = if let Some(nonempty) = self.nonempty {
            quote!(#nonempty)
        } else if let Some(ref parent_attrs) = inherit_from_field_attrs {
            quote!(#parent_attrs.nonempty)
        } else {
            quote!(false)
        };

        let pii = if let Some(pii) = self.pii {
            quote!(#pii)
        } else if let Some(ref parent_attrs) = inherit_from_field_attrs {
            quote!(#parent_attrs.pii)
        } else {
            quote!(false)
        };

        let retain = self.retain;

        let max_chars = if let Some(ref max_chars) = self.max_chars {
            quote!(Some(#max_chars))
        } else if let Some(ref parent_attrs) = inherit_from_field_attrs {
            quote!(#parent_attrs.max_chars)
        } else {
            quote!(None)
        };

        let bag_size = if let Some(ref bag_size) = self.bag_size {
            quote!(Some(#bag_size))
        } else if let Some(ref parent_attrs) = inherit_from_field_attrs {
            quote!(#parent_attrs.bag_size)
        } else {
            quote!(None)
        };

        let match_regex = if let Some(ref match_regex) = self.match_regex {
            quote!(Some(
                #[allow(clippy::trivial_regex)]
                ::regex::Regex::new(#match_regex).unwrap()
            ))
        } else if let Some(ref parent_attrs) = inherit_from_field_attrs {
            quote!(#parent_attrs.match_regex.clone())
        } else {
            quote!(None)
        };

        quote!({
            crate::processor::FieldAttrs {
                name: Some(#field_name),
                required: #required,
                nonempty: #nonempty,
                match_regex: #match_regex,
                max_chars: #max_chars,
                bag_size: #bag_size,
                pii: #pii,
                retain: #retain,
            }
        })
    }
}

#[derive(Copy, Clone)]
enum SkipSerialization {
    Never,
    Null(bool),
    Empty(bool),
}

impl SkipSerialization {
    fn as_tokens(self) -> TokenStream {
        match self {
            SkipSerialization::Never => quote!(crate::types::SkipSerialization::Never),
            SkipSerialization::Null(deep) => quote!(crate::types::SkipSerialization::Null(#deep)),
            SkipSerialization::Empty(deep) => quote!(crate::types::SkipSerialization::Empty(#deep)),
        }
    }
}

impl Default for SkipSerialization {
    fn default() -> SkipSerialization {
        SkipSerialization::Null(true)
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
            _ => Err(())?,
        })
    }
}

fn parse_field_attributes(
    index: usize,
    bi_ast: &syn::Field,
    is_tuple_struct: &mut bool,
) -> FieldAttrs {
    if bi_ast.ident.is_none() {
        *is_tuple_struct = true;
    } else if *is_tuple_struct {
        panic!("invalid tuple struct");
    }

    let mut rv = FieldAttrs::default();
    rv.field_name = bi_ast
        .ident
        .as_ref()
        .map(|ident| ident.to_string())
        .unwrap_or_else(|| index.to_string());

    for attr in &bi_ast.attrs {
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
                                        rv.field_name = litstr.value();
                                    }
                                    _ => {
                                        panic!("Got non string literal for field");
                                    }
                                }
                            } else if ident == "required" {
                                match lit {
                                    Lit::Str(litstr) => match litstr.value().as_str() {
                                        "true" => rv.required = Some(true),
                                        "false" => rv.required = Some(false),
                                        other => panic!("Unknown value {}", other),
                                    },
                                    _ => {
                                        panic!("Got non string literal for required");
                                    }
                                }
                            } else if ident == "nonempty" {
                                match lit {
                                    Lit::Str(litstr) => match litstr.value().as_str() {
                                        "true" => rv.nonempty = Some(true),
                                        "false" => rv.nonempty = Some(false),
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
                                        rv.max_chars = Some(quote!(#attr));
                                    }
                                    _ => {
                                        panic!("Got non string literal for max_chars");
                                    }
                                }
                            } else if ident == "bag_size" {
                                match lit {
                                    Lit::Str(litstr) => {
                                        let attr = parse_bag_size(litstr.value().as_str());
                                        rv.bag_size = Some(quote!(#attr));
                                    }
                                    _ => {
                                        panic!("Got non string literal for bag_size");
                                    }
                                }
                            } else if ident == "pii" {
                                match lit {
                                    Lit::Str(litstr) => match litstr.value().as_str() {
                                        "true" => rv.pii = Some(true),
                                        "false" => rv.pii = Some(false),
                                        other => panic!("Unknown value {}", other),
                                    },
                                    _ => {
                                        panic!("Got non string literal for pii");
                                    }
                                }
                            } else if ident == "retain" {
                                match lit {
                                    Lit::Str(litstr) => match litstr.value().as_str() {
                                        "true" => rv.retain = true,
                                        "false" => rv.retain = false,
                                        other => panic!("Unknown value {}", other),
                                    },
                                    _ => {
                                        panic!("Got non string literal for retain");
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
                            } else if ident == "skip_serialization" {
                                match lit {
                                    Lit::Str(litstr) => {
                                        rv.skip_serialization = FromStr::from_str(&litstr.value())
                                            .expect("Unknown value for skip_serialization");
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
