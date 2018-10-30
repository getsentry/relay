#![recursion_limit = "256"]
extern crate syn;

#[macro_use]
extern crate synstructure;
#[macro_use]
extern crate quote;
extern crate proc_macro2;

use proc_macro2::{TokenStream, Span};
use quote::ToTokens;
use syn::{Lit, Meta, MetaNameValue, NestedMeta, LitStr, LitBool, Ident};

#[derive(Debug, Clone, Copy)]
enum Trait {
    FromValue,
    ToValue,
    ProcessValue,
}

decl_derive!([ToValue, attributes(metastructure)] => process_to_value);
decl_derive!([FromValue, attributes(metastructure)] => process_from_value);
decl_derive!([ProcessValue, attributes(metastructure)] => process_process_value);

fn process_to_value(s: synstructure::Structure) -> TokenStream {
    process_metastructure_impl(s, Trait::ToValue)
}

fn process_from_value(s: synstructure::Structure) -> TokenStream {
    process_metastructure_impl(s, Trait::FromValue)
}

fn process_process_value(s: synstructure::Structure) -> TokenStream {
    process_metastructure_impl(s, Trait::ProcessValue)
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

    if let Some(_) = s.variants()[0].bindings()[0].ast().ident {
        // The variant has a name
        // e.g. `struct Foo { bar: Bar }` instead of `struct Foo(Bar)`
        return Err(s);
    }

    // At this point we know we have a struct of the form:
    // struct Foo(Bar)
    //
    // Those structs get special treatment: Bar does not need to be wrapped in Annnotated, and no
    // #[process_annotated_value] is necessary on the value.
    //
    // It basically works like a type alias.

    // Last check: It's a programming error to add `#[process_annotated_value]` to the single
    // field, because it does not do anything.
    //
    // e.g.
    // `struct Foo(#[process_annotated_value] Annnotated<Bar>)` is useless
    // `struct Foo(Bar)` is how it's supposed to be used
    for attr in &s.variants()[0].bindings()[0].ast().attrs {
        if let Some(meta) = attr.interpret_meta() {
            if meta.name() == "process_annotated_value" {
                panic!("Single-field tuple structs are treated as type aliases");
            }
        }
    }

    let name = &s.ast().ident;

    Ok(match t {
        Trait::FromValue => {
            s.gen_impl(quote! {
                use processor as __processor;
                use meta as __meta;

                gen impl __processor::FromValue for @Self {
                    #[inline(always)]
                    fn from_value(
                        __value: __meta::Annotated<__meta::Value>,
                    ) -> __meta::Annotated<Self> {
                        match __processor::FromValue::from_value(__value) {
                            Annotated(Some(__value), __meta) => Annotated(Some(#name(__value)), __meta),
                            Annotated(None, __meta) => Annotated(None, __meta),
                        }
                    }
                }
            })
        }
        Trait::ToValue => {
            s.gen_impl(quote! {
                use processor as __processor;
                use types as __types;
                use meta as __meta;
                extern crate serde as __serde;

                gen impl __processor::ToValue for @Self {
                    #[inline(always)]
                    fn to_value(
                        mut __value: __meta::Annotated<Self>
                    ) -> __meta::Annotated<__meta::Value> {
                        __processor::ToValue::to_value(Annotated(__value.0.take(), __value.1))
                    }

                    #[inline(always)]
                    fn serialize_payload<S>(&self, __serializer: S) -> Result<S::Ok, S::Error>
                    where
                        Self: Sized,
                        S: __serde::ser::Serializer
                    {
                        __processor::ToValue::serialize_payload(&self.0, __serializer)
                    }

                    #[inline(always)]
                    fn extract_child_meta(&self) -> __meta::MetaMap
                    where
                        Self: Sized,
                    {
                        __processor::ToValue::extract_child_meta(&self.0)
                    }
                }
            })
        }
        Trait::ProcessValue => {
            s.gen_impl(quote! {
                use processor as __processor;
                use meta as __meta;

                gen impl __processor::ProcessValue for @Self {
                    #[inline(always)]
                    fn process_value<P: __processor::Processor>(
                        __value: __meta::Annotated<Self>,
                        __processor: &P,
                        __state: __processor::ProcessingState
                    ) -> __meta::Annotated<Self> {
                        let __new_annotated = match __value {
                            __meta::Annotated(Some(__value), __meta) => {
                                __processor::ProcessValue::process_value(
                                    __meta::Annotated(Some(__value.0), __meta), __processor, __state)
                            }
                            __meta::Annotated(None, __meta) => __meta::Annotated(None, __meta)
                        };
                        match __new_annotated {
                            __meta::Annotated(Some(__value), __meta) => __meta::Annotated(Some(#name(__value)), __meta),
                            __meta::Annotated(None, __meta) => __meta::Annotated(None, __meta)
                        }
                    }
                }
            })
        }
    })
}

fn process_metastructure_impl(s: synstructure::Structure, t: Trait) -> TokenStream {
    let mut s = match process_wrapper_struct_derive(s, t) {
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
    let mut to_structure_body = TokenStream::new();
    let mut to_value_body = TokenStream::new();
    let mut process_body = TokenStream::new();
    let mut serialize_body = TokenStream::new();
    let mut extract_meta_tree_body = TokenStream::new();
    let mut process_func = None;
    let mut tmp_idx = 0;

    for attr in &s.ast().attrs {
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
                                        process_func = Some(litstr.value());
                                    }
                                    _ => {
                                        panic!("Got non string literal for field");
                                    }
                                }
                            } else {
                                panic!("Unknown attribute")
                            }
                        }
                        _ => panic!("Unsupported attribute")
                    }
                }
            }
        }
    }

    for bi in variant.bindings() {
        let mut additional_properties = false;
        let mut field_name = bi.ast().ident.as_ref().unwrap().to_string();
        let mut cap_size_attr = quote!(None);
        let mut pii_kind_attr = quote!(None);
        let mut required = false;
        let mut legacy_alias = None;
        for attr in &bi.ast().attrs {
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
                                    additional_properties = true;
                                } else {
                                    panic!("Unknown attribute {}", ident);
                                }
                            }
                            Meta::NameValue(MetaNameValue { ident, lit, .. }) => {
                                if ident == "field" {
                                    match lit {
                                        Lit::Str(litstr) => {
                                            field_name = litstr.value();
                                        }
                                        _ => {
                                            panic!("Got non string literal for field");
                                        }
                                    }
                                } else if ident == "required" {
                                    match lit {
                                        Lit::Str(litstr) => {
                                            match litstr.value().as_str() {
                                                "true" => required = true,
                                                "false" => required = false,
                                                other => panic!("Unknown value {}", other),
                                            }
                                        }
                                        _ => {
                                            panic!("Got non string literal for required");
                                        }
                                    }
                                } else if ident == "cap_size" {
                                    match lit {
                                        Lit::Str(litstr) => {
                                            let attr = parse_cap_size(litstr.value().as_str());
                                            cap_size_attr = quote!(Some(#attr));
                                        }
                                        _ => {
                                            panic!("Got non string literal for cap_size");
                                        }
                                    }
                                } else if ident == "pii_kind" {
                                    match lit {
                                        Lit::Str(litstr) => {
                                            let attr = parse_pii_kind(litstr.value().as_str());
                                            pii_kind_attr = quote!(Some(#attr));
                                        }
                                        _ => {
                                            panic!("Got non string literal for pii_kind");
                                        }
                                    }
                                } else if ident == "legacy_alias" {
                                    match lit {
                                        Lit::Str(litstr) => {
                                            legacy_alias = Some(litstr.value());
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

        let field_name = LitStr::new(&field_name, Span::call_site());

        if additional_properties {
            (quote! {
                let #bi = __obj.into_iter().map(|(__key, __value)| (__key, __processor::FromValue::from_value(__value))).collect();
            }).to_tokens(&mut to_structure_body);
            (quote! {
                __map.extend(#bi.into_iter().map(|(__key, __value)| (__key, __processor::ToValue::to_value(__value))));
            }).to_tokens(&mut to_value_body);
            (quote! {
                let #bi = #bi.into_iter().map(|(__key, __value)| {
                    let __value = __processor::ProcessValue::process_value(__value, __processor, __state.enter_borrowed(__key.as_str(), None));
                    (__key, __value)
                }).collect();
            }).to_tokens(&mut process_body);
            (quote! {
                for (__key, __value) in #bi.iter() {
                    if !__value.skip_serialization() {
                        __serde::ser::SerializeMap::serialize_key(&mut __map_serializer, __key)?;
                        __serde::ser::SerializeMap::serialize_value(&mut __map_serializer, &__processor::SerializePayload(__value))?;
                    }
                }
            }).to_tokens(&mut serialize_body);
            (quote! {
                for (__key, __value) in #bi.iter() {
                    let __inner_tree = __processor::ToValue::extract_meta_tree(__value);
                    if !__inner_tree.is_empty() {
                        __child_meta.insert(__key.to_string(), __inner_tree);
                    }
                }
            }).to_tokens(&mut extract_meta_tree_body);
        } else {
            let field_attrs_name = Ident::new(&format!("__field_attrs_{}", {tmp_idx += 1; tmp_idx }), Span::call_site());
            let required_attr = LitBool {
                value: required,
                span: Span::call_site()
            };

            if let Some(legacy_alias) = legacy_alias {
                let legacy_field_name = LitStr::new(&legacy_alias, Span::call_site());
                (quote! {
                    let #bi = {
                        let __legacy_value = __obj.remove(#legacy_field_name);
                        let __canonical_value = __obj.remove(#field_name);
                        __processor::FromValue::from_value(
                            __canonical_value.or(__legacy_value).unwrap_or_else(|| __meta::Annotated(None, __meta::Meta::default())))
                    };
                }).to_tokens(&mut to_structure_body);
            } else {
                (quote! {
                    let #bi = __processor::FromValue::from_value(
                        __obj.remove(#field_name).unwrap_or_else(|| __meta::Annotated(None, __meta::Meta::default())));
                }).to_tokens(&mut to_structure_body);
            }

            if required {
                (quote! {
                    let #bi = #bi.require_value();
                }).to_tokens(&mut to_structure_body);
            }
            (quote! {
                __map.insert(#field_name.to_string(), __processor::ToValue::to_value(#bi));
            }).to_tokens(&mut to_value_body);
            (quote! {
                const #field_attrs_name: __processor::FieldAttrs = __processor::FieldAttrs {
                    name: Some(#field_name),
                    required: #required_attr,
                    cap_size: #cap_size_attr,
                    pii_kind: #pii_kind_attr,
                };
                let #bi = __processor::ProcessValue::process_value(#bi, __processor, __state.enter_static(#field_name, Some(::std::borrow::Cow::Borrowed(&#field_attrs_name))));
            }).to_tokens(&mut process_body);
            (quote! {
                if !#bi.skip_serialization() {
                    __serde::ser::SerializeMap::serialize_key(&mut __map_serializer, #field_name)?;
                    __serde::ser::SerializeMap::serialize_value(&mut __map_serializer, &__processor::SerializePayload(#bi))?;
                }
            }).to_tokens(&mut serialize_body);
            (quote! {
                let __inner_tree = __processor::ToValue::extract_meta_tree(#bi);
                if !__inner_tree.is_empty() {
                    __child_meta.insert(#field_name.to_string(), __inner_tree);
                }
            }).to_tokens(&mut extract_meta_tree_body);
        }
    }

    let ast = s.ast();
    let expectation = LitStr::new(&format!("expected {}", ast.ident.to_string().to_lowercase()), Span::call_site());
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

    let invoke_process_func = process_func.map(|func_name| {
        let func_name = Ident::new(&func_name, Span::call_site());
        quote! {
            let __result = __processor.#func_name(__result, __state);
        }
    });

    match t {
        Trait::FromValue => {
            s.gen_impl(quote! {
                use processor as __processor;
                use meta as __meta;

                gen impl __processor::FromValue for @Self {
                    fn from_value(
                        __value: __meta::Annotated<__meta::Value>,
                    ) -> __meta::Annotated<Self> {
                        match __value {
                            __meta::Annotated(Some(__meta::Value::Object(mut __obj)), __meta) => {
                                #to_structure_body;
                                __meta::Annotated(Some(#to_structure_assemble_pat), __meta)
                            }
                            __meta::Annotated(None, __meta) => __meta::Annotated(None, __meta),
                            __meta::Annotated(Some(__value), mut __meta) => {
                                __meta.add_unexpected_value_error(#expectation, __value);
                                __meta::Annotated(None, __meta)
                            }
                        }
                    }
                }
            })
        }
        Trait::ToValue => {
            s.gen_impl(quote! {
                use processor as __processor;
                use types as __types;
                use meta as __meta;
                extern crate serde as __serde;

                gen impl __processor::ToValue for @Self {
                    fn to_value(
                        __value: __meta::Annotated<Self>
                    ) -> __meta::Annotated<__meta::Value> {
                        let __meta::Annotated(__value, __meta) = __value;
                        if let Some(__value) = __value {
                            let mut __map = __types::Object::new();
                            let #to_value_pat = __value;
                            #to_value_body;
                            __meta::Annotated(Some(__meta::Value::Object(__map)), __meta)
                        } else {
                            __meta::Annotated(None, __meta)
                        }
                    }

                    fn serialize_payload<S>(&self, __serializer: S) -> Result<S::Ok, S::Error>
                    where
                        Self: Sized,
                        S: __serde::ser::Serializer
                    {
                        let #serialize_pat = *self;
                        let mut __map_serializer = __serializer.serialize_map(None)?;
                        #serialize_body;
                        __serde::ser::SerializeMap::end(__map_serializer)
                    }

                    fn extract_child_meta(&self) -> __meta::MetaMap
                    where
                        Self: Sized,
                    {
                        let mut __child_meta = __meta::MetaMap::new();
                        let #serialize_pat = *self;
                        #extract_meta_tree_body;
                        __child_meta
                    }
                }
            })
        }
        Trait::ProcessValue => {
            s.gen_impl(quote! {
                use processor as __processor;
                use meta as __meta;

                gen impl __processor::ProcessValue for @Self {
                    fn process_value<P: __processor::Processor>(
                        __value: __meta::Annotated<Self>,
                        __processor: &P,
                        __state: __processor::ProcessingState
                    ) -> __meta::Annotated<Self> {
                        let __meta::Annotated(__value, __meta) = __value;
                        let __result = if let Some(__value) = __value {
                            let #to_value_pat = __value;
                            #process_body;
                            __meta::Annotated(Some(#to_structure_assemble_pat), __meta)
                        } else {
                            __meta::Annotated(None, __meta)
                        };
                        #invoke_process_func
                        __result
                    }
                }
            })
        }
    }
}

fn parse_cap_size(name: &str) -> TokenStream {
    match name {
        "enumlike" => quote!(__processor::CapSize::EnumLike),
        "summary" => quote!(__processor::CapSize::Summary),
        "message" => quote!(__processor::CapSize::Message),
        "symbol" => quote!(__processor::CapSize::Symbol),
        "path" => quote!(__processor::CapSize::Path),
        "short_path" => quote!(__processor::CapSize::ShortPath),
        _ => panic!("invalid cap_size variant '{}'", name),
    }
}

fn parse_pii_kind(kind: &str) -> TokenStream {
    match kind {
        "freeform" => quote!(__processor::PiiKind::Freeform),
        "ip" => quote!(__processor::PiiKind::Ip),
        "id" => quote!(__processor::PiiKind::Id),
        "username" => quote!(__processor::PiiKind::Username),
        "hostname" => quote!(__processor::PiiKind::Hostname),
        "sensitive" => quote!(__processor::PiiKind::Sensitive),
        "name" => quote!(__processor::PiiKind::Name),
        "email" => quote!(__processor::PiiKind::Email),
        "location" => quote!(__processor::PiiKind::Location),
        "databag" => quote!(__processor::PiiKind::Databag),
        _ => panic!("invalid pii_kind variant '{}'", kind),
    }
}
