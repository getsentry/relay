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

decl_derive!([MetaStructure, attributes(metastructure)] => process_metastructure);


fn process_metastructure(s: synstructure::Structure) -> TokenStream {
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
                let #bi = __obj.into_iter().map(|(__key, __value)| (__key, __processor::MetaStructure::from_value(__value))).collect();
            }).to_tokens(&mut to_structure_body);
            (quote! {
                __map.extend(#bi.into_iter().map(|(__key, __value)| (__key, __processor::MetaStructure::to_value(__value))));
            }).to_tokens(&mut to_value_body);
            (quote! {
                let #bi = #bi.into_iter().map(|(__key, __value)| {
                    let __value = __processor::MetaStructure::process(__value, __processor, __state.enter_borrowed(__key.as_str(), None));
                    (__key, __value)
                }).collect();
            }).to_tokens(&mut process_body);
            (quote! {
                for (__key, __value) in #bi.iter() {
                    if !__value.skip_serialization() {
                        __map_serializer.serialize_key(__key)?;
                        __map_serializer.serialize_value(&__processor::SerializeMetaStructurePayload(__value))?;
                    }
                }
            }).to_tokens(&mut serialize_body);
            (quote! {
                for (__key, __value) in #bi.iter() {
                    let __inner_tree = __processor::MetaStructure::extract_meta_tree(__value);
                    if !__inner_tree.is_empty() {
                        __meta_tree.children.insert(__key.to_string(), __inner_tree);
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
                        __processor::MetaStructure::from_value(
                            __canonical_value.or(__legacy_value).unwrap_or_else(|| __meta::Annotated(None, __meta::Meta::default())))
                    };
                }).to_tokens(&mut to_structure_body);
            } else {
                (quote! {
                    let #bi = __processor::MetaStructure::from_value(
                        __obj.remove(#field_name).unwrap_or_else(|| __meta::Annotated(None, __meta::Meta::default())));
                }).to_tokens(&mut to_structure_body);
            }

            if required {
                (quote! {
                    let #bi = #bi.require_value();
                }).to_tokens(&mut to_structure_body);
            }
            (quote! {
                __map.insert(#field_name.to_string(), __processor::MetaStructure::to_value(#bi));
            }).to_tokens(&mut to_value_body);
            (quote! {
                const #field_attrs_name: __processor::FieldAttrs = __processor::FieldAttrs {
                    name: Some(#field_name),
                    required: #required_attr,
                    cap_size: #cap_size_attr,
                    pii_kind: #pii_kind_attr,
                };
                let #bi = __processor::MetaStructure::process(#bi, __processor, __state.enter_static(#field_name, Some(::std::borrow::Cow::Borrowed(&#field_attrs_name))));
            }).to_tokens(&mut process_body);
            (quote! {
                __map_serializer.serialize_key(#field_name)?;
                __map_serializer.serialize_value(&__processor::SerializeMetaStructurePayload(#bi))?;
            }).to_tokens(&mut serialize_body);
            (quote! {
                let __inner_tree = __processor::MetaStructure::extract_meta_tree(#bi);
                if !__inner_tree.is_empty() {
                    __meta_tree.children.insert(#field_name.to_string(), __inner_tree);
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

    s.gen_impl(quote! {
        use processor as __processor;
        use types as __types;
        use meta as __meta;
        extern crate serde;
        use serde::ser::SerializeMap;

        gen impl __processor::MetaStructure for @Self {
            fn from_value(
                __value: __meta::Annotated<__meta::Value>,
            ) -> __meta::Annotated<Self> {
                match __value {
                    __meta::Annotated(Some(__meta::Value::Object(mut __obj)), __meta) => {
                        #to_structure_body;
                        __meta::Annotated(Some(#to_structure_assemble_pat), __meta)
                    }
                    __meta::Annotated(None, __meta) => {
                        // TODO: hook?
                        __meta::Annotated(None, __meta)
                    }
                    __meta::Annotated(_, mut __meta) => {
                        __meta.add_error(#expectation.to_string());
                        __meta::Annotated(None, __meta)
                    }
                }
            }

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

            fn serialize_payload<S>(__value: &__meta::Annotated<Self>, __serializer: S) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: ::serde::ser::Serializer
            {
                let __meta::Annotated(__value, _) = __value;
                if let Some(__value) = __value {
                    let #serialize_pat = __value;
                    let mut __map_serializer = __serializer.serialize_map(None)?;
                    #serialize_body;
                    __map_serializer.end()
                } else {
                    __serializer.serialize_unit()
                }
            }

            fn extract_meta_tree(__value: &__meta::Annotated<Self>) -> __meta::MetaTree
            where
                Self: Sized,
            {
                let &__meta::Annotated(ref __value, ref __meta) = __value;
                let mut __meta_tree = __meta::MetaTree {
                    meta: __meta.clone(),
                    children: Default::default(),
                };
                if let Some(__value) = __value {
                    let #serialize_pat = __value;
                    #extract_meta_tree_body;
                }
                __meta_tree
            }

            fn process<P: __processor::Processor>(
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

fn parse_cap_size(name: &str) -> TokenStream {
    match name {
        "enumlike" => quote!(__processor::CapSize::EnumLike),
        "summary" => quote!(__processor::CapSize::Summary),
        "message" => quote!(__processor::CapSize::Message),
        "symbol" => quote!(__processor::CapSize::Symbol),
        "path" => quote!(__processor::CapSize::Path),
        "shortpath" => quote!(__processor::CapSize::ShortPath),
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
