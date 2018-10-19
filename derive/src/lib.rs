#![recursion_limit = "256"]
extern crate syn;

#[macro_use]
extern crate synstructure;
#[macro_use]
extern crate quote;
extern crate proc_macro2;

use proc_macro2::{TokenStream, Span};
use quote::ToTokens;
use syn::{Lit, Meta, MetaNameValue, NestedMeta, LitStr, Ident};

decl_derive!([MetaStructure, attributes(metastructure)] => process_metastructure);

/*
fn process_wrapper_struct_derive(
    s: synstructure::Structure,
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
    // Those structs get special treatment: Bar does not need to be wrapped in Annotated, and no
    // #[metastructure] is necessary on the value.
    //
    // It basically works like a type alias.

    // Last check: It's a programming error to add `#[metastructure]` to the single
    // field, because it does not do anything.
    //
    // e.g.
    // `struct Foo(#[metastructure] Annotated<Bar>)` is useless
    // `struct Foo(Bar)` is how it's supposed to be used
    for attr in &s.variants()[0].bindings()[0].ast().attrs {
        if let Some(meta) = attr.interpret_meta() {
            if meta.name() == "metastructure" {
                panic!("Single-field tuple structs are treated as type aliases");
            }
        }
    }

    let name = &s.ast().ident;

    Ok(s.gen_impl(quote! {
        use processor as __processor;
        use protocol as __meta;

        gen impl __processor::ProcessAnnotatedValue for @Self {
            fn metastructure(
                __annotated: __meta::Annotated<Self>,
                __processor: &__processor::Processor,
                __info: &__processor::ValueInfo
            ) -> __meta::Annotated<Self> {
                __processor::ProcessAnnotatedValue::metastructure(
                    __annotated.map(|x| x.0),
                    __processor,
                    __info
                ).map(#name)
            }
        }
    }))
}
*/

fn process_metastructure(s: synstructure::Structure) -> TokenStream {
    /*
    let s = match process_wrapper_struct_derive(s) {
        Ok(stream) => return stream,
        Err(s) => s,
    };
    */

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
    let mut process_func = None;

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
        let mut required = false;
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
                                            panic!("Got non bool literal for required");
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
                    let __value = __processor::MetaStructure::process(__value, __processor, __state.enter(&__key));
                    (__key, __value)
                }).collect();
            }).to_tokens(&mut process_body);
            (quote! {
                for (__key, __value) in #bi.iter() {
                    __map_serializer.serialize_key(__key)?;
                    __map_serializer.serialize_value(&__processor::SerializeMetaStructurePayload(__value))?;
                }
            }).to_tokens(&mut serialize_body);
        } else {
            (quote! {
                let #bi = __processor::MetaStructure::from_value(
                    __obj.remove(#field_name).unwrap_or_else(|| __meta::Annotated(None, __meta::Meta::default())));
            }).to_tokens(&mut to_structure_body);
            if required {
                (quote! {
                    let #bi = #bi.require_value();
                }).to_tokens(&mut to_structure_body);
            }
            (quote! {
                __map.insert(#field_name.to_string(), __processor::MetaStructure::to_value(#bi));
            }).to_tokens(&mut to_value_body);
            (quote! {
                let #bi = __processor::MetaStructure::process(#bi, __processor, __state.enter(#field_name));
            }).to_tokens(&mut process_body);
            (quote! {
                __map_serializer.serialize_key(#field_name)?;
                __map_serializer.serialize_value(&__processor::SerializeMetaStructurePayload(#bi))?;
            }).to_tokens(&mut serialize_body);
        }
    }

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
                    __meta::Annotated(_, __meta) => {
                        // TODO: add error
                        __meta::Annotated(None, __meta)
                    }
                }
            }

            fn to_value(
                __value: __meta::Annotated<Self>
            ) -> __meta::Annotated<__meta::Value> {
                let __meta::Annotated(__value, __meta) = __value;
                if let Some(__value) = __value {
                    let mut __map = ::std::collections::BTreeMap::new();
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
