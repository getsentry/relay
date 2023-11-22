//! Derive for visitor traits on the Event schema.
//!
//! This derive provides the `ProcessValue` trait. It must be used within the `relay-event-schema`
//! crate.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![recursion_limit = "256"]

use std::str::FromStr;

use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};
use syn::{Ident, Lit, Meta, NestedMeta};
use synstructure::decl_derive;

decl_derive!(
    [ProcessValue, attributes(metastructure)] =>
    /// Derive the `ProcessValue` trait.
    derive_process_value
);

fn derive_process_value(mut s: synstructure::Structure<'_>) -> TokenStream {
    let _ = s.bind_with(|_bi| synstructure::BindStyle::RefMut);
    let _ = s.add_bounds(synstructure::AddBounds::Generics);

    let type_attrs = parse_type_attributes(&s);
    let process_func_call_tokens = type_attrs.process_func_call_tokens();

    let process_value_arms = s.each_variant(|variant| {
        if is_newtype(variant) {
            // Process variant twice s.t. both processor functions are called.
            //
            // E.g.:
            // - For `Value::String`, call `process_string` as well as `process_value`.
            // - For `LenientString`, call `process_lenient_string` (not a thing.. yet) as well as
            // `process_string`.

            let bi = &variant.bindings()[0];
            let ident = &bi.binding;
            let field_attrs = parse_field_attributes(0, bi.ast(), &mut true);
            let field_attrs_tokens = field_attrs.as_tokens(Some(quote!(parent_attrs)));

            quote! {
                let parent_attrs = __state.attrs();
                let attrs = #field_attrs_tokens;
                let __state = &__state.enter_nothing(
                    Some(::std::borrow::Cow::Owned(attrs))
                );

                // This is a copy of `funcs::process_value`, due to ownership issues. In particular
                // we want to pass the same meta twice.
                //
                // NOTE: Handling for ProcessingAction is slightly different (early-return). This
                // should be fine though.
                let action = __processor.before_process(
                    Some(&*#ident),
                    __meta,
                    &__state
                )?;

                crate::processor::ProcessValue::process_value(
                    #ident,
                    __meta,
                    __processor,
                    &__state
                )?;

                __processor.after_process(
                    Some(#ident),
                    __meta,
                    &__state
                )?;
            }
        } else {
            quote!()
        }
    });

    let process_child_values_arms = s.each_variant(|variant| {
        let mut is_tuple_struct = false;

        if is_newtype(variant) {
            // `process_child_values` has to be a noop because otherwise we recurse into the
            // subtree twice due to the weird `process_value` impl

            return quote!();
        }

        let mut body = TokenStream::new();
        for (index, bi) in variant.bindings().iter().enumerate() {
            let field_attrs = parse_field_attributes(index, bi.ast(), &mut is_tuple_struct);
            let ident = &bi.binding;
            let field_attrs_name = Ident::new(&format!("FIELD_ATTRS_{index}"), Span::call_site());
            let field_name = field_attrs.field_name.clone();

            let field_attrs_tokens = field_attrs.as_tokens(None);

            (quote! {
                static #field_attrs_name: crate::processor::FieldAttrs = #field_attrs_tokens;
            })
            .to_tokens(&mut body);

            let enter_state = if field_attrs.additional_properties {
                if is_tuple_struct {
                    panic!("additional_properties not allowed in tuple struct");
                }

                quote! {
                    __state.enter_nothing(Some(::std::borrow::Cow::Borrowed(&#field_attrs_name)))
                }
            } else if is_tuple_struct {
                quote! {
                    __state.enter_index(
                        #index,
                        Some(::std::borrow::Cow::Borrowed(&#field_attrs_name)),
                        crate::processor::ValueType::for_field(#ident),
                    )
                }
            } else {
                quote! {
                    __state.enter_static(
                        #field_name,
                        Some(::std::borrow::Cow::Borrowed(&#field_attrs_name)),
                        crate::processor::ValueType::for_field(#ident),
                    )
                }
            };

            if field_attrs.additional_properties {
                (quote! {
                    __processor.process_other(#ident, &#enter_state)?;
                })
                .to_tokens(&mut body);
            } else {
                (quote! {
                    crate::processor::process_value(#ident, __processor, &#enter_state)?;
                })
                .to_tokens(&mut body);
            }
        }

        quote!({ #body })
    });

    let _ = s.bind_with(|_bi| synstructure::BindStyle::Ref);

    let value_type_arms = s.each_variant(|variant| {
        if !type_attrs.value_type.is_empty() {
            let value_names = type_attrs
                .value_type
                .iter()
                .map(|value_name| Ident::new(value_name, Span::call_site()));
            quote! {
                // enumset produces a deprecation warning because it thinks we use its internals
                // directly, but we do actually use the macro
                #[allow(deprecated)]
                {
                    enumset::enum_set!( #(crate::processor::ValueType::#value_names)|* )
                }
            }
        } else if is_newtype(variant) {
            let bi = &variant.bindings()[0];
            let ident = &bi.binding;
            quote!(crate::processor::ProcessValue::value_type(#ident))
        } else {
            quote!(enumset::EnumSet::empty())
        }
    });

    s.gen_impl(quote! {
        #[automatically_derived]
        gen impl crate::processor::ProcessValue for @Self {
            fn value_type(&self) -> enumset::EnumSet<crate::processor::ValueType> {
                match *self {
                    #value_type_arms
                }
            }

            fn process_value<P>(
                &mut self,
                __meta: &mut relay_protocol::Meta,
                __processor: &mut P,
                __state: &crate::processor::ProcessingState<'_>,
            ) -> crate::processor::ProcessingResult
            where
                P: crate::processor::Processor,
            {
                #process_func_call_tokens;
                match *self {
                    #process_value_arms
                }

                Ok(())
            }

            #[inline]
            fn process_child_values<P>(
                &mut self,
                __processor: &mut P,
                __state: &crate::processor::ProcessingState<'_>
            ) -> crate::processor::ProcessingResult
            where
                P: crate::processor::Processor,
            {
                match *self {
                    #process_child_values_arms
                }

                Ok(())
            }
        }
    })
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
        "distribution" => quote!(crate::processor::MaxChars::Distribution),
        _ => panic!("invalid max_chars variant '{name}'"),
    }
}

fn parse_bag_size(name: &str) -> TokenStream {
    match name {
        "small" => quote!(crate::processor::BagSize::Small),
        "medium" => quote!(crate::processor::BagSize::Medium),
        "large" => quote!(crate::processor::BagSize::Large),
        "larger" => quote!(crate::processor::BagSize::Larger),
        "massive" => quote!(crate::processor::BagSize::Massive),
        _ => panic!("invalid bag_size variant '{name}'"),
    }
}

#[derive(Default)]
struct TypeAttrs {
    process_func: Option<String>,
    value_type: Vec<String>,
}

impl TypeAttrs {
    fn process_func_call_tokens(&self) -> TokenStream {
        if let Some(ref func_name) = self.process_func {
            let func_name = Ident::new(func_name, Span::call_site());
            quote! {
                __processor.#func_name(self, __meta, __state)?;
            }
        } else {
            quote! {
                self.process_child_values(__processor, __state)?;
            }
        }
    }
}

fn parse_type_attributes(s: &synstructure::Structure<'_>) -> TypeAttrs {
    let mut rv = TypeAttrs::default();

    for attr in &s.ast().attrs {
        let meta = match attr.parse_meta() {
            Ok(meta) => meta,
            Err(_) => continue,
        };

        let ident = match meta.path().get_ident() {
            Some(x) => x,
            None => continue,
        };

        if ident != "metastructure" {
            continue;
        }

        if let Meta::List(metalist) = meta {
            for nested_meta in metalist.nested {
                match nested_meta {
                    NestedMeta::Lit(..) => panic!("unexpected literal attribute"),
                    NestedMeta::Meta(meta) => match meta {
                        Meta::NameValue(name_value) => {
                            let ident = name_value.path.get_ident().expect("Unexpected path");

                            if ident == "process_func" {
                                match name_value.lit {
                                    Lit::Str(litstr) => {
                                        rv.process_func = Some(litstr.value());
                                    }
                                    _ => {
                                        panic!("Got non string literal for process_func");
                                    }
                                }
                            } else if ident == "value_type" {
                                match name_value.lit {
                                    Lit::Str(litstr) => {
                                        rv.value_type.push(litstr.value());
                                    }
                                    _ => {
                                        panic!("Got non string literal for value type");
                                    }
                                }
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

#[derive(Copy, Clone, Debug)]
enum Pii {
    True,
    False,
    Maybe,
}

impl Pii {
    fn as_tokens(self) -> TokenStream {
        match self {
            Pii::True => quote!(crate::processor::Pii::True),
            Pii::False => quote!(crate::processor::Pii::False),
            Pii::Maybe => quote!(crate::processor::Pii::Maybe),
        }
    }
}

#[derive(Default)]
struct FieldAttrs {
    additional_properties: bool,
    omit_from_schema: bool,
    field_name: String,
    required: Option<bool>,
    nonempty: Option<bool>,
    trim_whitespace: Option<bool>,
    pii: Option<Pii>,
    retain: bool,
    characters: Option<TokenStream>,
    max_chars: Option<TokenStream>,
    bag_size: Option<TokenStream>,
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

        let trim_whitespace = if let Some(trim_whitespace) = self.trim_whitespace {
            quote!(#trim_whitespace)
        } else if let Some(ref parent_attrs) = inherit_from_field_attrs {
            quote!(#parent_attrs.trim_whitespace)
        } else {
            quote!(false)
        };

        let pii = if let Some(pii) = self.pii {
            pii.as_tokens()
        } else if let Some(ref parent_attrs) = inherit_from_field_attrs {
            quote!(#parent_attrs.pii)
        } else {
            quote!(crate::processor::Pii::False)
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

        let characters = if let Some(ref characters) = self.characters {
            quote!(Some(#characters))
        } else if let Some(ref parent_attrs) = inherit_from_field_attrs {
            quote!(#parent_attrs.characters)
        } else {
            quote!(None)
        };

        quote!({
            crate::processor::FieldAttrs {
                name: Some(#field_name),
                required: #required,
                nonempty: #nonempty,
                trim_whitespace: #trim_whitespace,
                max_chars: #max_chars,
                characters: #characters,
                bag_size: #bag_size,
                pii: #pii,
                retain: #retain,
            }
        })
    }
}

#[derive(Copy, Clone, Default)]
enum SkipSerialization {
    #[default]
    Never,
    Null(bool),
    Empty(bool),
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
) -> FieldAttrs {
    if bi_ast.ident.is_none() {
        *is_tuple_struct = true;
    } else if *is_tuple_struct {
        panic!("invalid tuple struct");
    }

    let mut rv = FieldAttrs {
        field_name: bi_ast
            .ident
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| index.to_string()),
        ..Default::default()
    };

    for attr in &bi_ast.attrs {
        let meta = match attr.parse_meta() {
            Ok(meta) => meta,
            Err(_) => continue,
        };

        let ident = match meta.path().get_ident() {
            Some(x) => x,
            None => continue,
        };

        if ident != "metastructure" {
            continue;
        }

        if let Meta::List(metalist) = meta {
            for nested_meta in metalist.nested {
                match nested_meta {
                    NestedMeta::Lit(..) => panic!("unexpected literal attribute"),
                    NestedMeta::Meta(meta) => match meta {
                        Meta::Path(path) => {
                            let ident = path.get_ident().expect("Unexpected path");

                            if ident == "additional_properties" {
                                rv.additional_properties = true;
                            } else if ident == "omit_from_schema" {
                                rv.omit_from_schema = true;
                            } else {
                                panic!("Unknown attribute {ident}");
                            }
                        }
                        Meta::NameValue(name_value) => {
                            let ident = name_value.path.get_ident().expect("Unexpected path");
                            if ident == "field" {
                                match name_value.lit {
                                    Lit::Str(litstr) => {
                                        rv.field_name = litstr.value();
                                    }
                                    _ => {
                                        panic!("Got non string literal for field");
                                    }
                                }
                            } else if ident == "required" {
                                match name_value.lit {
                                    Lit::Str(litstr) => match litstr.value().as_str() {
                                        "true" => rv.required = Some(true),
                                        "false" => rv.required = Some(false),
                                        other => panic!("Unknown value {other}"),
                                    },
                                    _ => {
                                        panic!("Got non string literal for required");
                                    }
                                }
                            } else if ident == "nonempty" {
                                match name_value.lit {
                                    Lit::Str(litstr) => match litstr.value().as_str() {
                                        "true" => rv.nonempty = Some(true),
                                        "false" => rv.nonempty = Some(false),
                                        other => panic!("Unknown value {other}"),
                                    },
                                    _ => {
                                        panic!("Got non string literal for nonempty");
                                    }
                                }
                            } else if ident == "trim_whitespace" {
                                match name_value.lit {
                                    Lit::Str(litstr) => match litstr.value().as_str() {
                                        "true" => rv.trim_whitespace = Some(true),
                                        "false" => rv.trim_whitespace = Some(false),
                                        other => panic!("Unknown value {other}"),
                                    },
                                    _ => {
                                        panic!("Got non string literal for trim_whitespace");
                                    }
                                }
                            } else if ident == "allow_chars" || ident == "deny_chars" {
                                if rv.characters.is_some() {
                                    panic!("allow_chars and deny_chars are mutually exclusive");
                                }

                                match name_value.lit {
                                    Lit::Str(litstr) => {
                                        let attr =
                                            parse_character_set(ident, litstr.value().as_str());
                                        rv.characters = Some(attr);
                                    }
                                    _ => {
                                        panic!("Got non string literal for max_chars");
                                    }
                                }
                            } else if ident == "max_chars" {
                                match name_value.lit {
                                    Lit::Str(litstr) => {
                                        let attr = parse_max_chars(litstr.value().as_str());
                                        rv.max_chars = Some(quote!(#attr));
                                    }
                                    _ => {
                                        panic!("Got non string literal for max_chars");
                                    }
                                }
                            } else if ident == "bag_size" {
                                match name_value.lit {
                                    Lit::Str(litstr) => {
                                        let attr = parse_bag_size(litstr.value().as_str());
                                        rv.bag_size = Some(quote!(#attr));
                                    }
                                    _ => {
                                        panic!("Got non string literal for bag_size");
                                    }
                                }
                            } else if ident == "pii" {
                                match name_value.lit {
                                    Lit::Str(litstr) => match litstr.value().as_str() {
                                        "true" => rv.pii = Some(Pii::True),
                                        "false" => rv.pii = Some(Pii::False),
                                        "maybe" => rv.pii = Some(Pii::Maybe),
                                        other => panic!("Unknown value {other}"),
                                    },
                                    _ => {
                                        panic!("Got non string literal for pii");
                                    }
                                }
                            } else if ident == "retain" {
                                match name_value.lit {
                                    Lit::Str(litstr) => match litstr.value().as_str() {
                                        "true" => rv.retain = true,
                                        "false" => rv.retain = false,
                                        other => panic!("Unknown value {other}"),
                                    },
                                    _ => {
                                        panic!("Got non string literal for retain");
                                    }
                                }
                            } else if ident == "legacy_alias" || ident == "skip_serialization" {
                                // Skip
                            } else {
                                panic!("Unknown argument to metastructure: {ident}");
                            }
                        }
                        other => {
                            panic!("Unexpected or bad attribute {:?}", other.path());
                        }
                    },
                }
            }
        }
    }
    rv
}

fn is_newtype(variant: &synstructure::VariantInfo) -> bool {
    variant.bindings().len() == 1 && variant.bindings()[0].ast().ident.is_none()
}

fn parse_character_set(ident: &Ident, value: &str) -> TokenStream {
    #[derive(Clone, Copy)]
    enum State {
        Blank,
        OpenRange(char),
        MidRange(char),
    }

    let mut state = State::Blank;
    let mut ranges = Vec::new();

    for c in value.chars() {
        match (state, c) {
            (State::Blank, a) => state = State::OpenRange(a),
            (State::OpenRange(a), '-') => state = State::MidRange(a),
            (State::OpenRange(a), c) => {
                state = State::OpenRange(c);
                ranges.push(quote!(#a..=#a));
            }
            (State::MidRange(a), b) => {
                ranges.push(quote!(#a..=#b));
                state = State::Blank;
            }
        }
    }

    match state {
        State::OpenRange(a) => ranges.push(quote!(#a..=#a)),
        State::MidRange(a) => {
            ranges.push(quote!(#a..=#a));
            ranges.push(quote!('-'..='-'));
        }
        State::Blank => {}
    }

    let is_negative = ident == "deny_chars";

    quote! {
        crate::processor::CharacterSet {
            char_is_valid: |c: char| -> bool {
                match c {
                    #((#ranges) => !#is_negative,)*
                    _ => #is_negative,
                }
            },
            ranges: &[ #(#ranges,)* ],
            is_negative: #is_negative,
        }
    }
}
