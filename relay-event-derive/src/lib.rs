//! Derive for visitor traits on the Event schema.
//!
//! This derive provides the `ProcessValue` trait. It must be used within the `relay-event-schema`
//! crate.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![recursion_limit = "256"]

use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};
use syn2::{self as syn, Ident, Lit, LitBool, LitInt, LitStr};
use synstructure2::{self as synstructure, decl_derive};

mod utils;

use utils::SynstructureExt as _;

decl_derive!(
    [ProcessValue, attributes(metastructure)] =>
    /// Derive the `ProcessValue` trait.
    derive_process_value
);

fn derive_process_value(mut s: synstructure::Structure<'_>) -> syn::Result<TokenStream> {
    let _ = s.bind_with(|_bi| synstructure::BindStyle::RefMut);
    let _ = s.add_bounds(synstructure::AddBounds::Generics);

    let type_attrs = parse_type_attributes(&s)?;
    let process_func_call_tokens = type_attrs.process_func_call_tokens();

    let process_value_arms = s.try_each_variant(|variant| {
        if is_newtype(variant) {
            // Process variant twice s.t. both processor functions are called.
            //
            // E.g.:
            // - For `Value::String`, call `process_string` as well as `process_value`.
            // - For `LenientString`, call `process_lenient_string` (not a thing.. yet) as well as
            // `process_string`.

            let bi = &variant.bindings()[0];
            let ident = &bi.binding;
            let field_attrs = parse_field_attributes(0, bi.ast(), &mut true)?;
            let field_attrs_tokens = field_attrs.as_tokens(Some(quote!(parent_attrs)));

            Ok(quote! {
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
            })
        } else {
            Ok(quote!())
        }
    })?;

    let process_child_values_arms = s.try_each_variant(|variant| {
        let mut is_tuple_struct = false;

        if is_newtype(variant) {
            // `process_child_values` has to be a noop because otherwise we recurse into the
            // subtree twice due to the weird `process_value` impl

            return Ok(quote!());
        }

        let mut body = TokenStream::new();
        for (index, bi) in variant.bindings().iter().enumerate() {
            let field_attrs = parse_field_attributes(index, bi.ast(), &mut is_tuple_struct)?;
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

        Ok(quote!({ #body }))
    })?;

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

    Ok(s.gen_impl(quote! {
        #[automatically_derived]
        #[expect(non_local_definitions, reason = "crate needs to be migrated to syn")]
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
    }))
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

fn parse_type_attributes(s: &synstructure::Structure<'_>) -> syn::Result<TypeAttrs> {
    let mut rv = TypeAttrs::default();

    for attr in &s.ast().attrs {
        if !attr.path().is_ident("metastructure") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            let ident = meta.path.require_ident()?;

            if ident == "process_func" {
                let s = meta.value()?.parse::<LitStr>()?;
                rv.process_func = Some(s.value());
            } else if ident == "value_type" {
                let s = meta.value()?.parse::<LitStr>()?;
                rv.value_type.push(s.value());
            } else {
                // Ignore other attributes used by `relay-protocol-derive`.
                if !meta.input.peek(syn::Token![,]) {
                    let _ = meta.value()?.parse::<Lit>()?;
                }
            }

            Ok(())
        })?;
    }

    Ok(rv)
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
    max_chars_allowance: Option<TokenStream>,
    max_depth: Option<TokenStream>,
    max_bytes: Option<TokenStream>,
    trim: Option<bool>,
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

        let trim = if let Some(trim) = self.trim {
            quote!(#trim)
        } else if let Some(ref parent_attrs) = inherit_from_field_attrs {
            quote!(#parent_attrs.trim)
        } else {
            quote!(true)
        };

        let retain = self.retain;

        let max_chars = if let Some(ref max_chars) = self.max_chars {
            quote!(Some(#max_chars))
        } else if let Some(ref parent_attrs) = inherit_from_field_attrs {
            quote!(#parent_attrs.max_chars)
        } else {
            quote!(None)
        };

        let max_chars_allowance = if let Some(ref max_chars_allowance) = self.max_chars_allowance {
            quote!(#max_chars_allowance)
        } else if let Some(ref parent_attrs) = inherit_from_field_attrs {
            quote!(#parent_attrs.max_chars_allowance)
        } else {
            quote!(0)
        };

        let max_depth = if let Some(ref max_depth) = self.max_depth {
            quote!(Some(#max_depth))
        } else if let Some(ref parent_attrs) = inherit_from_field_attrs {
            quote!(#parent_attrs.max_depth)
        } else {
            quote!(None)
        };

        let max_bytes = if let Some(ref max_bytes) = self.max_bytes {
            quote!(Some(#max_bytes))
        } else if let Some(ref parent_attrs) = inherit_from_field_attrs {
            quote!(#parent_attrs.max_bytes)
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
                max_chars_allowance: #max_chars_allowance,
                characters: #characters,
                max_depth: #max_depth,
                max_bytes: #max_bytes,
                pii: #pii,
                retain: #retain,
                trim: #trim,
            }
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

    let mut rv = FieldAttrs {
        field_name: bi_ast
            .ident
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| index.to_string()),
        ..Default::default()
    };

    for attr in &bi_ast.attrs {
        if !attr.path().is_ident("metastructure") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            let ident = meta.path.require_ident()?;

            if ident == "additional_properties" {
                rv.additional_properties = true;
            } else if ident == "omit_from_schema" {
                rv.omit_from_schema = true;
            } else if ident == "field" {
                let s = meta.value()?.parse::<LitStr>()?;
                rv.field_name = s.value();
            } else if ident == "required" {
                let s = meta.value()?.parse::<LitBool>()?;
                rv.required = Some(s.value());
            } else if ident == "nonempty" {
                let s = meta.value()?.parse::<LitBool>()?;
                rv.nonempty = Some(s.value());
            } else if ident == "trim_whitespace" {
                let s = meta.value()?.parse::<LitBool>()?;
                rv.trim_whitespace = Some(s.value());
            } else if ident == "allow_chars" || ident == "deny_chars" {
                if rv.characters.is_some() {
                    return Err(meta.error("allow_chars and deny_chars are mutually exclusive"));
                }
                let s = meta.value()?.parse::<LitStr>()?;
                rv.characters = Some(parse_character_set(ident, &s.value()));
            } else if ident == "max_chars" {
                let s = meta.value()?.parse::<LitInt>()?;
                rv.max_chars = Some(quote!(#s));
            } else if ident == "max_chars_allowance" {
                let s = meta.value()?.parse::<LitInt>()?;
                rv.max_chars_allowance = Some(quote!(#s));
            } else if ident == "max_depth" {
                let s = meta.value()?.parse::<LitInt>()?;
                rv.max_depth = Some(quote!(#s));
            } else if ident == "max_bytes" {
                let s = meta.value()?.parse::<LitInt>()?;
                rv.max_bytes = Some(quote!(#s));
            } else if ident == "pii" {
                let s = meta.value()?.parse::<LitStr>()?;
                rv.pii = Some(match s.value().as_str() {
                    "true" => Pii::True,
                    "false" => Pii::False,
                    "maybe" => Pii::Maybe,
                    _ => return Err(meta.error("Expected one of `true`, `false`, `maybe`")),
                });
            } else if ident == "retain" {
                let s = meta.value()?.parse::<LitBool>()?;
                rv.retain = s.value();
            } else if ident == "trim" {
                let s = meta.value()?.parse::<LitBool>()?;
                rv.trim = Some(s.value());
            } else if ident == "legacy_alias" || ident == "skip_serialization" {
                let _ = meta.value()?.parse::<Lit>()?;
            } else {
                return Err(meta.error("Unknown argument"));
            }

            Ok(())
        })?;
    }

    Ok(rv)
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
