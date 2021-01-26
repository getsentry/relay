use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};
use syn::Ident;

use crate::{is_newtype, parse_field_attributes, parse_type_attributes};

pub fn derive_process_value(mut s: synstructure::Structure<'_>) -> TokenStream {
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
            let field_attrs = parse_field_attributes(index, &bi.ast(), &mut is_tuple_struct);
            let ident = &bi.binding;
            let field_attrs_name = Ident::new(&format!("FIELD_ATTRS_{}", index), Span::call_site());
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
            quote!(enumset::enum_set!( #(crate::processor::ValueType::#value_names)|* ))
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
                __meta: &mut crate::types::Meta,
                __processor: &mut P,
                __state: &crate::processor::ProcessingState<'_>,
            ) -> crate::types::ProcessingResult
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
            ) -> crate::types::ProcessingResult
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
