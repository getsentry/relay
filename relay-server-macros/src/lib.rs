//! Generate typed envelope item payloads and their wire tags.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::Result;

/// Generate wire tags and variant helpers from the owning serialized Item enum.
#[proc_macro_derive(TypedItems, attributes(item))]
pub fn typed_items(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    syn::parse::<syn::ItemEnum>(input)
        .and_then(expand_typed_items)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand_typed_items(input: syn::ItemEnum) -> Result<TokenStream> {
    if input.ident != "Item" || !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            input.ident,
            "expected the non-generic owning Item enum",
        ));
    }
    let variants = input
        .variants
        .into_iter()
        .map(VariantCode::parse)
        .collect::<Result<Vec<_>>>()?;
    let wire_variants = variants.iter().map(|variant| &variant.wire_variant);
    let structs = variants.iter().map(|variant| &variant.payload_struct);
    let constructors = variants.iter().map(|variant| &variant.conversions);
    let from_data = variants.iter().map(|variant| &variant.from_data);
    let tags = variants.iter().map(|variant| &variant.tag);
    let counts = variants.iter().map(|variant| &variant.count);
    let refs = variants.iter().map(|variant| &variant.reference);
    let muts = variants.iter().map(|variant| &variant.reference_mut);

    Ok(quote! {
        /// The wire tag of a serialized envelope item, generated from [`Item`].
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub enum ItemType { #(#wire_variants),* }
        #(#structs)*
        #(#constructors)*
        impl Item {
            pub fn new(ty: ItemType) -> Self { Self::from_parts(ty, ItemData::new()) }
            fn from_parts(ty: ItemType, data: ItemData) -> Self {
                match ty { #(#from_data),* }
            }
            pub fn ty(&self) -> &ItemType { match self { #(#tags),* } }
            pub fn len(&self) -> usize { std::ops::Deref::deref(self).len() }
            pub fn item_count(&self) -> Option<u32> {
                match self { #(#counts),* }
            }
        }
        impl std::ops::Deref for Item {
            type Target = ItemData;
            fn deref(&self) -> &Self::Target { match self { #(#refs),* } }
        }
        impl std::ops::DerefMut for Item {
            fn deref_mut(&mut self) -> &mut Self::Target { match self { #(#muts),* } }
        }
    })
}

/// Count metadata can be absent, readable, or readable and writable as a container.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum ItemCountSupport {
    None,
    Read,
    ReadWrite,
}

impl ItemCountSupport {
    fn from_attributes(attributes: &[syn::Attribute]) -> Result<Self> {
        attributes
            .iter()
            .filter(|attribute| attribute.path().is_ident("item"))
            .try_fold(Self::None, |support, attribute| {
                let declared = attribute.parse_args_with(
                    syn::punctuated::Punctuated::<Self, syn::Token![,]>::parse_terminated,
                )?;
                Ok(declared.into_iter().fold(support, std::cmp::max))
            })
    }

    fn count_method(self) -> TokenStream {
        match self {
            Self::None => quote!(),
            Self::Read | Self::ReadWrite => quote! {
                fn item_count(&self) -> Option<u32> {
                    self.data.headers.get(ItemHeaderKey::ItemCount)
                }
            },
        }
    }

    fn container_impl(self, payload: &syn::Ident) -> TokenStream {
        match self {
            Self::None | Self::Read => quote!(),
            Self::ReadWrite => quote! {
                impl ContainerPayload for #payload {
                    fn set_payload_with_item_count(&mut self, content_type: ContentType, payload: impl Into<bytes::Bytes>, item_count: u32) {
                        self.data.set_payload_with_item_count(content_type, payload, item_count);
                    }
                }
            },
        }
    }
}

impl syn::parse::Parse for ItemCountSupport {
    fn parse(input: syn::parse::ParseStream<'_>) -> Result<Self> {
        let name: syn::Ident = input.parse()?;
        match name.to_string().as_str() {
            "item_count" => Ok(Self::Read),
            "container" => Ok(Self::ReadWrite),
            _ => Err(syn::Error::new_spanned(
                name,
                "expected item_count or container",
            )),
        }
    }
}

/// All differences between a fixed wire tag and an unknown tag carrying its name.
struct WireTag {
    constructor_args: TokenStream,
    wire_variant: TokenStream,
    wire_pattern: TokenStream,
    payload_field: TokenStream,
    payload_init: TokenStream,
    debug_tag: TokenStream,
    item_tag: TokenStream,
}

impl WireTag {
    fn for_variant(name: &syn::Ident) -> Self {
        match name.to_string().as_str() {
            "Unknown" => Self {
                constructor_args: quote!(name: String),
                wire_variant: quote!(#name(String)),
                wire_pattern: quote!(ItemType::Unknown(name)),
                payload_field: quote!(ty: ItemType,),
                payload_init: quote!(ty: ItemType::Unknown(name),),
                debug_tag: quote!(&self.ty),
                item_tag: quote!(Self::Unknown(item) => &item.ty),
            },
            _ => Self {
                constructor_args: quote!(),
                wire_variant: quote!(#name),
                wire_pattern: quote!(ItemType::#name),
                payload_field: quote!(),
                payload_init: quote!(),
                debug_tag: quote!(&ItemType::#name),
                item_tag: quote!(Self::#name(_) => &ItemType::#name),
            },
        }
    }
}

/// A variant's generated declarations and match arms, ready for interpolation.
struct VariantCode {
    wire_variant: TokenStream,
    payload_struct: TokenStream,
    conversions: TokenStream,
    from_data: TokenStream,
    tag: TokenStream,
    count: TokenStream,
    reference: TokenStream,
    reference_mut: TokenStream,
}

impl VariantCode {
    fn parse(variant: syn::Variant) -> Result<Self> {
        let count_support = ItemCountSupport::from_attributes(&variant.attrs)?;
        let name = &variant.ident;
        let payload = format_ident!("{}Item", name);
        let valid_field = match &variant.fields {
            syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                matches!(&fields.unnamed[0].ty, syn::Type::Path(ty) if ty.path.is_ident(&payload))
            }
            _ => false,
        };
        if !valid_field {
            return Err(syn::Error::new_spanned(
                variant.fields,
                format!("expected {name}({payload})"),
            ));
        }
        let docs = variant
            .attrs
            .iter()
            .filter(|attribute| attribute.path().is_ident("doc"));
        let WireTag {
            constructor_args,
            wire_variant,
            wire_pattern,
            payload_field,
            payload_init,
            debug_tag,
            item_tag,
        } = WireTag::for_variant(name);
        let container_impl = count_support.container_impl(&payload);
        let count_method = count_support.count_method();

        Ok(Self {
            wire_variant: quote!(#(#docs)* #wire_variant),
            payload_struct: quote! {
                #container_impl
                #[derive(Clone)]
                pub struct #payload { data: ItemData, #payload_field }
                impl #payload {
                    pub fn new(#constructor_args) -> Self { Self { data: ItemData::new(), #payload_init } }
                }
                impl std::ops::Deref for #payload {
                    type Target = ItemData;
                    fn deref(&self) -> &Self::Target { &self.data }
                }
                impl std::ops::DerefMut for #payload {
                    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.data }
                }
                impl std::fmt::Debug for #payload {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        self.data.debug_with_type(f, #debug_tag)
                    }
                }
            },
            conversions: quote! {
                impl ItemPayload for #payload {
                    fn from_item_ref(item: &Item) -> Option<&Self> {
                        match item { Item::#name(payload) => Some(payload), _ => None }
                    }
                    #count_method
                }
                impl TryFrom<Item> for #payload {
                    type Error = Item;
                    fn try_from(item: Item) -> Result<Self, Item> {
                        match item { Item::#name(payload) => Ok(payload), other => Err(other) }
                    }
                }
                impl From<#payload> for Item {
                    fn from(item: #payload) -> Self { Self::#name(item) }
                }
            },
            from_data: quote!(#wire_pattern => Self::#name(#payload { data, #payload_init })),
            tag: item_tag,
            count: quote!(Self::#name(item) => item.item_count()),
            reference: quote!(Self::#name(item) => &item.data),
            reference_mut: quote!(Self::#name(item) => &mut item.data),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_tags_from_owning_variants() {
        let input = syn::parse_quote!(
            pub enum Item {
                Transaction(TransactionItem),
                Profile(ProfileItem),
                Unknown(UnknownItem),
            }
        );
        let generated = expand_typed_items(input).unwrap();
        syn::parse2::<syn::File>(generated.clone()).unwrap();
        let generated = generated.to_string();
        assert!(generated.contains("enum ItemType { Transaction , Profile , Unknown (String) }"));
        assert!(generated.contains("struct TransactionItem { data : ItemData"));
        assert!(generated.contains("ItemType :: Unknown (name) => Self :: Unknown"));
        assert!(!generated.contains("Box"));
    }

    #[test]
    fn container_setters_are_only_generated_for_container_payloads() {
        let generated = expand_typed_items(syn::parse_quote! {
            pub enum Item {
                Transaction(TransactionItem),
                #[item(item_count)]
                Profile(ProfileItem),
                #[item(container)]
                Log(LogItem),
            }
        })
        .unwrap();
        let file: syn::File = syn::parse2(generated).unwrap();
        let container_impls: Vec<_> = file
            .items
            .iter()
            .filter_map(|item| {
                let syn::Item::Impl(item) = item else {
                    return None;
                };
                let (_, implemented, _) = item.trait_.as_ref()?;
                implemented.is_ident("ContainerPayload").then(|| {
                    let ty = &item.self_ty;
                    quote!(#ty).to_string()
                })
            })
            .collect();
        assert_eq!(container_impls, ["LogItem"]);
        assert!(
            expand_typed_items(syn::parse_quote! {
                pub enum Item { #[item(contianer)] Log(LogItem) }
            })
            .is_err()
        );
    }

    #[test]
    fn rejects_mismatched_variant_payloads() {
        for input in [
            syn::parse_quote!(
                pub enum Item {
                    Transaction(ProfileItem),
                }
            ),
            syn::parse_quote!(
                pub enum Item {
                    Profile,
                }
            ),
        ] {
            assert!(
                expand_typed_items(input)
                    .unwrap_err()
                    .to_string()
                    .contains("expected")
            );
        }
    }
}
