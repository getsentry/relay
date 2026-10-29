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
    let mut variants = Vec::new();
    let mut structs = Vec::new();
    let mut constructors = Vec::new();
    let mut from_data = Vec::new();
    let mut refs = Vec::new();
    let mut muts = Vec::new();
    let mut tags = Vec::new();
    let mut counts = Vec::new();
    for variant in input.variants {
        let mut supports_item_count = false;
        let mut container = false;
        for attr in variant
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("item"))
        {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("item_count") {
                    supports_item_count = true;
                } else if meta.path.is_ident("container") {
                    supports_item_count = true;
                    container = true;
                } else {
                    return Err(meta.error("expected item_count or container"));
                }
                Ok(())
            })?;
        }
        let name = variant.ident;
        let alias = format_ident!("{}Item", name);
        let valid_field = match &variant.fields {
            syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                matches!(&fields.unnamed[0].ty, syn::Type::Path(ty) if ty.path.is_ident(&alias))
            }
            _ => false,
        };
        if !valid_field {
            return Err(syn::Error::new_spanned(
                variant.fields,
                format!("expected {name}({alias})"),
            ));
        }
        let docs = variant
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("doc"));
        let (args, pattern, tag) = if name == "Unknown" {
            (
                quote!(name: String),
                quote!(ItemType::#name(_)),
                quote!(#name(String)),
            )
        } else {
            (quote!(), quote!(ItemType::#name), quote!(#name))
        };
        variants.push(quote!(#(#docs)* #tag));
        let (extra_field, extra_init, tag_ref, pattern) = if name == "Unknown" {
            (
                quote!(ty: ItemType,),
                quote!(ty: ItemType::Unknown(name),),
                quote!(&self.ty),
                quote!(ItemType::Unknown(name)),
            )
        } else {
            (quote!(), quote!(), quote!(&ItemType::#name), pattern)
        };
        let container_api = container.then(|| quote! {
            impl ContainerPayload for #alias {
                fn set_payload_with_item_count(&mut self, content_type: ContentType, payload: impl Into<bytes::Bytes>, item_count: u32) {
                    self.data.set_payload_with_item_count(content_type, payload, item_count);
                }
            }
        });
        let item_count = supports_item_count.then(|| {
            quote! {
                fn item_count(&self) -> Option<u32> {
                    self.data.headers.get(ItemHeaderKey::ItemCount)
                }
            }
        });
        structs.push(quote! {
            #container_api
            #[derive(Clone)]
            pub struct #alias { data: ItemData, #extra_field }
            impl #alias {
                pub fn new(#args) -> Self { Self { data: ItemData::new(), #extra_init } }
            }
            impl std::ops::Deref for #alias {
                type Target = ItemData;
                fn deref(&self) -> &Self::Target { &self.data }
            }
            impl std::ops::DerefMut for #alias {
                fn deref_mut(&mut self) -> &mut Self::Target { &mut self.data }
            }
            impl std::fmt::Debug for #alias {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    self.data.debug_with_type(f, #tag_ref)
                }
            }
        });
        constructors.push(quote! {
            impl ItemPayload for #alias {
                fn from_item_ref(item: &Item) -> Option<&Self> {
                    match item { Item::#name(payload) => Some(payload), _ => None }
                }
                #item_count
            }
            impl TryFrom<Item> for #alias {
                type Error = Item;
                fn try_from(item: Item) -> Result<Self, Item> {
                    match item { Item::#name(payload) => Ok(payload), other => Err(other) }
                }
            }
            impl From<#alias> for Item {
                fn from(item: #alias) -> Self { Self::#name(item) }
            }
        });
        from_data.push(quote!(#pattern => Self::#name(#alias { data, #extra_init })));
        tags.push(if name == "Unknown" {
            quote!(Self::Unknown(item) => &item.ty)
        } else {
            quote!(Self::#name(_) => &ItemType::#name)
        });
        counts.push(quote!(Self::#name(item) => item.item_count()));
        refs.push(quote!(Self::#name(item) => &item.data));
        muts.push(quote!(Self::#name(item) => &mut item.data));
    }
    Ok(quote! {
        /// The wire tag of a serialized envelope item, generated from [`Item`].
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub enum ItemType { #(#variants),* }
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
