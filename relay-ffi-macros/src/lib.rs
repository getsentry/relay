//! TODO

#![warn(missing_docs)]

use proc_macro::TokenStream;
use quote::ToTokens;
use syn::fold::Fold;

struct CatchUnwind;

impl CatchUnwind {
    fn fold(&mut self, input: TokenStream) -> TokenStream {
        let f = syn::parse(input).expect("#[catch_unwind] can only be applied to functions");
        self.fold_item_fn(f).to_token_stream().into()
    }
}

impl Fold for CatchUnwind {
    fn fold_item_fn(&mut self, i: syn::ItemFn) -> syn::ItemFn {
        if i.sig.unsafety.is_none() {
            panic!("#[catch_unwind] requires `unsafe fn`");
        }

        let inner = i.block;
        let folded = quote::quote! {{
            ::relay_ffi::__internal::catch_errors(|| {
                let __ret = #inner;

                #[allow(unreachable_code)]
                Ok(__ret)
            })
        }};

        let block = Box::new(syn::parse2(folded).unwrap());
        syn::ItemFn { block, ..i }
    }
}

/// TODO: Doc
#[proc_macro_attribute]
pub fn catch_unwind(_attr: TokenStream, item: TokenStream) -> TokenStream {
    CatchUnwind.fold(item)
}
