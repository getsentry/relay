use proc_macro2::TokenStream;
use quote::ToTokens;
use synstructure::{Structure, VariantInfo};

pub trait SynstructureExt {
    fn try_each_variant<F, R>(&self, f: F) -> syn::Result<TokenStream>
    where
        F: FnMut(&VariantInfo<'_>) -> syn::Result<R>,
        R: ToTokens;
}

impl SynstructureExt for Structure<'_> {
    fn try_each_variant<F, R>(&self, mut f: F) -> syn::Result<TokenStream>
    where
        F: FnMut(&VariantInfo<'_>) -> syn::Result<R>,
        R: ToTokens,
    {
        let mut t = TokenStream::new();
        for variant in self.variants() {
            let pat = variant.pat();
            let body = f(variant)?;
            quote::quote!(#pat => { #body }).to_tokens(&mut t);
        }
        if self.omitted_variants() {
            quote::quote!(_ => {}).to_tokens(&mut t);
        }
        Ok(t)
    }
}
