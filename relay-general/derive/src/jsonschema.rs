use proc_macro2::TokenStream;
use quote::quote;
use syn::Visibility;

use crate::parse_field_attributes;

pub fn derive_jsonschema(mut s: synstructure::Structure<'_>) -> TokenStream {
    let _ = s.add_bounds(synstructure::AddBounds::Generics);

    let mut arms = quote!();

    for variant in s.variants() {
        let mut fields = quote!();

        let mut is_tuple_struct = false;

        for (index, bi) in variant.bindings().iter().enumerate() {
            let field_attrs = parse_field_attributes(index, &bi.ast(), &mut is_tuple_struct);
            let name = field_attrs.field_name;

            fields = quote!(#fields #[schemars(rename = #name)]);

            if !field_attrs.required.unwrap_or(false) {
                fields = quote!(#fields #[schemars(default)]);
            }

            if field_attrs.additional_properties || field_attrs.omit_from_schema {
                fields = quote!(#fields #[schemars(skip)]);
            }

            let mut ast = bi.ast().clone();
            ast.vis = Visibility::Inherited;
            ast.attrs.retain(|attr| {
                attr.parse_meta()
                    .ok()
                    .and_then(|meta| Some(meta.path().get_ident()? == "doc"))
                    .unwrap_or(false)
            });
            fields = quote!(#fields #ast,);
        }

        let ident = variant.ast().ident;

        let arm = if is_tuple_struct {
            quote!( #ident( #fields ) )
        } else {
            quote!( #ident { #fields } )
        };

        arms = quote! {
            #arms
            #arm,
        };
    }

    let ident = &s.ast().ident;

    s.gen_impl(quote! {
        #[automatically_derived]
        gen impl schemars::JsonSchema for @Self {
            fn schema_name() -> String {
                stringify!(#ident).to_owned()
            }

            fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
                #[derive(schemars::JsonSchema)]
                #[schemars(untagged)]
                enum Helper {
                    #arms
                }

                Helper::json_schema(gen)
            }
        }
    })
}
