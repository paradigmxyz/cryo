extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, ItemStruct};

#[proc_macro_attribute]
pub fn to_df(_attrs: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let name = &input.ident;

    let field_names_and_types: Vec<_> =
        input.clone().fields.into_iter().map(|f| (f.ident.unwrap(), f.ty)).collect();

    let field_processing: Vec<_> = field_names_and_types
        .iter()
        .filter(|(name, _)| format!("{}", quote!(#name)) != "n_rows")
        .filter(|(_, value)| format!("{}", quote!(#value)).starts_with("Vec"))
        .map(|(name, ty)| {
            let macro_name = match quote!(#ty).to_string().as_str() {
                "Vec < Vec < u8 > >" => syn::Ident::new("with_series_binary", Span::call_site()),
                "Vec < U256 >" => syn::Ident::new("with_series_u256", Span::call_site()),
                "Vec < Option < U256 > >" => {
                    syn::Ident::new("with_series_option_u256", Span::call_site())
                }
                _ => syn::Ident::new("with_series", Span::call_site()),
            };
            let field_name_str = format!("{}", quote!(#name));
            quote! {
                #macro_name!(cols, #field_name_str, self.#name, schema);
            }
        })
        .collect();

    let expanded = quote! {
        #input

        impl ColumnData for #name {
            fn create_df(self, schema: &Table, chain_id: u64) -> Result<DataFrame, CollectError> {
                let mut cols = Vec::with_capacity(schema.columns().len());

                #(#field_processing)*

                with_series!(cols, "chain_id", vec![chain_id; self.n_rows as usize], schema);

                DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
            }
        }
    };

    // println!("{}", expanded.to_string());
    expanded.into()
}
