extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, ItemStruct};

#[proc_macro_attribute]
pub fn to_df(attrs: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    // parse input args
    let attrs = parse_macro_input!(attrs as syn::AttributeArgs);
    let datatypes: Vec<_> = attrs
        .into_iter()
        .map(|arg| {
            if let syn::NestedMeta::Meta(syn::Meta::Path(path)) = arg {
                path
            } else {
                panic!("Expected Meta::Path");
            }
        })
        .collect();
    if datatypes.is_empty() {
        panic!("At least one datatype must be specified");
    }

    let name = &input.ident;

    let field_names_and_types: Vec<_> =
        input.clone().fields.into_iter().map(|f| (f.ident.unwrap(), f.ty)).collect();

    let field_processing: Vec<_> = field_names_and_types
        .iter()
        .filter(|(name, _)| format!("{}", quote!(#name)) != "n_rows")
        .filter(|(_, value)| format!("{}", quote!(#value)).starts_with("Vec"))
        .filter(|(name, _)| name != "chain_id")
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

    fn map_type_to_column_type(ty: &syn::Type) -> proc_macro2::TokenStream {
        match quote!(#ty).to_string().as_str() {
            "Vec < Option < Vec < u8 > > >" => quote! { ColumnType::Binary },
            "Vec < String >" => quote! { ColumnType::String },
            "Vec < u32 >" => quote! { ColumnType::UInt32 },
            "Vec < Vec < u8 > >" => quote! { ColumnType::Binary },
            _ => quote! { ColumnType::Binary },
        }
    }

    let column_types: Vec<_> = field_names_and_types
        .iter()
        .map(|(name, ty)| {
            let column_type = map_type_to_column_type(ty);
            let field_name_str = format!("{}", quote!(#name));
            quote! {
                (#field_name_str, #column_type)
            }
        })
        .collect();

    let expanded = quote! {
        #input

        impl ToDataFrames for #name {

            fn create_dfs(
                self,
                schemas: &HashMap<Datatype, Table>,
                chain_id: u64,
            ) -> Result<HashMap<Datatype, DataFrame>> {
                let datatypes = vec![#(#datatypes),*];
                let datatype = if datatypes.len() == 1 {
                    datatypes[0]
                } else {
                    panic!("improper datatypes for single schema")
                };
                let schema = schemas.get(&datatype).expect("schema not provided");
                let mut cols = Vec::with_capacity(schema.columns().len());

                #(#field_processing)*

                if self.chain_id.len() == 0 {
                    with_series!(cols, "chain_id", vec![chain_id; self.n_rows as usize], schema);
                } else {
                    with_series!(cols, "chain_id", self.chain_id, schema);
                }

                let df = DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)?;
                let mut output = HashMap::new();
                output.insert(datatype, df);
                Ok(output)
            }
        }

        impl ColumnData for #name {

            fn column_types() -> HashMap<&'static str, ColumnType> {
                HashMap::from_iter(vec![
                    #(#column_types),*
                ])
            }
        }
    };

    expanded.into()
}
