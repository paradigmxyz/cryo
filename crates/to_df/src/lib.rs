extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, ItemStruct};

/// implements ToDataFrames and ColumnData for struct
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
                "Vec < Option < Vec < u8 > > >" => {
                    syn::Ident::new("with_series_binary", Span::call_site())
                }
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

    fn map_type_to_column_type(ty: &syn::Type) -> Option<proc_macro2::TokenStream> {
        match quote!(#ty).to_string().as_str() {
            "Vec < bool >" => Some(quote! { ColumnType::Boolean }),
            "Vec < u32 >" => Some(quote! { ColumnType::UInt32 }),
            "Vec < u64 >" => Some(quote! { ColumnType::UInt64 }),
            "Vec < U256 >" => Some(quote! { ColumnType::UInt256 }),
            "Vec < i32 >" => Some(quote! { ColumnType::Int32 }),
            "Vec < i64 >" => Some(quote! { ColumnType::Int64 }),
            "Vec < f32 >" => Some(quote! { ColumnType::Float32 }),
            "Vec < f64 >" => Some(quote! { ColumnType::Float64 }),
            "Vec < String >" => Some(quote! { ColumnType::String }),
            "Vec < Vec < u8 > >" => Some(quote! { ColumnType::Binary }),

            "Vec < Option < bool > >" => Some(quote! { ColumnType::Boolean }),
            "Vec < Option < u32 > >" => Some(quote! { ColumnType::UInt32 }),
            "Vec < Option < u64 > >" => Some(quote! { ColumnType::UInt64 }),
            "Vec < Option < U256 > >" => Some(quote! { ColumnType::UInt256 }),
            "Vec < Option < i32 > >" => Some(quote! { ColumnType::Int32 }),
            "Vec < Option < i64 > >" => Some(quote! { ColumnType::Int64 }),
            "Vec < Option < f32 > >" => Some(quote! { ColumnType::Float32 }),
            "Vec < Option < f64 > >" => Some(quote! { ColumnType::Float64 }),
            "Vec < Option < String > >" => Some(quote! { ColumnType::String }),
            "Vec < Option < Vec < u8 > > >" => Some(quote! { ColumnType::Binary }),
            _ => None,
            // _ => quote! {ColumnType::Binary},
        }
    }

    let datatype_str =
        datatypes[0].segments.iter().map(|seg| seg.ident.to_string()).collect::<Vec<_>>();
    let datatype_str = datatype_str.iter().last().unwrap();

    let mut column_types = Vec::new();
    for (name, ty) in field_names_and_types.iter() {
        if let Some(column_type) = map_type_to_column_type(ty) {
            let field_name_str = format!("{}", quote!(#name));
            column_types.push(quote! { (#field_name_str, #column_type) });
        } else if name != "n_rows" && name != "event_cols" {
            println!("invalid column type for {name} in table {}", datatype_str);
        }
    }

    let expanded = quote! {
        #input

        impl ToDataFrames for #name {

            fn create_dfs(
                self,
                schemas: &std::collections::HashMap<Datatype, Table>,
                chain_id: u64,
            ) -> R<std::collections::HashMap<Datatype, DataFrame>> {
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
                let mut output = std::collections::HashMap::new();
                output.insert(datatype, df);
                Ok(output)
            }
        }

        impl ColumnData for #name {

            fn column_types() -> std::collections::HashMap<&'static str, ColumnType> {
                std::collections::HashMap::from_iter(vec![
                    #(#column_types),*
                ])
            }
        }
    };

    expanded.into()
}
