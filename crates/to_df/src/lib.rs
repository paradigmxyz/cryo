extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, parse_str, ItemStruct};

struct ToDataFramesMetaParams {
    flatten: Option<String>,
}

impl ToDataFramesMetaParams {
    fn parse_attributes(attrs: &[syn::Attribute]) -> syn::Result<Self> {
        let mut flatten = None;

        for attr in attrs.iter().filter(|a| a.path().is_ident("to_df")) {
            attr.parse_nested_meta(|meta| {
                // get path from #[to_df(flatten = "<path>")]
                if meta.path.is_ident("flatten") {
                    let lit = meta.value()?.parse::<syn::LitStr>()?;
                    flatten = Some(lit.value());
                }
                Ok(())
            })?;
        }

        Ok(ToDataFramesMetaParams { flatten })
    }
}

/// Implements ToDataFrames and ColumnData for struct.
///
/// Usage:
/// ```no_run
/// # use cryo_to_df::ToDataFrames;
/// # type DynSolValue = u64;
/// # pub mod cryo_freeze {
/// #     pub mod indexmap { pub use std::collections::BTreeMap as IndexMap; }
/// #     pub mod polars {
/// #         pub mod prelude {
/// #             pub struct DataFrame; impl DataFrame { pub fn new<T>(_: T) -> Result<Self, ()> { Ok(Self) } }
/// #             pub struct Column;
/// #         }
/// #     }
/// #     use polars::prelude::*;
/// #     use std::collections::HashMap;
/// #     pub enum ColumnType { Boolean, UInt32, UInt64, UInt256, Int32, Int64, Float32, Float64, String, Binary }
/// #     pub enum CollectError { PolarsError(()) }
/// #     #[derive(Hash, PartialEq, Eq)]
/// #     pub enum Datatype { MyStruct }
/// #     pub struct Table; impl Table { pub fn columns(&self) -> Vec<&'static str> { vec![] } }
/// #     pub trait SortableDataFrame { fn sort_by_schema(self, schema: &Table) -> Self; }
/// #     impl SortableDataFrame for Result<DataFrame, CollectError> { fn sort_by_schema(self, schema: &Table) -> Self { self } }
/// #     #[macro_export]
/// #     macro_rules! with_column_impl { ($cols:expr, $name:expr, $value:expr, $schema:expr) => {let _: Vec<Column> = $cols;}; }
/// #     pub use with_column_impl as with_column;
/// #     pub use with_column_impl as with_column_primitive;
/// #     pub trait ColumnData {
/// #         fn column_types() -> indexmap::IndexMap<&'static str, ColumnType>;
/// #     }
/// #     pub trait ToDataFrames: Sized {
/// #         fn create_dfs(self, schemas: &HashMap<Datatype, Table>, chain_id: u64) -> Result<HashMap<Datatype, DataFrame>, CollectError>;
/// #     }
/// # }
/// use cryo_freeze::*;
/// use polars::prelude::*;
///
/// #[derive(ToDataFrames)]
/// struct MyStruct {
///     n_rows: u64,
///     field1: Vec<u32>,
///     field2: Vec<String>,
///     #[to_df(flatten = "extract_others")]
///     others: indexmap::IndexMap<String, Vec<DynSolValue>>,
///     chain_id: Vec<u64>,
/// }
///
/// fn extract_others(
///     cols: &mut Vec<Column>,
///     name: &str,
///     values: indexmap::IndexMap<String, Vec<DynSolValue>>,
///     n_rows: usize,
///     schema: &Table,
/// ) -> Result<(), CollectError> {
///     todo!()
/// }
/// ```
#[proc_macro_derive(ToDataFrames, attributes(to_df))]
pub fn to_data_frames(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let name = &input.ident;

    let datatype = quote!(Datatype::#name);
    let datatype_str = name.to_string();

    let field_names_and_types: Vec<_> =
        input.fields.iter().map(|f| (f.ident.clone().unwrap(), f.ty.clone())).collect();

    let field_processing: Vec<_> = field_names_and_types
        .iter()
        .filter(|(name, _)| quote!(#name).to_string() != "n_rows")
        .filter(|(_, value)| quote!(#value).to_string().starts_with("Vec"))
        .filter(|(name, _)| name != "chain_id")
        .map(|(name, ty)| {
            let macro_name = match quote!(#ty).to_string().as_str() {
                "Vec < u32 >" | "Vec < u64 >" => "with_column_primitive",
                "Vec < i32 >" | "Vec < i64 >" => "with_column_primitive",
                "Vec < f32 >" | "Vec < f64 >" => "with_column_primitive",
                _ => "with_column",
            };
            let macro_name = syn::Ident::new(macro_name, Span::call_site());
            let field_name_str = quote!(#name).to_string();
            quote! {
                #macro_name!(cols, #field_name_str, self.#name, schema);
            }
        })
        .collect();

    let mut flatten_field = None;
    let event_code = input
        .fields
        .iter()
        .find_map(|f| {
            let params = ToDataFramesMetaParams::parse_attributes(&f.attrs).unwrap();
            params.flatten.map(|s| (f, s))
        })
        .map(|(field, flatten)| {
            let expr = parse_str::<syn::Expr>(&flatten).unwrap();
            let field_name = field.ident.as_ref().unwrap();
            let field_name_str = field_name.to_string();
            flatten_field = Some(field_name_str.clone());
            quote! {
                #expr(&mut cols, #field_name_str, self.#field_name, self.n_rows as usize, schema)?;
            }
        });

    fn map_type_to_column_type(ty: &syn::Type) -> Option<proc_macro2::TokenStream> {
        match quote!(#ty).to_string().as_str() {
            "Vec < bool >" => Some(quote! { ColumnType::Boolean }),
            "Vec < u32 >" => Some(quote! { ColumnType::UInt32 }),
            "Vec < u64 >" => Some(quote! { ColumnType::UInt64 }),
            "Vec < U256 >" => Some(quote! { ColumnType::UInt256 }),
            "Vec < i32 >" => Some(quote! { ColumnType::Int32 }),
            "Vec < i64 >" => Some(quote! { ColumnType::Int64 }),
            "Vec < I256 >" => Some(quote! { ColumnType::Int256 }),
            "Vec < f32 >" => Some(quote! { ColumnType::Float32 }),
            "Vec < f64 >" => Some(quote! { ColumnType::Float64 }),
            "Vec < String >" => Some(quote! { ColumnType::String }),
            "Vec < Vec < u8 > >" => Some(quote! { ColumnType::Binary }),
            "Vec < RawBytes >" => Some(quote! { ColumnType::Binary }),

            "Vec < Option < bool > >" => Some(quote! { ColumnType::Boolean }),
            "Vec < Option < u32 > >" => Some(quote! { ColumnType::UInt32 }),
            "Vec < Option < u64 > >" => Some(quote! { ColumnType::UInt64 }),
            "Vec < Option < U256 > >" => Some(quote! { ColumnType::UInt256 }),
            "Vec < Option < i32 > >" => Some(quote! { ColumnType::Int32 }),
            "Vec < Option < i64 > >" => Some(quote! { ColumnType::Int64 }),
            "Vec < Option < I256 > >" => Some(quote! { ColumnType::Int256 }),
            "Vec < Option < f32 > >" => Some(quote! { ColumnType::Float32 }),
            "Vec < Option < f64 > >" => Some(quote! { ColumnType::Float64 }),
            "Vec < Option < String > >" => Some(quote! { ColumnType::String }),
            "Vec < Option < Vec < u8 > > >" => Some(quote! { ColumnType::Binary }),
            "Vec < Option < RawBytes > >" => Some(quote! { ColumnType::Binary }),
            _ => None,
            // _ => quote! {ColumnType::Binary},
        }
    }

    let mut column_types = Vec::new();
    for (name, ty) in field_names_and_types.iter() {
        if let Some(column_type) = map_type_to_column_type(ty) {
            let field_name_str = format!("{}", quote!(#name));
            column_types.push(quote! { (#field_name_str, #column_type) });
        } else if name != "n_rows" &&
            (flatten_field.is_none() || name != flatten_field.as_deref().unwrap())
        {
            println!("invalid column type for {name} in table {datatype_str}");
        }
    }

    let expanded = quote! {
        impl ToDataFrames for #name {

            fn create_dfs(
                self,
                schemas: &std::collections::HashMap<Datatype, Table>,
                chain_id: u64,
            ) -> Result<std::collections::HashMap<Datatype, DataFrame>, CollectError> {
                let datatype = #datatype;
                let schema = schemas.get(&datatype).expect("schema not provided");
                let mut cols = Vec::with_capacity(schema.columns().len());

                #(#field_processing)*

                if self.chain_id.len() == 0 {
                    with_column!(cols, "chain_id", vec![chain_id; self.n_rows as usize], schema);
                } else {
                    with_column!(cols, "chain_id", self.chain_id, schema);
                }

                #event_code

                let df = DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)?;
                let mut output = std::collections::HashMap::new();
                output.insert(datatype, df);
                Ok(output)
            }
        }

        impl ColumnData for #name {

            fn column_types() -> indexmap::IndexMap<&'static str, ColumnType> {
                indexmap::IndexMap::from_iter(vec![
                    #(#column_types),*
                ])
            }
        }
    };

    expanded.into()
}

/// Implements FromDataFrames for struct.
///
/// Usage:
/// ```no_run
/// # use cryo_to_df::FromDataFrames;
/// # pub mod cryo_freeze {
/// #     pub mod polars {
/// #         pub mod prelude {
/// #             pub enum PolarsError { ColumnNotFound(String) }
/// #             pub struct DataFrame;
/// #             impl DataFrame {
/// #                 pub fn column(&self, name: &str) -> Result<&Series, ()> { Ok(&Series) }
/// #                 pub fn height(&self) -> usize { 0 }
/// #             }
/// #             pub struct Series;
/// #             impl Series {
/// #                 pub fn u32(&self) -> Result<Vec<Option<u32>>, ()> { Ok(vec![]) }
/// #                 pub fn u64(&self) -> Result<Vec<Option<u64>>, ()> { Ok(vec![]) }
/// #                 pub fn str(&self) -> Result<Vec<Option<&str>>, ()> { Ok(vec![]) }
/// #             }
/// #         }
/// #     }
/// #     use polars::prelude::*;
/// #     use std::collections::HashMap;
/// #     pub enum CollectError { PolarsError(PolarsError) }
/// #     #[derive(Hash, PartialEq, Eq)]
/// #     pub enum Datatype { MyStruct }
/// #     pub struct Table; impl Table { pub fn columns(&self) -> Vec<&'static str> { vec![] } }
/// #     #[macro_export]
/// #     macro_rules! parse_column_impl { ($df:expr, $name:expr, $ty:ident, $value:expr, $schema:expr) => {}; }
/// #     pub use parse_column_impl as parse_column;
/// #     pub use parse_column_impl as parse_column_primitive;
/// #     pub trait FromDataFrames: Sized {
/// #         fn parse_dfs(&mut self, dfs: HashMap<Datatype, DataFrame>, schemas: &HashMap<Datatype, Table>) -> Result<&mut Self, CollectError>;
/// #     }
/// # }
/// use cryo_freeze::*;
/// use polars::prelude::*;
///
/// #[derive(FromDataFrames)]
/// struct MyStruct {
///     n_rows: u64,
///     field1: Vec<u32>,
///     field2: Vec<String>,
///     chain_id: Vec<u64>,
/// }
/// ```
#[proc_macro_derive(FromDataFrames, attributes(to_df))]
pub fn from_data_frames(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let name = &input.ident;

    let datatype = quote!(Datatype::#name);

    let field_names_and_types: Vec<_> =
        input.fields.iter().map(|f| (f.ident.clone().unwrap(), f.ty.clone())).collect();

    // Generate field population code using the new parse_column macros
    let field_population: Vec<_> = field_names_and_types
        .iter()
        .filter(|(name, _)| quote!(#name).to_string() != "n_rows")
        .filter(|(_, value)| quote!(#value).to_string().starts_with("Vec"))
        .map(|(name, ty)| {
            let field_name_str = quote!(#name).to_string();
            match quote!(#ty).to_string().as_str() {
                "Vec < u32 >" | "Vec < Option < u32 > >" => quote! {
                    parse_column_primitive!(df, #field_name_str, u32, self.#name, schema);
                },
                "Vec < u64 >" | "Vec < Option < u64 > >" => quote! {
                    parse_column_primitive!(df, #field_name_str, u64, self.#name, schema);
                },
                "Vec < i32 >" | "Vec < Option < i32 > >" => quote! {
                    parse_column_primitive!(df, #field_name_str, i32, self.#name, schema);
                },
                "Vec < i64 >" | "Vec < Option < i64 > >" => quote! {
                    parse_column_primitive!(df, #field_name_str, i64, self.#name, schema);
                },
                "Vec < f32 >" | "Vec < Option < f32 > >" => quote! {
                    parse_column_primitive!(df, #field_name_str, f32, self.#name, schema);
                },
                "Vec < f64 >" | "Vec < Option < f64 > >" => quote! {
                    parse_column_primitive!(df, #field_name_str, f64, self.#name, schema);
                },
                "Vec < String >" | "Vec < Option < String > >" => quote! {
                    parse_column_primitive!(df, #field_name_str, str, self.#name, schema);
                },
                "Vec < bool >" | "Vec < Option < bool > >" => quote! {
                    parse_column_primitive!(df, #field_name_str, bool, self.#name, schema);
                },
                // Handle U256/I256 types with binary column name fallback
                "Vec < U256 >" | "Vec < Option < U256 > >" => quote! {
                    parse_column!(df, #field_name_str, U256, self.#name, schema);
                },
                "Vec < I256 >" | "Vec < Option < I256 > >" => quote! {
                    parse_column!(df, #field_name_str, I256, self.#name, schema);
                },
                "Vec < RawBytes >" | "Vec < Option < RawBytes > >" => quote! {
                    parse_column_primitive!(df, #field_name_str, binary, self.#name, schema);
                },
                _ => quote! {
                    // Handle unsupported types - for now, just set to default
                    result.#name = vec![];
                },
            }
        })
        .collect();

    let expanded = quote! {
        impl FromDataFrames for #name {
            fn parse_dfs(
                &mut self,
                dfs: std::collections::HashMap<Datatype, DataFrame>,
                schemas: &std::collections::HashMap<Datatype, Table>,
            ) -> Result<&mut Self, CollectError> {
                let datatype = #datatype;
                let schema = schemas.get(&datatype).expect("schema not provided");

                let df = dfs.get(&datatype).ok_or_else(|| {
                    CollectError::PolarsError(polars::prelude::PolarsError::ColumnNotFound("dataframe not found".into()))
                })?;

                self.n_rows = df.height() as u64;

                #(#field_population)*

                Ok(self)
            }
        }
    };

    expanded.into()
}
