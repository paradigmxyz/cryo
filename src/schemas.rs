use indexmap::{IndexMap};
use std::collections::HashSet;

use crate::datatypes;
use crate::types::{ColumnEncoding, Datatype, ColumnType, Schema};

pub fn get_schema(
    datatype: &Datatype,
    binary_column_format: &ColumnEncoding,
    include_columns: &Option<Vec<String>>,
    exclude_columns: &Option<Vec<String>>,
) -> Schema {
    let (column_types, default_columns) = match datatype {
        Datatype::Blocks => (
            datatypes::blocks::get_block_column_types(),
            datatypes::blocks::get_default_block_columns(),
        ),
        Datatype::Logs => (
            datatypes::logs::get_log_column_types(),
            datatypes::logs::get_default_log_columns(),
        ),
        Datatype::Transactions => (
            datatypes::transactions::get_transaction_column_types(),
            datatypes::transactions::get_default_transaction_columns(),
        ),
    };

    let used_columns = compute_used_columns(default_columns, include_columns, exclude_columns);
    let mut schema: Schema = IndexMap::new();
    used_columns.iter().for_each(|column| {
        let mut column_type = *column_types.get(column.as_str()).unwrap();
        if (*binary_column_format == ColumnEncoding::Hex) & (column_type == ColumnType::Binary) {
            column_type = ColumnType::Hex;
        }
        schema.insert((*column.clone()).to_string(), column_type);
    });
    schema
}

fn compute_used_columns(
    default_columns: Vec<&str>,
    include_columns: &Option<Vec<String>>,
    exclude_columns: &Option<Vec<String>>,
) -> Vec<String> {
    match (include_columns, exclude_columns) {
        (Some(include), Some(exclude)) => {
            let include_set: HashSet<_> = include.iter().collect();
            let exclude_set: HashSet<_> = exclude.iter().collect();
            let intersection: HashSet<_> = include_set.intersection(&exclude_set).collect();
            assert!(
                intersection.is_empty(),
                "include and exclude should be non-overlapping"
            );
            include.to_vec()
        }
        (Some(include), None) => include.to_vec(),
        (None, Some(exclude)) => {
            let exclude_set: HashSet<_> = exclude.iter().collect();
            default_columns
                .into_iter()
                .filter(|s| !exclude_set.contains(&s.to_string()))
                .map(|s| s.to_string())
                .collect()
        }
        (None, None) => default_columns.iter().map(|s| s.to_string()).collect(),
    }
}

//pub fn df_binary_columns_to_hex(df: &DataFrame) -> Result<DataFrame, PolarsError> {
//    // let mut binary_columns: Vec<Series> = vec![];
//    // let mut binary_columns: Vec<ChunkedArray<Utf8Type>> = vec![];
//    // let mut binary_exprs: Vec<Expr> = vec![];
//    let columns: &[Series] = &df.get_columns();
//    let mut lazy = df.clone().lazy();
//    // let mut df_hex = df.clone();
//    columns.iter().for_each(|column| {
//        match column.dtype() {
//            polars::datatypes::DataType::Binary => {
//                // binary_columns.push(column.binary().unwrap().encode("hex"));
//                // binary_columns.push(column.binary().unwrap().encode("hex"));
//                // let encoded = column.utf8().unwrap().hex_encode();
//                // let encoded = col("hi").binary().encode();
//                // binary_columns.push(encoded.into_series());
//                // binary_exprs.push(column.encode_hex());
//                // df_hex = df_hex.clone().with_column(encoded).unwrap().clone();
//                //
//                let expr = col(column.name()).bin().encode("hex");
//                lazy = lazy.clone().with_column(expr);
//            },
//            _ => ()
//        }
//    });
//    lazy.collect()
//    // Ok(df_hex)
//}

// pub fn df_binary_columns_to_hex(df: &DataFrame) -> Result<DataFrame, PolarsError> {
//     let columns: &[Series] = &df.get_columns();
//     let mut lazy = df.clone().lazy();
//     columns.iter().for_each(|column| {
//         if column.dtype() == &polars::datatypes::DataType::Binary {
//             let expr = lit("0x") + col(column.name()).binary().encode("hex");
//             lazy = lazy.clone().with_column(expr);
//         }
//     });
//     lazy.collect()
// }

