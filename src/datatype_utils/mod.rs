use crate::types::{ColumnEncoding, Datatype, ColumnType, Schema};
use indexmap::{IndexMap};
use std::collections::HashSet;

mod block_utils;
mod log_utils;
mod transaction_utils;

pub fn get_schema(
    datatype: &Datatype,
    binary_column_format: &ColumnEncoding,
    include_columns: &Option<Vec<String>>,
    exclude_columns: &Option<Vec<String>>,
) -> Schema {
    let (column_types, default_columns) = match datatype {
        Datatype::Blocks => (
            block_utils::get_block_column_types(),
            block_utils::get_default_block_columns(),
        ),
        Datatype::Logs => (
            log_utils::get_log_column_types(),
            log_utils::get_default_log_columns(),
        ),
        Datatype::Transactions => (
            transaction_utils::get_transaction_column_types(),
            transaction_utils::get_default_transaction_columns(),
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
