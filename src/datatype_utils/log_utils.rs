use crate::types::ColumnType;
use std::collections::HashMap;

pub fn get_default_log_columns() -> Vec<&'static str> {
    vec![
        "block_number",
        "timestamp",
        "block_hash",
        "author",
        "extra_data",
    ]
}

pub fn get_log_column_types() -> HashMap<&'static str, ColumnType> {
    HashMap::from_iter(vec![
        ("block_number", ColumnType::Int32),
        ("timestamp", ColumnType::Int32),
        ("block_hash", ColumnType::Binary),
        ("author", ColumnType::Binary),
        ("extra_data", ColumnType::Binary),
    ])
}
