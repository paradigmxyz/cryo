use crate::types::ColumnType;
use std::collections::HashMap;

pub fn get_default_block_columns() -> Vec<&'static str> {
    vec![
        "block_number",
        "block_hash",
        "timestamp",
        "author",
        "gas_used",
        "extra_data",
        "base_fee_per_gas",
    ]
}

pub fn get_block_column_types() -> HashMap<&'static str, ColumnType> {
    HashMap::from_iter(vec![
        ("block_number", ColumnType::Int32),
        ("block_hash", ColumnType::Binary),
        ("timestamp", ColumnType::Int32),
        ("author", ColumnType::Binary),
        ("gas_used", ColumnType::Int32),
        ("extra_data", ColumnType::Binary),
        ("base_fee_per_gas", ColumnType::Int64),
    ])
}
