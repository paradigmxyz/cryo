use crate::types::ColumnType;
use std::collections::HashMap;

pub fn get_default_log_columns() -> Vec<&'static str> {
    vec![
        "block_number",
        "transaction_index",
        "log_index",
        "transaction_hash",
        "contract_address",
        "topic0",
        "topic1",
        "topic2",
        "topic3",
        "data",
    ]
}

pub fn get_log_column_types() -> HashMap<&'static str, ColumnType> {
    HashMap::from_iter(vec![
        ("block_number", ColumnType::Int32),
        ("transaction_index", ColumnType::Int32),
        ("log_index", ColumnType::Int32),
        ("transaction_hash", ColumnType::Binary),
        ("contract_address", ColumnType::Binary),
        ("topic0", ColumnType::Binary),
        ("topic1", ColumnType::Binary),
        ("topic2", ColumnType::Binary),
        ("topic3", ColumnType::Binary),
        ("data", ColumnType::Binary),
    ])
}
