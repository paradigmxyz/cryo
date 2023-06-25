use crate::types::ColumnType;
use std::collections::HashMap;

pub fn get_default_transaction_columns() -> Vec<&'static str> {
    vec![
        "block_number",
        "transaction_index",
        "transaction_hash",
        "nonce",
        "from_address",
        "to_address",
        "value",
        "input",
        "gas_limit",
        "gas_price",
        "transaction_type",
        "max_priority_fee_per_gas",
        "max_fee_per_gas",
        "chain_id",
    ]
}

pub fn get_transaction_column_types() -> HashMap<&'static str, ColumnType> {
    HashMap::from_iter(vec![
        ("block_number", ColumnType::Int32),
        ("transaction_index", ColumnType::Int32),
        ("transaction_hash", ColumnType::Binary),
        ("nonce", ColumnType::Int32),
        ("from_address", ColumnType::Binary),
        ("to_address", ColumnType::Binary),
        ("value", ColumnType::Decimal128),
        ("value_str", ColumnType::String),
        ("value_float", ColumnType::Float64),
        ("input", ColumnType::Binary),
        ("gas_limit", ColumnType::Int64),
        ("gas_price", ColumnType::Int64),
        ("transaction_type", ColumnType::Int32),
        ("max_priority_fee_per_gas", ColumnType::Int64),
        ("max_fee_per_gas", ColumnType::Int64),
        ("chain_id", ColumnType::Int64),
    ])
}
