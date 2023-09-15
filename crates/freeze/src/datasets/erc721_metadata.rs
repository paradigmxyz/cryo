use crate::{types::Erc721Metadata, ColumnType, Dataset, Datatype};
use std::collections::HashMap;

impl Dataset for Erc721Metadata {
    fn datatype(&self) -> Datatype {
        Datatype::Erc721Metadata
    }

    fn name(&self) -> &'static str {
        "erc721_metadata"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("erc20", ColumnType::Binary),
            ("name", ColumnType::String),
            ("symbol", ColumnType::String),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec!["block_number", "erc20", "name", "symbol", "chain_id"]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["symbol".to_string(), "block_number".to_string()]
    }
}
