use std::collections::HashMap;
use indexmap::IndexMap;
use ethers::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Datatype {
    Blocks,
    Logs,
    Transactions,
}

impl Datatype {
    pub fn as_str(&self) -> &'static str {
        match *self {
            Datatype::Blocks => "blocks",
            Datatype::Logs => "logs",
            Datatype::Transactions => "transactions",
        }
    }

    pub fn default_sort(&self) -> Vec<String> {
        let columns = match *self {
            Datatype::Blocks => vec!["block_number"],
            Datatype::Logs => vec!["block_number", "log_index"],
            Datatype::Transactions => vec!["block_nubmer", "transaction_index"],
        };
        columns.iter().map(|column| {column.to_string()}).collect()
    }
}

#[derive(Clone)]
pub enum FileFormat {
    Parquet,
    Csv,
}

impl FileFormat {
    pub fn as_str(&self) -> &'static str {
        match *self {
            FileFormat::Parquet => "parquet",
            FileFormat::Csv => "csv",
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum ColumnEncoding {
    Binary,
    Hex,
}

impl ColumnEncoding {
    pub fn as_str(&self) -> &'static str {
        match *self {
            ColumnEncoding::Binary => "binary",
            ColumnEncoding::Hex => "hex",
        }
    }
}

#[derive(Clone)]
pub struct FreezeOpts {
    pub datatypes: Vec<Datatype>,
    pub provider: Provider<Http>,
    pub block_chunks: Vec<BlockChunk>,
    pub network_name: String,
    pub max_concurrent_chunks: u64,
    pub max_concurrent_blocks: u64,
    pub log_request_size: u64,
    pub binary_column_format: ColumnEncoding,
    pub schemas: HashMap<Datatype, Schema>,
    pub output_dir: String,
    pub output_format: FileFormat,
    pub dry_run: bool,
    pub sort: HashMap<Datatype, Vec<String>>,
    pub row_groups: Option<u64>,
    pub row_group_size: Option<u64>,
    pub parquet_statistics: bool,
}

#[derive(Default, Clone)]
pub struct BlockChunk {
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub block_numbers: Option<Vec<u64>>,
}

#[derive(Default)]
pub struct SlimBlock {
    pub number: u64,
    pub hash: Vec<u8>,
    pub author: Vec<u8>,
    pub gas_used: u64,
    pub extra_data: Vec<u8>,
    pub timestamp: u64,
    pub base_fee_per_gas: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnType {
    Int32,
    Int64,
    Decimal128,
    Float64,
    String,
    Binary,
    Hex,
}

impl ColumnType {
    pub fn as_str(&self) -> &'static str {
        match *self {
            ColumnType::Int32 => "int32",
            ColumnType::Int64 => "int64",
            ColumnType::Decimal128 => "decimal128",
            ColumnType::Float64 => "float64",
            ColumnType::String => "string",
            ColumnType::Binary => "binary",
            ColumnType::Hex => "hex",
        }
    }
}


pub type Schema = IndexMap<String, ColumnType>;
