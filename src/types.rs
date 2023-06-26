use std::collections::HashMap;
use std::collections::HashSet;


use ethers::prelude::*;
use indexmap::IndexMap;
use polars::prelude::*;
use async_trait;

pub struct Blocks;
pub struct Logs;
pub struct Transactions;

#[async_trait::async_trait]
pub trait Dataset: Sync + Send {
    fn name(&self) -> &'static str;
    fn get_block_column_types(&self) -> HashMap<&'static str, ColumnType>;
    fn get_default_block_columns(&self) -> Vec<&'static str>;
    fn get_default_sort(&self) -> Vec<&'static str>;
    async fn collect_dataset(&self, _block_chunk: &BlockChunk, _opts: &FreezeOpts) -> DataFrame;
    // async fn collect_datasets(
    //     &self,
    //     block_chunk: &BlockChunk,
    //     extra_datsets: &Vec<Box<dyn Dataset>>,
    //     opts: &FreezeOpts,
    // ) -> Vec<&mut DataFrame> {
    //     if extra_datsets.len() > 0 {
    //         panic!("custom collect_datasets() required when using extra_datasets")
    //     }
    //     vec![self.collect_dataset(block_chunk, opts).await]
    // }

    fn get_schema(
        &self,
        binary_column_format: &ColumnEncoding,
        include_columns: &Option<Vec<String>>,
        exclude_columns: &Option<Vec<String>>,
    ) -> Schema {
        let column_types = self.get_block_column_types();
        let default_columns = self.get_default_block_columns();
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
        columns.iter().map(|column| column.to_string()).collect()
    }

    pub fn get_dataset(&self) -> Box<dyn Dataset> {
        match *self {
            Datatype::Blocks => Box::new(Blocks),
            Datatype::Logs => Box::new(Logs),
            Datatype::Transactions => Box::new(Transactions),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum FileFormat {
    Parquet,
    Csv,
    Json,
}

impl FileFormat {
    pub fn as_str(&self) -> &'static str {
        match *self {
            FileFormat::Parquet => "parquet",
            FileFormat::Csv => "csv",
            FileFormat::Json => "json",
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
