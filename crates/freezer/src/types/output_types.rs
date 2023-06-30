

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

// #[derive(Default, Debug, Clone)]
// pub struct BlockChunk {
//     pub start_block: Option<u64>,
//     pub end_block: Option<u64>,
//     pub block_numbers: Option<Vec<u64>>,
// }

#[derive(Debug, Clone)]
pub enum BlockChunk {
    Numbers(Vec<u64>),
    Range(u64, u64),
}
