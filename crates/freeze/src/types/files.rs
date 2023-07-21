use polars::prelude::*;

/// Options for file output
#[derive(Clone)]
pub struct FileOutput {
    /// Path of directory where to save files
    pub output_dir: String,
    /// Prefix of file name
    pub prefix: String,
    /// Suffix to use at the end of file names
    pub suffix: Option<String>,
    /// Whether to overwrite existing files or skip them
    pub overwrite: bool,
    /// File format to used for output files
    pub format: FileFormat,
    /// Number of rows per parquet row group
    pub row_group_size: Option<usize>,
    /// Parquet statistics recording flag
    pub parquet_statistics: bool,
    /// Parquet compression options
    pub parquet_compression: ParquetCompression,
}

/// File format
#[derive(Clone, Eq, PartialEq)]
pub enum FileFormat {
    /// Parquet file format
    Parquet,
    /// Csv file format
    Csv,
    /// Json file format
    Json,
}

impl FileFormat {
    /// convert FileFormat to str
    pub fn as_str(&self) -> &'static str {
        match *self {
            FileFormat::Parquet => "parquet",
            FileFormat::Csv => "csv",
            FileFormat::Json => "json",
        }
    }
}

/// Encoding for binary data in a column
#[derive(Clone, Eq, PartialEq)]
pub enum ColumnEncoding {
    /// Raw binary encoding
    Binary,
    /// Hex binary encoding
    Hex,
}

impl ColumnEncoding {
    /// convert ColumnEncoding to str
    pub fn as_str(&self) -> &'static str {
        match *self {
            ColumnEncoding::Binary => "binary",
            ColumnEncoding::Hex => "hex",
        }
    }
}
