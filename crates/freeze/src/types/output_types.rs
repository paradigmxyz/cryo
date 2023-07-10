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
