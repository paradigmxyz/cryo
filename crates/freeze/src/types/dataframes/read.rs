use crate::{ParseError, RawBytes};
use polars::prelude::*;

/// read single binary column of parquet file as RawBytes
pub fn read_binary_column(path: &str, column: &str) -> Result<Vec<RawBytes>, ParseError> {
    let file = std::fs::File::open(path)
        .map_err(|_e| ParseError::ParseError("could not open file path".to_string()))?;

    let df = ParquetReader::new(file)
        .with_columns(Some(vec![column.to_string()]))
        .finish()
        .map_err(|_e| ParseError::ParseError("could not read data from column".to_string()))?;

    let series = df
        .column(column)
        .map_err(|_e| ParseError::ParseError("could not get column".to_string()))?
        .unique()
        .map_err(|_e| ParseError::ParseError("could not get column".to_string()))?;

    let ca = series
        .binary()
        .map_err(|_e| ParseError::ParseError("could not convert to binary column".to_string()))?;

    ca.into_iter()
        .map(|value| {
            value
                .ok_or_else(|| ParseError::ParseError("transaction hash missing".to_string()))
                .map(|data| data.into())
        })
        .collect()
}
