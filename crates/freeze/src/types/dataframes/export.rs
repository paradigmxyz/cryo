use std::path::Path;

use polars::prelude::*;

use crate::types::{FileError, FileOutput};

/// write polars dataframe to file
pub(crate) fn df_to_file(
    df: &mut DataFrame,
    filename: &Path,
    file_output: &FileOutput,
) -> Result<(), FileError> {
    let tmp_filename = filename.with_extension("_tmp");
    let result = match filename.extension().and_then(|ex| ex.to_str()) {
        Some("parquet") => df_to_parquet(df, &tmp_filename, file_output),
        Some("csv") => df_to_csv(df, &tmp_filename),
        Some("json") => df_to_json(df, &tmp_filename),
        _ => return Err(FileError::FileWriteError),
    };
    match result {
        Ok(()) => std::fs::rename(tmp_filename, filename).map_err(|_e| FileError::FileWriteError),
        Err(_e) => Err(FileError::FileWriteError),
    }
}

/// write polars dataframe to parquet file
fn df_to_parquet(
    df: &mut DataFrame,
    filename: &Path,
    file_output: &FileOutput,
) -> Result<(), FileError> {
    let file = std::fs::File::create(filename).map_err(|_e| FileError::FileWriteError)?;
    let result = ParquetWriter::new(file)
        .with_statistics(file_output.parquet_statistics)
        .with_compression(file_output.parquet_compression)
        .with_row_group_size(file_output.row_group_size)
        .finish(df);
    match result {
        Err(_e) => Err(FileError::FileWriteError),
        _ => Ok(()),
    }
}

/// write polars dataframe to csv file
fn df_to_csv(df: &mut DataFrame, filename: &Path) -> Result<(), FileError> {
    let file = std::fs::File::create(filename).map_err(|_e| FileError::FileWriteError)?;
    let result = CsvWriter::new(file).finish(df);
    match result {
        Err(_e) => Err(FileError::FileWriteError),
        _ => Ok(()),
    }
}

/// write polars dataframe to json file
fn df_to_json(df: &mut DataFrame, filename: &Path) -> Result<(), FileError> {
    let file = std::fs::File::create(filename).map_err(|_e| FileError::FileWriteError)?;
    let result = JsonWriter::new(file).with_json_format(JsonFormat::Json).finish(df);
    match result {
        Err(_e) => Err(FileError::FileWriteError),
        _ => Ok(()),
    }
}
