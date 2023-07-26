use std::collections::HashMap;

use polars::prelude::*;

use crate::types::{FileError, FileOutput};

pub(crate) fn dfs_to_files<T>(
    dfs: &mut HashMap<T, DataFrame>,
    filenames: &HashMap<T, String>,
    file_output: &FileOutput,
) -> Result<(), FileError>
where
    T: std::cmp::Eq,
    T: std::hash::Hash,
{
    for (name, df) in dfs.iter_mut() {
        let filename = match filenames.get(name) {
            Some(filename) => filename,
            None => {
                return Err(FileError::NoFilePathError("no path given for dataframe".to_string()))
            }
        };
        df_to_file(df, filename, file_output)?
    }
    Ok(())
}

/// write polars dataframe to file
pub(crate) fn df_to_file(
    df: &mut DataFrame,
    filename: &str,
    file_output: &FileOutput,
) -> Result<(), FileError> {
    let binding = filename.to_string() + "_tmp";
    let tmp_filename = binding.as_str();
    let result = match filename {
        _ if filename.ends_with(".parquet") => df_to_parquet(df, tmp_filename, file_output),
        _ if filename.ends_with(".csv") => df_to_csv(df, tmp_filename),
        _ if filename.ends_with(".json") => df_to_json(df, tmp_filename),
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
    filename: &str,
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
fn df_to_csv(df: &mut DataFrame, filename: &str) -> Result<(), FileError> {
    let file = std::fs::File::create(filename).map_err(|_e| FileError::FileWriteError)?;
    let result = CsvWriter::new(file).finish(df);
    match result {
        Err(_e) => Err(FileError::FileWriteError),
        _ => Ok(()),
    }
}

/// write polars dataframe to json file
fn df_to_json(df: &mut DataFrame, filename: &str) -> Result<(), FileError> {
    let file = std::fs::File::create(filename).map_err(|_e| FileError::FileWriteError)?;
    let result = JsonWriter::new(file).with_json_format(JsonFormat::Json).finish(df);
    match result {
        Err(_e) => Err(FileError::FileWriteError),
        _ => Ok(()),
    }
}
