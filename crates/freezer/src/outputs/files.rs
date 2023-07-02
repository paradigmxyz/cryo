use polars::prelude::*;

use crate::chunks;
use crate::types::FileError;
use crate::types::{BlockChunk, FreezeOpts};

/// get file path of output chunk
pub fn get_chunk_path(
    name: &str,
    chunk: &BlockChunk,
    opts: &FreezeOpts,
) -> Result<String, FileError> {
    let block_chunk_stub = chunks::get_block_chunk_stub(chunk).map_err(FileError::FilePathError)?;
    let filename = format!(
        "{}__{}__{}.{}",
        opts.network_name,
        name,
        block_chunk_stub,
        opts.output_format.as_str()
    );
    match opts.output_dir.as_str() {
        "." => Ok(filename),
        output_dir => Ok(output_dir.to_string() + "/" + filename.as_str()),
    }
}

/// write polars dataframe to file
pub fn df_to_file(df: &mut DataFrame, filename: &str, opts: &FreezeOpts) -> Result<(), FileError> {
    let binding = filename.to_string() + "_tmp";
    let tmp_filename = binding.as_str();
    let result = match filename {
        _ if filename.ends_with(".parquet") => df_to_parquet(df, tmp_filename, opts),
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
fn df_to_parquet(df: &mut DataFrame, filename: &str, opts: &FreezeOpts) -> Result<(), FileError> {
    let file = std::fs::File::create(filename).map_err(|_e| FileError::FileWriteError)?;
    let result = ParquetWriter::new(file)
        .with_statistics(opts.parquet_statistics)
        .with_compression(opts.parquet_compression)
        .with_row_group_size(opts.row_group_size)
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
    let result = JsonWriter::new(file).finish(df);
    match result {
        Err(_e) => Err(FileError::FileWriteError),
        _ => Ok(()),
    }
}
