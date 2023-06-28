use polars::prelude::*;
use crate::chunks;
use crate::types::{BlockChunk,FreezeOpts};

/// get file path of output chunk
pub fn get_chunk_path(name: &str, chunk: &BlockChunk, opts: &FreezeOpts) -> String {
    let block_chunk_stub = chunks::get_block_chunk_stub(chunk);
    let filename = format!(
        "{}__{}__{}.{}",
        opts.network_name,
        name,
        block_chunk_stub,
        opts.output_format.as_str()
    );
    match opts.output_dir.as_str() {
        "." => filename,
        output_dir => output_dir.to_string() + "/" + filename.as_str(),
    }
}

/// write polars dataframe to file
pub fn df_to_file(df: &mut DataFrame, filename: &str, opts: &FreezeOpts) {
    let binding = filename.to_string() + "_tmp";
    let tmp_filename = binding.as_str();
    match filename {
        _ if filename.ends_with(".parquet") => df_to_parquet(df, tmp_filename, &opts),
        _ if filename.ends_with(".csv") => df_to_csv(df, tmp_filename),
        _ if filename.ends_with(".json") => df_to_json(df, tmp_filename),
        _ => panic!("invalid file format")
    }
    std::fs::rename(tmp_filename, filename).unwrap();
}

/// write polars dataframe to parquet file
fn df_to_parquet(df: &mut DataFrame, filename: &str, opts: &FreezeOpts) {
    let file = std::fs::File::create(filename).unwrap();
    ParquetWriter::new(file)
        .with_statistics(opts.parquet_statistics)
        .with_compression(opts.parquet_compression)
        .with_row_group_size(opts.row_group_size)
        .finish(df)
        .unwrap();
}

/// write polars dataframe to csv file
fn df_to_csv(df: &mut DataFrame, filename: &str) {
    let file = std::fs::File::create(filename).unwrap();
    CsvWriter::new(file)
        .finish(df)
        .unwrap();
}

/// write polars dataframe to json file
fn df_to_json(df: &mut DataFrame, filename: &str) {
    let file = std::fs::File::create(filename).unwrap();
    JsonWriter::new(file)
        .finish(df)
        .unwrap();
}
