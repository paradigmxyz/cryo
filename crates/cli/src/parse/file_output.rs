use std::fs;

use polars::prelude::*;

use cryo_freeze::{FileFormat, FileOutput, ParseError, Source};

use crate::args::Args;

pub(crate) fn parse_file_output(args: &Args, source: &Source) -> Result<FileOutput, ParseError> {
    // process output directory
    let output_dir = std::fs::canonicalize(args.output_dir.clone()).map_err(|_e| {
        ParseError::ParseError("Failed to canonicalize output directory".to_string())
    })?;
    match fs::create_dir_all(&output_dir) {
        Ok(_) => {}
        Err(e) => return Err(ParseError::ParseError(format!("Error creating directory: {}", e))),
    };

    let file_suffix = &args.file_suffix;

    let parquet_compression = parse_compression(&args.compression)?;

    let row_group_size = parse_row_group_size(
        args.row_group_size,
        args.n_row_groups,
        Some(args.chunk_size as usize),
    );

    let format = parse_output_format(args)?;
    let file_prefix = parse_network_name(args, source.chain_id);

    let output = FileOutput {
        output_dir,
        parquet_statistics: !args.no_stats,
        overwrite: args.overwrite,
        prefix: file_prefix,
        format,
        suffix: file_suffix.clone(),
        parquet_compression,
        row_group_size,
    };

    Ok(output)
}

pub(crate) fn parse_network_name(args: &Args, chain_id: u64) -> String {
    match &args.network_name {
        Some(name) => name.clone(),
        None => match chain_id {
            1 => "ethereum".to_string(),
            10 => "optimism".to_string(),
            56 => "binance".to_string(),
            137 => "polygon".to_string(),
            42161 => "arbitrum".to_string(),
            43114 => "avalanche".to_string(),
            11155111 => "sepolia".to_string(),
            chain_id => "network_".to_string() + chain_id.to_string().as_str(),
        },
    }
}

pub(crate) fn parse_output_format(args: &Args) -> Result<FileFormat, ParseError> {
    match (args.csv, args.json) {
        (true, true) => {
            Err(ParseError::ParseError("choose one of parquet, csv, or json".to_string()))
        }
        (true, _) => Ok(FileFormat::Csv),
        (_, true) => Ok(FileFormat::Json),
        (false, false) => Ok(FileFormat::Parquet),
    }
}

fn parse_compression(input: &Vec<String>) -> Result<ParquetCompression, ParseError> {
    match input.as_slice() {
        [algorithm] if algorithm.as_str() == "uncompressed" => Ok(ParquetCompression::Uncompressed),
        [algorithm] if algorithm.as_str() == "snappy" => Ok(ParquetCompression::Snappy),
        [algorithm] if algorithm.as_str() == "lzo" => Ok(ParquetCompression::Lzo),
        [algorithm] if algorithm.as_str() == "lz4" => Ok(ParquetCompression::Lz4Raw),
        [algorithm, level_str] if algorithm.as_str() == "gzip" => match level_str.parse::<u8>() {
            Ok(level) => match GzipLevel::try_new(level) {
                Ok(gzip_level) => Ok(ParquetCompression::Gzip(Some(gzip_level))),
                Err(_) => Err(ParseError::ParseError("Invalid compression level".to_string())),
            },
            Err(_) => Err(ParseError::ParseError("Invalid compression level".to_string())),
        },
        [algorithm, level_str] if algorithm.as_str() == "brotli" => {
            match level_str.parse::<u32>() {
                Ok(level) => match BrotliLevel::try_new(level) {
                    Ok(brotli_level) => Ok(ParquetCompression::Brotli(Some(brotli_level))),
                    Err(_) => Err(ParseError::ParseError("Invalid compression level".to_string())),
                },
                Err(_) => Err(ParseError::ParseError("Invalid compression level".to_string())),
            }
        }
        [algorithm, level_str] if algorithm.as_str() == "zstd" => match level_str.parse::<i32>() {
            Ok(level) => match ZstdLevel::try_new(level) {
                Ok(zstd_level) => Ok(ParquetCompression::Zstd(Some(zstd_level))),
                Err(_) => Err(ParseError::ParseError("Invalid compression level".to_string())),
            },
            Err(_) => Err(ParseError::ParseError("Invalid compression level".to_string())),
        },
        [algorithm] if ["gzip", "brotli", "zstd"].contains(&algorithm.as_str()) => {
            Err(ParseError::ParseError("Missing compression level".to_string()))
        }
        _ => Err(ParseError::ParseError("Invalid compression algorithm".to_string())),
    }
}

fn parse_row_group_size(
    row_group_size: Option<usize>,
    n_row_groups: Option<usize>,
    chunk_size: Option<usize>,
) -> Option<usize> {
    match (row_group_size, n_row_groups, chunk_size) {
        (Some(row_group_size), _, _) => Some(row_group_size),
        (_, Some(n_row_groups), Some(cs)) => Some((cs + n_row_groups - 1) / n_row_groups),
        _ => None,
    }
}
