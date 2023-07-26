use std::sync::Arc;

use ethers::prelude::*;

use cryo_freeze::{BlockChunk, Chunk, ChunkData, ParseError, Subchunk};

use crate::args::Args;

pub(crate) async fn parse_blocks(
    args: &Args,
    provider: Arc<Provider<Http>>,
) -> Result<Vec<Chunk>, ParseError> {
    let block_chunks = parse_block_inputs(&args.blocks, &provider).await?;
    let block_chunks = if args.align {
        block_chunks.into_iter().filter_map(|x| x.align(args.chunk_size)).collect()
    } else {
        block_chunks
    };
    let block_chunks = match args.n_chunks {
        Some(n_chunks) => block_chunks.subchunk_by_count(&n_chunks),
        None => block_chunks.subchunk_by_size(&args.chunk_size),
    };
    let block_chunks = apply_reorg_buffer(block_chunks, args.reorg_buffer, &provider).await?;
    let chunks: Vec<Chunk> = block_chunks.iter().map(|x| Chunk::Block(x.clone())).collect();
    Ok(chunks)
}

/// parse block numbers to freeze
async fn parse_block_inputs(
    inputs: &Vec<String>,
    provider: &Provider<Http>,
) -> Result<Vec<BlockChunk>, ParseError> {
    match inputs.len() {
        1 => {
            let first_input = inputs.get(0).ok_or_else(|| {
                ParseError::ParseError("Failed to get the first input".to_string())
            })?;
            parse_block_token(first_input, true, provider).await.map(|x| vec![x])
        }
        _ => {
            let mut chunks = Vec::new();
            for input in inputs {
                chunks.push(parse_block_token(input, false, provider).await?);
            }
            Ok(chunks)
        }
    }
}

enum RangePosition {
    First,
    Last,
    None,
}

async fn parse_block_token(
    s: &str,
    as_range: bool,
    provider: &Provider<Http>,
) -> Result<BlockChunk, ParseError> {
    let s = s.replace('_', "");
    let parts: Vec<&str> = s.split(':').collect();
    match parts.as_slice() {
        [block_ref] => {
            let block = parse_block_number(block_ref, RangePosition::None, provider).await?;
            Ok(BlockChunk::Numbers(vec![block]))
        }
        [first_ref, second_ref] => {
            let (start_block, end_block) = match (first_ref, second_ref) {
                _ if first_ref.starts_with('-') => {
                    let end_block =
                        parse_block_number(second_ref, RangePosition::Last, provider).await?;
                    let start_block = end_block
                        .checked_sub(first_ref[1..].parse::<u64>().map_err(|_e| {
                            ParseError::ParseError("start_block parse error".to_string())
                        })?)
                        .ok_or_else(|| {
                            ParseError::ParseError("start_block underflow".to_string())
                        })?;
                    (start_block, end_block)
                }
                _ if second_ref.starts_with('+') => {
                    let start_block =
                        parse_block_number(first_ref, RangePosition::First, provider).await?;
                    let end_block = start_block
                        .checked_add(second_ref[1..].parse::<u64>().map_err(|_e| {
                            ParseError::ParseError("start_block parse error".to_string())
                        })?)
                        .ok_or_else(|| ParseError::ParseError("end_block underflow".to_string()))?;
                    (start_block, end_block)
                }
                _ => {
                    let start_block =
                        parse_block_number(first_ref, RangePosition::First, provider).await?;
                    let end_block =
                        parse_block_number(second_ref, RangePosition::Last, provider).await?;
                    (start_block, end_block)
                }
            };

            if end_block <= start_block {
                Err(ParseError::ParseError(
                    "end_block should not be less than start_block".to_string(),
                ))
            } else if as_range {
                Ok(BlockChunk::Range(start_block, end_block))
            } else {
                Ok(BlockChunk::Numbers((start_block..=end_block).collect()))
            }
        }
        _ => Err(ParseError::ParseError(
            "blocks must be in format block_number or start_block:end_block".to_string(),
        )),
    }
}

async fn parse_block_number(
    block_ref: &str,
    range_position: RangePosition,
    provider: &Provider<Http>,
) -> Result<u64, ParseError> {
    match (block_ref, range_position) {
        ("latest", _) => provider.get_block_number().await.map(|n| n.as_u64()).map_err(|_e| {
            ParseError::ParseError("Error retrieving latest block number".to_string())
        }),
        ("", RangePosition::First) => Ok(0),
        ("", RangePosition::Last) => {
            provider.get_block_number().await.map(|n| n.as_u64()).map_err(|_e| {
                ParseError::ParseError("Error retrieving last block number".to_string())
            })
        }
        ("", RangePosition::None) => Err(ParseError::ParseError("invalid input".to_string())),
        _ if block_ref.ends_with('B') | block_ref.ends_with('b') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e9 * n) as u64)
                .map_err(|_e| ParseError::ParseError("Error parsing block ref".to_string()))
        }
        _ if block_ref.ends_with('M') | block_ref.ends_with('m') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e6 * n) as u64)
                .map_err(|_e| ParseError::ParseError("Error parsing block ref".to_string()))
        }
        _ if block_ref.ends_with('K') | block_ref.ends_with('k') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e3 * n) as u64)
                .map_err(|_e| ParseError::ParseError("Error parsing block ref".to_string()))
        }
        _ => block_ref
            .parse::<f64>()
            .map_err(|_e| ParseError::ParseError("Error parsing block ref".to_string()))
            .map(|x| x as u64),
    }
}

async fn apply_reorg_buffer(
    block_chunks: Vec<BlockChunk>,
    reorg_filter: u64,
    provider: &Provider<Http>,
) -> Result<Vec<BlockChunk>, ParseError> {
    match reorg_filter {
        0 => Ok(block_chunks),
        reorg_filter => {
            let latest_block = match provider.get_block_number().await {
                Ok(result) => result.as_u64(),
                Err(_e) => {
                    return Err(ParseError::ParseError("reorg buffer parse error".to_string()))
                }
            };
            let max_allowed = latest_block - reorg_filter;
            Ok(block_chunks
                .into_iter()
                .filter_map(|x| match x.max_value() {
                    Some(max_block) if max_block <= max_allowed => Some(x),
                    _ => None,
                })
                .collect())
        }
    }
}
