use ethers::prelude::*;
use ring::digest::{self, Digest};

use crate::types::BlockChunk;

pub fn get_subchunks_by_size(block_chunk: &BlockChunk, chunk_size: &u64) -> Vec<BlockChunk> {
    match block_chunk {
        BlockChunk {
            block_numbers: Some(block_numbers),
            ..
        } => block_numbers
            .chunks(*chunk_size as usize)
            .map(|chunk| BlockChunk {
                block_numbers: Some(chunk.to_vec()),
                ..Default::default()
            })
            .collect(),
        BlockChunk {
            start_block: Some(start_block),
            end_block: Some(end_block),
            ..
        } => range_to_chunks(start_block, end_block, chunk_size)
            .iter()
            .map(|(start, end)| BlockChunk {
                start_block: Some(*start),
                end_block: Some(*end),
                block_numbers: None,
            })
            .collect(),
        _ => panic!("invalid block range"),
    }
}

pub fn get_subchunks_by_count(block_chunk: &BlockChunk, n_chunks: &u64) -> Vec<BlockChunk> {
    let total_blocks = get_total_blocks(&[block_chunk.clone()]);

    // ceiling division
    let chunk_size = (total_blocks + n_chunks - 1) / n_chunks;
    get_subchunks_by_size(block_chunk, &chunk_size)
}

/// return a Vec of all blocks in chunk
pub fn get_chunk_block_numbers(block_chunk: &BlockChunk) -> Vec<u64> {
    match block_chunk {
        BlockChunk {
            block_numbers: Some(block_numbers),
            ..
        } => block_numbers.to_vec(),
        BlockChunk {
            start_block: Some(start_block),
            end_block: Some(end_block),
            ..
        } => (*start_block..=*end_block).collect(),
        _ => panic!("invalid block range"),
    }
}

/// break a block chunk into FilterBlockOption for log requests
pub fn block_chunk_to_filter_options(
    block_chunk: &BlockChunk,
    log_request_size: &u64,
) -> Vec<FilterBlockOption> {
    match block_chunk {
        BlockChunk {
            block_numbers: Some(block_numbers),
            ..
        } => block_numbers
            .iter()
            .map(|block| FilterBlockOption::Range {
                from_block: Some((*block).into()),
                to_block: Some((*block).into()),
            })
            .collect(),
        BlockChunk {
            start_block: Some(start_block),
            end_block: Some(end_block),
            ..
        } => {
            let chunks = range_to_chunks(start_block, end_block, log_request_size);
            chunks
                .iter()
                .map(|(start, end)| FilterBlockOption::Range {
                    from_block: Some((*start).into()),
                    to_block: Some((*end).into()),
                })
                .collect()
        }
        _ => panic!("invalid block range"),
    }
}

/// convert a range of numbers into a Vec of (start, end) chunk tuples
fn range_to_chunks(start: &u64, end: &u64, chunk_size: &u64) -> Vec<(u64, u64)> {
    let mut chunks = Vec::new();
    let mut chunk_start = *start;
    while chunk_start < *end {
        let chunk_end = (chunk_start + chunk_size).min(*end) - 1;
        chunks.push((chunk_start, chunk_end));
        chunk_start += chunk_size;
    }
    chunks
}

/// compute a hex hash of a slice of numbers
fn compute_numbers_hash(numbers: &[u64]) -> String {
    let joined_numbers = numbers
        .iter()
        .map(|num| num.to_string())
        .collect::<Vec<String>>()
        .join("");

    let hash: Digest = digest::digest(&digest::SHA256, joined_numbers.as_bytes());

    hex::encode(hash.as_ref())
}

/// convert a block chunk into a String representation
pub fn get_block_chunk_stub(chunk: &BlockChunk) -> String {
    match chunk {
        BlockChunk {
            block_numbers: Some(block_numbers),
            ..
        } => {
            let min = block_numbers.iter().min().unwrap();
            let max = block_numbers.iter().max().unwrap();
            let hash = compute_numbers_hash(block_numbers);
            format!("mixed_{}_to_{}_{}", min, max, &hash[0..8].to_string())
        }
        BlockChunk {
            start_block: Some(start_block),
            end_block: Some(end_block),
            ..
        } => format!("{}_to_{}", start_block, end_block),
        _ => panic!("invalid block range"),
    }
}

pub fn get_total_blocks(block_chunks: &[BlockChunk]) -> u64 {
    let mut total = 0;
    block_chunks.iter().for_each(|chunk| {
        total += match chunk {
            BlockChunk {
                block_numbers: Some(block_numbers),
                ..
            } => block_numbers.len() as u64,
            BlockChunk {
                start_block: Some(start_block),
                end_block: Some(end_block),
                ..
            } => end_block - start_block + 1,
            _ => panic!("invalid BlockChunk"),
        }
    });
    total
}

pub fn get_min_block(block_chunks: &[BlockChunk]) -> u64 {
    let mut block_min = std::u64::MAX;
    block_chunks.iter().for_each(|chunk| {
        let chunk_min = match chunk {
            BlockChunk {
                block_numbers: Some(block_numbers),
                ..
            } => *block_numbers.iter().min().unwrap(),
            BlockChunk {
                start_block: Some(start_block),
                end_block: Some(_end_block),
                ..
            } => *start_block,
            _ => panic!("invalid BlockChunk"),
        };
        block_min = std::cmp::min(chunk_min, block_min);
    });
    block_min
}

pub fn get_max_block(block_chunks: &[BlockChunk]) -> u64 {
    let mut block_max = std::u64::MIN;
    block_chunks.iter().for_each(|chunk| {
        let chunk_max = match chunk {
            BlockChunk {
                block_numbers: Some(block_numbers),
                ..
            } => *block_numbers.iter().max().unwrap(),
            BlockChunk {
                start_block: Some(_start_block),
                end_block: Some(end_block),
                ..
            } => *end_block,
            _ => panic!("invalid BlockChunk"),
        };
        block_max = std::cmp::max(chunk_max, block_max);
    });
    block_max
}
