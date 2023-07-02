use ethers::prelude::*;
use ring::digest::{self, Digest};

use crate::types::error_types;
use crate::types::BlockChunk;

pub fn get_subchunks_by_size(block_chunk: &BlockChunk, chunk_size: &u64) -> Vec<BlockChunk> {
    match block_chunk {
        BlockChunk::Numbers(numbers) => numbers
            .chunks(*chunk_size as usize)
            .map(|chunk| BlockChunk::Numbers(chunk.to_vec()))
            .collect(),
        BlockChunk::Range(start_block, end_block) => {
            range_to_chunks(start_block, end_block, chunk_size)
                .iter()
                .map(|(start, end)| BlockChunk::Range(*start, *end))
                .collect()
        }
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
        BlockChunk::Numbers(numbers) => numbers.to_vec(),
        BlockChunk::Range(start_block, end_block) => (*start_block..=*end_block).collect(),
    }
}

/// break a block chunk into FilterBlockOption for log requests
pub fn block_chunk_to_filter_options(
    block_chunk: &BlockChunk,
    log_request_size: &u64,
) -> Vec<FilterBlockOption> {
    match block_chunk {
        BlockChunk::Numbers(block_numbers) => block_numbers
            .iter()
            .map(|block| FilterBlockOption::Range {
                from_block: Some((*block).into()),
                to_block: Some((*block).into()),
            })
            .collect(),
        BlockChunk::Range(start_block, end_block) => {
            let chunks = range_to_chunks(start_block, end_block, log_request_size);
            chunks
                .iter()
                .map(|(start, end)| FilterBlockOption::Range {
                    from_block: Some((*start).into()),
                    to_block: Some((*end).into()),
                })
                .collect()
        }
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
pub fn get_block_chunk_stub(chunk: &BlockChunk) -> Result<String, error_types::ChunkError> {
    match chunk {
        BlockChunk::Numbers(numbers) => match (numbers.iter().min(), numbers.iter().max()) {
            (Some(min), Some(max)) => {
                let hash = compute_numbers_hash(numbers);
                Ok(format!(
                    "mixed_{}_to_{}_{}",
                    min,
                    max,
                    &hash[0..8].to_string()
                ))
            }
            _ => Err(error_types::ChunkError::StubError),
        },
        BlockChunk::Range(start_block, end_block) => {
            Ok(format!("{}_to_{}", start_block, end_block))
        }
    }
}

pub fn get_total_blocks(block_chunks: &[BlockChunk]) -> u64 {
    let mut total = 0;
    block_chunks.iter().for_each(|chunk| {
        total += match chunk {
            BlockChunk::Numbers(numbers) => numbers.len() as u64,
            BlockChunk::Range(start_block, end_block) => end_block - start_block + 1,
        }
    });
    total
}

pub fn get_min_block(block_chunks: &[BlockChunk]) -> Option<u64> {
    if block_chunks.is_empty() {
        None
    } else {
        let mut block_min = std::u64::MAX;
        block_chunks.iter().for_each(|chunk| {
            let chunk_min = match chunk {
                BlockChunk::Numbers(numbers) => numbers.iter().min(),
                BlockChunk::Range(start_block, _) => Some(start_block),
            };
            if let Some(&chunk_min) = chunk_min {
                block_min = std::cmp::min(chunk_min, block_min);
            }
        });
        Some(block_min)
    }
}

pub fn get_max_block(block_chunks: &[BlockChunk]) -> Option<u64> {
    if block_chunks.is_empty() {
        None
    } else {
        let mut block_max = std::u64::MIN;
        block_chunks.iter().for_each(|chunk| {
            let chunk_max = match chunk {
                BlockChunk::Numbers(numbers) => numbers.iter().max(),
                BlockChunk::Range(_, end_block) => Some(end_block),
            };
            if let Some(&chunk_max) = chunk_max {
                block_max = std::cmp::max(chunk_max, block_max);
            }
        });
        Some(block_max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_total_blocks() {
        let chunks = &[BlockChunk::Range(0, 4)];
        let result = get_total_blocks(chunks);
        assert_eq!(result, 5);

        let chunks = &[BlockChunk::Range(0, 4), BlockChunk::Range(2, 6)];
        let result = get_total_blocks(chunks);
        assert_eq!(result, 10);
    }

    #[test]
    fn test_get_max_block() {
        let chunks = &[BlockChunk::Range(0, 4)];
        let result = get_max_block(chunks);
        assert_eq!(result, Some(4));

        let chunks = &[BlockChunk::Range(0, 4), BlockChunk::Range(2, 6)];
        let result = get_max_block(chunks);
        assert_eq!(result, Some(6));
    }

    #[test]
    fn test_get_min_block() {
        let chunks = &[BlockChunk::Range(0, 4)];
        let result = get_min_block(chunks);
        assert_eq!(result, Some(0));

        let chunks = &[BlockChunk::Range(0, 4), BlockChunk::Range(2, 6)];
        let result = get_min_block(chunks);
        assert_eq!(result, Some(0));
    }
}
