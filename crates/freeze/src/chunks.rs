use ethers::prelude::*;
use ring::digest::{self, Digest};

use crate::types::error_types;
use crate::types::BlockChunk;

pub trait ChunkAgg {
    /// return a Vec of all blocks in chunk
    fn numbers(&self) -> Vec<u64>;
    fn total_blocks(&self) -> u64;
    fn min_block(&self) -> Option<u64>;
    fn max_block(&self) -> Option<u64>;
}

impl ChunkAgg for BlockChunk {
    fn numbers(&self) -> Vec<u64> {
        match self {
            BlockChunk::Numbers(numbers) => numbers.to_vec(),
            BlockChunk::Range(start_block, end_block) => (*start_block..=*end_block).collect(),
        }
    }
    fn total_blocks(&self) -> u64 {
        match self {
            BlockChunk::Numbers(numbers) => numbers.len() as u64,
            BlockChunk::Range(start_block, end_block) => end_block - start_block + 1,
        }
    }
    fn min_block(&self) -> Option<u64> {
        match self {
            BlockChunk::Numbers(numbers) => numbers.iter().min().cloned(),
            BlockChunk::Range(start_block, _) => Some(*start_block),
        }
    }
    fn max_block(&self) -> Option<u64> {
        match self {
            BlockChunk::Numbers(numbers) => numbers.iter().max().cloned(),
            BlockChunk::Range(_, end_block) => Some(*end_block),
        }
    }
}

impl ChunkAgg for Vec<BlockChunk> {
    fn numbers(&self) -> Vec<u64> {
        self.iter().flat_map(|chunk| chunk.numbers()).collect()
    }

    fn total_blocks(&self) -> u64 {
        let mut total = 0;
        self.iter().for_each(|chunk| {
            total += chunk.total_blocks();
        });
        total
    }

    fn min_block(&self) -> Option<u64> {
        if self.is_empty() {
            None
        } else {
            let mut block_min = std::u64::MAX;
            self.iter().for_each(|chunk| {
                if let Some(chunk_min) = chunk.min_block() {
                    block_min = std::cmp::min(chunk_min, block_min);
                }
            });
            Some(block_min)
        }
    }

    fn max_block(&self) -> Option<u64> {
        if self.is_empty() {
            None
        } else {
            let mut block_max = std::u64::MIN;
            self.iter().for_each(|chunk| {
                if let Some(chunk_max) = chunk.max_block() {
                    block_max = std::cmp::max(chunk_max, block_max);
                }
            });
            Some(block_max)
        }
    }
}

pub trait ChunkOps {
    /// divide into subchunks by size
    fn subchunk_by_size(&self, chunk_size: &u64) -> Vec<BlockChunk>;

    /// divide into number of subchunks
    fn subchunk_by_count(&self, n_chunks: &u64) -> Vec<BlockChunk>;

    /// convert a block chunk into a String representation
    fn stub(&self) -> Result<String, error_types::ChunkError>;

    /// break a block chunk into FilterBlockOption for log requests
    fn to_log_filter_options(&self, log_request_size: &u64) -> Vec<FilterBlockOption>;
}

impl ChunkOps for BlockChunk {
    fn subchunk_by_size(&self, chunk_size: &u64) -> Vec<BlockChunk> {
        match &self {
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

    fn subchunk_by_count(&self, n_chunks: &u64) -> Vec<BlockChunk> {
        let total_blocks = &self.total_blocks();
        let chunk_size = (total_blocks + n_chunks - 1) / n_chunks;
        self.subchunk_by_size(&chunk_size)
    }

    fn stub(&self) -> Result<String, error_types::ChunkError> {
        match self {
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

    fn to_log_filter_options(&self, log_request_size: &u64) -> Vec<FilterBlockOption> {
        match self {
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
}

//
// // old
//

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_total_blocks() {
        let chunks = vec![BlockChunk::Range(0, 4)];
        assert_eq!(chunks.total_blocks(), 5);

        let chunks = vec![BlockChunk::Range(0, 4), BlockChunk::Range(2, 6)];
        assert_eq!(chunks.total_blocks(), 10);
    }

    #[test]
    fn test_get_max_block() {
        let chunks = vec![BlockChunk::Range(0, 4)];
        assert_eq!(chunks.max_block(), Some(4));

        let chunks = vec![BlockChunk::Range(0, 4), BlockChunk::Range(2, 6)];
        assert_eq!(chunks.max_block(), Some(6));
    }

    #[test]
    fn test_get_min_block() {
        let chunks = vec![BlockChunk::Range(0, 4)];
        assert_eq!(chunks.min_block(), Some(0));

        let chunks = vec![BlockChunk::Range(0, 4), BlockChunk::Range(2, 6)];
        assert_eq!(chunks.min_block(), Some(0));
    }
}
