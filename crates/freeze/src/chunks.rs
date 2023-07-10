use ethers::prelude::*;

use crate::types::{error_types, BlockChunk};

/// Aggregation operations related to chunks
pub trait ChunkAgg {
    /// return a Vec of all blocks in chunk
    fn numbers(&self) -> Vec<u64>;

    /// get total block count
    fn total_blocks(&self) -> u64;

    /// get minimum block
    fn min_block(&self) -> Option<u64>;

    /// get maximum block
    fn max_block(&self) -> Option<u64>;

    /// divide into subchunks by size
    fn subchunk_by_size(&self, chunk_size: &u64) -> Vec<BlockChunk>;

    /// divide into number of subchunks
    fn subchunk_by_count(&self, n_chunks: &u64) -> Vec<BlockChunk>;
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

    fn subchunk_by_size(&self, chunk_size: &u64) -> Vec<BlockChunk> {
        self.to_single_chunk().subchunk_by_size(chunk_size)
    }

    fn subchunk_by_count(&self, n_chunks: &u64) -> Vec<BlockChunk> {
        self.to_single_chunk().subchunk_by_count(n_chunks)
    }
}

/// Operations that can be performed on chunks
pub trait ChunkOps {
    /// convert a block chunk into a String representation
    fn stub(&self) -> Result<String, error_types::ChunkError>;

    /// break a block chunk into FilterBlockOption for log requests
    fn to_log_filter_options(&self, log_request_size: &u64) -> Vec<FilterBlockOption>;

    /// align chunk boundaries to standard boundaries
    fn align(self, chunk_size: u64) -> Option<BlockChunk>;
}

impl ChunkOps for BlockChunk {
    fn stub(&self) -> Result<String, error_types::ChunkError> {
        match self {
            BlockChunk::Numbers(numbers) => match (numbers.iter().min(), numbers.iter().max()) {
                (Some(min), Some(max)) => {
                    // let hash = compute_numbers_hash(numbers);
                    Ok(format!(
                        "mixed_{:0>8}_to_{:0>8}",
                        min,
                        max,
                        // &hash[0..8].to_string()
                    ))
                }
                _ => Err(error_types::ChunkError::StubError),
            },
            BlockChunk::Range(start_block, end_block) => {
                Ok(format!("{:0>8}_to_{:0>8}", start_block, end_block))
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
                let chunks = range_to_chunks(start_block, &(end_block + 1), log_request_size);
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

    fn align(self, chunk_size: u64) -> Option<BlockChunk> {
        match self {
            BlockChunk::Numbers(numbers) => Some(BlockChunk::Numbers(numbers)),
            BlockChunk::Range(start, end) => {
                let start = ((start + chunk_size - 1) / chunk_size) * chunk_size;
                let end = (end / chunk_size) * chunk_size;
                if end > start {
                    Some(BlockChunk::Range(start, end))
                } else {
                    None
                }
            }
        }
    }
}

/// Operations that can be performed on Vec's of chunks
pub(crate) trait ChunkVecOps {
    fn to_single_chunk(&self) -> BlockChunk;
}

impl ChunkVecOps for Vec<BlockChunk> {
    fn to_single_chunk(&self) -> BlockChunk {
        match (self.len(), self.get(0)) {
            (1, Some(chunk)) => chunk.clone(),
            _ => {
                let numbers = self.iter().flat_map(|x| x.numbers()).collect();
                BlockChunk::Numbers(numbers)
            }
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

// /// compute a hex hash of a slice of numbers
// fn compute_numbers_hash(numbers: &[u64]) -> String {
//     let joined_numbers = numbers
//         .iter()
//         .map(|num| num.to_string())
//         .collect::<Vec<String>>()
//         .join("");

//     let hash: Digest = digest::digest(&digest::SHA256, joined_numbers.as_bytes());

//     hex::encode(hash.as_ref())
// }

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
