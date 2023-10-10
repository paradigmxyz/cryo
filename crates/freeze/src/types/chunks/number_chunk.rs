use super::chunk_ops::ChunkData;
use crate::ChunkError;
use ethers::types::FilterBlockOption;

/// Chunk of blocks
#[derive(Debug, Clone)]
pub enum NumberChunk {
    /// Vec of block numbers
    Numbers(Vec<u64>),

    /// Range of block numbers
    Range(u64, u64),
}

impl ChunkData for NumberChunk {
    type Inner = u64;

    fn format_item(value: Self::Inner) -> Result<String, ChunkError> {
        Ok(format!("{:0>8}", value))
    }

    fn size(&self) -> u64 {
        match self {
            NumberChunk::Numbers(numbers) => numbers.len() as u64,
            NumberChunk::Range(start, end) => end - start + 1,
        }
    }

    fn min_value(&self) -> Option<Self::Inner> {
        match self {
            NumberChunk::Numbers(numbers) => numbers.iter().min().cloned(),
            NumberChunk::Range(start, _) => Some(*start),
        }
    }

    fn max_value(&self) -> Option<Self::Inner> {
        match self {
            NumberChunk::Numbers(numbers) => numbers.iter().max().cloned(),
            NumberChunk::Range(_, end) => Some(*end),
        }
    }

    fn values(&self) -> Vec<u64> {
        match self {
            NumberChunk::Numbers(numbers) => numbers.to_vec(),
            NumberChunk::Range(start, end) => (*start..=*end).collect(),
        }
    }
}

impl NumberChunk {
    /// get list of block numbers in chunk
    /// TODO: remove in favor of values()
    pub fn numbers(&self) -> Vec<u64> {
        match self {
            NumberChunk::Numbers(numbers) => numbers.to_vec(),
            NumberChunk::Range(start, end) => (*start..=*end).collect(),
        }
    }

    /// convert block range to a list of Filters for get_logs()
    pub fn to_log_filter_options(&self, log_request_size: &u64) -> Vec<FilterBlockOption> {
        match self {
            NumberChunk::Numbers(block_numbers) => block_numbers
                .iter()
                .map(|block| FilterBlockOption::Range {
                    from_block: Some((*block).into()),
                    to_block: Some((*block).into()),
                })
                .collect(),
            NumberChunk::Range(start_block, end_block) => {
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

    /// align boundaries of chunk to clean boundaries
    pub fn align(self, chunk_size: u64) -> Option<NumberChunk> {
        match self {
            NumberChunk::Numbers(numbers) => Some(NumberChunk::Numbers(numbers)),
            NumberChunk::Range(start, end) => {
                let start = ((start + chunk_size - 1) / chunk_size) * chunk_size;
                let end = (end / chunk_size) * chunk_size;
                if end > start {
                    Some(NumberChunk::Range(start, end))
                } else {
                    None
                }
            }
        }
    }
}

pub(crate) fn range_to_chunks(start: &u64, end: &u64, chunk_size: &u64) -> Vec<(u64, u64)> {
    let mut chunks: Vec<(u64, u64)> = Vec::new();
    let mut chunk_start = *start;
    loop {
        let chunk_end: u64 = chunk_start + chunk_size - 1;
        let chunk_end = if chunk_end > *end { *end } else { chunk_end };
        chunks.push((chunk_start, chunk_end));
        if chunk_end == *end {
            break
        } else {
            chunk_start += chunk_size;
        }
    }
    chunks
}
