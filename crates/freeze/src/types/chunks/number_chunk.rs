use super::chunk_ops::ChunkData;
use crate::ChunkError;
use alloy::rpc::types::FilterBlockOption;

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

    fn format_item(value: Self::Inner, pad_width: usize) -> Result<String, ChunkError> {
        Ok(format!("{:0>width$}", value, width = pad_width))
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
                let start = start.div_ceil(chunk_size) * chunk_size;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_item_pads_to_width() {
        assert_eq!(NumberChunk::format_item(1000, 8).unwrap(), "00001000");
        assert_eq!(NumberChunk::format_item(1000, 16).unwrap(), "0000000000001000");
    }

    #[test]
    fn test_format_item_no_truncation_on_overflow() {
        // Rust's {:0>N} never truncates — a 9-digit number with pad_width 8
        // produces 9 chars. This is the documented BSC overflow behaviour.
        assert_eq!(NumberChunk::format_item(100_000_000, 8).unwrap(), "100000000");
        // With pad_width 16 the same number is correctly padded.
        assert_eq!(NumberChunk::format_item(100_000_000, 16).unwrap(), "0000000100000000");
    }

    #[test]
    fn test_stub_default_width() {
        let chunk = NumberChunk::Range(1_000, 1_999);
        assert_eq!(chunk.stub(8).unwrap(), "00001000_to_00001999");
    }

    #[test]
    fn test_stub_width_16() {
        let chunk = NumberChunk::Range(1_000, 1_999);
        assert_eq!(chunk.stub(16).unwrap(), "0000000000001000_to_0000000000001999");
    }

    #[test]
    fn test_stub_bsc_overflow_scenario() {
        let chunk = NumberChunk::Range(100_000_000, 100_004_999);
        // pad_width 8 overflows (9-char numbers) — documents the existing bug
        assert_eq!(chunk.stub(8).unwrap(), "100000000_to_100004999");
        // pad_width 16 handles it correctly
        assert_eq!(chunk.stub(16).unwrap(), "0000000100000000_to_0000000100004999");
    }
}
