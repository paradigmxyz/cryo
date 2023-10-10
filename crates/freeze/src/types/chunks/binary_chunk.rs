use super::chunk_ops::ChunkData;
use crate::ChunkError;

/// Chunk of raw data entries
#[derive(Debug, Clone)]
pub enum BinaryChunk {
    /// Vec of values
    Values(Vec<Vec<u8>>),
    // /// Range of values (start_prefix, end_prefix)
    // Range(Vec<u8>, Vec<u8>),
}

impl ChunkData for BinaryChunk {
    type Inner = Vec<u8>;

    fn format_item(value: Self::Inner) -> Result<String, ChunkError> {
        let hash = prefix_hex::encode(value);
        let eigth = match hash.char_indices().nth(8) {
            Some(x) => x,
            None => return Err(ChunkError::ChunkError("could not format chunk".to_string())),
        };
        let start = &hash[..eigth.0];
        Ok(start.to_string())
    }

    fn size(&self) -> u64 {
        match self {
            BinaryChunk::Values(values) => values.len() as u64,
        }

        // match (self.min_value(), self.max_value()) {
        //     (Some(min), Some(max)) => {
        //         let min_int = ethers::types::U256::from_big_endian(&min);
        //         let max_int = ethers::types::U256::from_big_endian(&max);
        //         (max_int - min_int).as_u64()
        //     }
        //     e => 0
        // }
    }

    fn min_value(&self) -> Option<Self::Inner> {
        match self {
            BinaryChunk::Values(numbers) => numbers.iter().min().cloned(),
            // BinaryChunk::Range(start, _) => Some(start.clone()),
        }
    }

    fn max_value(&self) -> Option<Self::Inner> {
        match self {
            BinaryChunk::Values(values) => values.iter().max().cloned(),
            // BinaryChunk::Range(_, end) => Some(end.clone()),
        }
    }

    fn values(&self) -> Vec<Vec<u8>> {
        match self {
            BinaryChunk::Values(values) => values.to_vec(),
            // BinaryChunk::Range(_start, _end) =>
        }
    }
}
