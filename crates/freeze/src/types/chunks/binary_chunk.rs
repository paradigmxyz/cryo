use super::chunk_ops::ChunkData;

/// Chunk of raw data entries
#[derive(Debug, Clone)]
pub enum BinaryChunk {
    /// Vec of values
    Values(Vec<Vec<u8>>),

    /// Range of values (start_prefix, end_prefix)
    Range(Vec<u8>, Vec<u8>),
}

impl ChunkData for BinaryChunk {
    type Inner = Vec<u8>;

    fn format_item(value: Self::Inner) -> String {
        let hash = prefix_hex::encode(value);
        let start = &hash[..hash.char_indices().nth(8).unwrap().0];
        start.to_string()
    }

    fn size(&self) -> u64 {
        match (self.min_value(), self.max_value()) {
            (Some(min), Some(max)) => {
                let min_int = ethers::types::U256::from_big_endian(&min);
                let max_int = ethers::types::U256::from_big_endian(&max);
                (max_int - min_int).as_u64()
            }
            _ => 0,
        }
    }

    fn min_value(&self) -> Option<Self::Inner> {
        match self {
            BinaryChunk::Values(numbers) => numbers.iter().min().cloned(),
            BinaryChunk::Range(start, _) => Some(start.clone()),
        }
    }

    fn max_value(&self) -> Option<Self::Inner> {
        match self {
            BinaryChunk::Values(values) => values.iter().max().cloned(),
            BinaryChunk::Range(_, end) => Some(end.clone()),
        }
    }
}

impl BinaryChunk {
    /// get list of values in chunk
    pub fn values(&self) -> &Vec<Vec<u8>> {
        match self {
            BinaryChunk::Values(values) => values,
            BinaryChunk::Range(_start, _end) => panic!("values not implemented for binary ranges"),
        }
    }
}
