use ethers::prelude::*;

use crate::types::error_types;
use crate::types::FreezeOpts;

/// block chunk
pub type BlockChunk = NumberChunk;
/// transaction chunk
pub type TransactionChunk = BinaryChunk;
/// address chunk
pub type AddressChunk = BinaryChunk;

/// Chunk of data
#[derive(Debug, Clone)]
pub enum Chunk {
    /// block chunk
    Block(BlockChunk),

    /// transaction chunk
    Transaction(TransactionChunk),

    /// address chunk chunk
    Address(AddressChunk),
}

/// Chunk of blocks
#[derive(Debug, Clone)]
pub enum NumberChunk {
    /// Vec of block numbers
    Numbers(Vec<u64>),

    /// Range of block numbers
    Range(u64, u64),
}

/// Chunk of raw data entries
#[derive(Debug, Clone)]
pub enum BinaryChunk {
    /// Vec of values
    Values(Vec<Vec<u8>>),

    /// Range of values (start_prefix, end_prefix)
    Range(Vec<u8>, Vec<u8>),
}

/// Chunk methods
impl Chunk {
    /// get filepath for chunk
    pub fn filepath(
        &self,
        name: &str,
        opts: &FreezeOpts,
    ) -> Result<String, error_types::FileError> {
        match self {
            Chunk::Block(chunk) => chunk.filepath(name, opts),
            Chunk::Transaction(chunk) => chunk.filepath(name, opts),
            Chunk::Address(chunk) => chunk.filepath(name, opts),
        }
    }
}

impl NumberChunk {
    /// get list of block numbers in chunk
    pub fn numbers(&self) -> Vec<u64> {
        match self {
            NumberChunk::Numbers(numbers) => numbers.to_vec(),
            NumberChunk::Range(start, end) => (*start..=*end).collect(),
        }
    }

    /// convert block range to a list of Filters for get_logs()
    pub fn to_log_filter_options(&self, log_request_size: &u64) -> Vec<FilterBlockOption> {
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

    /// align boundaries of chunk to clean boundaries
    pub fn align(self, chunk_size: u64) -> Option<BlockChunk> {
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

/// Trait for common chunk methods
pub trait ChunkData: Sized {
    /// inner type used to express items in chunk
    type Inner: Ord;

    /// format a single item in chunk
    fn format_item(value: Self::Inner) -> String;

    /// size of chunk
    fn size(&self) -> u64;

    /// get minimum item in chunk
    fn min_value(&self) -> Option<Self::Inner>;

    /// get maximum item in chunk
    fn max_value(&self) -> Option<Self::Inner>;

    /// convert chunk to string representation
    fn stub(&self) -> Result<String, error_types::ChunkError> {
        match (self.min_value(), self.max_value()) {
            (Some(min), Some(max)) => Ok(format!(
                "{}_to_{}",
                Self::format_item(min),
                Self::format_item(max),
            )),
            _ => Err(error_types::ChunkError::InvalidChunk),
        }
    }

    /// get filepath for chunk
    fn filepath(&self, name: &str, opts: &FreezeOpts) -> Result<String, error_types::FileError> {
        let network_name = opts.network_name.clone();
        let pieces: Vec<String> = match &opts.file_suffix {
            Some(suffix) => vec![network_name, name.to_string(), self.stub()?, suffix.clone()],
            None => vec![network_name, name.to_string(), self.stub()?],
        };
        let filename = format!("{}.{}", pieces.join("__"), opts.output_format.as_str());
        match opts.output_dir.as_str() {
            "." => Ok(filename),
            output_dir => Ok(output_dir.to_string() + "/" + filename.as_str()),
        }
    }
}

impl ChunkData for NumberChunk {
    type Inner = u64;

    fn format_item(value: Self::Inner) -> String {
        format!("{:0>8}", value)
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
}

impl ChunkData for BinaryChunk {
    type Inner = Vec<u8>;

    fn format_item(_value: Self::Inner) -> String {
        todo!()
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

impl<T: ChunkData> ChunkData for Vec<T> {
    type Inner = T::Inner;

    fn format_item(value: Self::Inner) -> String {
        T::format_item(value)
    }

    fn size(&self) -> u64 {
        self.iter().map(|x| x.size()).sum()
    }

    fn min_value(&self) -> Option<Self::Inner> {
        self.iter().filter_map(|x| x.min_value()).min()
    }

    fn max_value(&self) -> Option<Self::Inner> {
        self.iter().filter_map(|x| x.max_value()).max()
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
