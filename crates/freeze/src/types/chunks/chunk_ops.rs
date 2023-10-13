use crate::{ChunkError, Datatype, FileError, FileOutput};
use thousands::Separable;

/// Trait for common chunk methods
pub trait ChunkData: Sized {
    /// inner type used to express items in chunk
    type Inner: Ord + ValueToString;

    /// format a single item in chunk
    fn format_item(value: Self::Inner) -> Result<String, ChunkError>;

    /// size of chunk
    fn size(&self) -> u64;

    /// get minimum item in chunk
    fn min_value(&self) -> Option<Self::Inner>;

    /// get maximum item in chunk
    fn max_value(&self) -> Option<Self::Inner>;

    /// get values in chunk
    fn values(&self) -> Vec<Self::Inner>;

    /// convert chunk to string representation
    fn stub(&self) -> Result<String, ChunkError> {
        match (self.min_value(), self.max_value()) {
            (Some(min), Some(max)) => {
                Ok(format!("{}_to_{}", Self::format_item(min)?, Self::format_item(max)?,))
            }
            _ => Err(ChunkError::InvalidChunk),
        }
    }

    /// get filepath for chunk
    fn filepath(
        &self,
        datatype: &Datatype,
        file_output: &FileOutput,
        chunk_label: &Option<String>,
    ) -> Result<std::path::PathBuf, FileError> {
        let network_name = file_output.prefix.clone();
        let stub = match chunk_label {
            Some(chunk_label) => chunk_label.clone(),
            None => self.stub()?,
        };
        let pieces: Vec<String> = match &file_output.suffix {
            Some(suffix) => {
                vec![network_name, datatype.name(), stub, suffix.clone()]
            }
            None => vec![network_name, datatype.name(), stub],
        };
        let filename = format!("{}.{}", pieces.join("__"), file_output.format.as_str());
        Ok(file_output.output_dir.join(filename))
    }

    /// get stats of Chunk
    fn stats(&self) -> ChunkStats<Self::Inner> {
        ChunkStats {
            min_value: self.min_value(),
            max_value: self.max_value(),
            total_values: self.size(),
            chunk_size: self.size(),
            n_chunks: 1,
        }
    }
}

/// statistics of a Chunk's contents
#[derive(Clone)]
pub struct ChunkStats<T: std::cmp::Ord + ValueToString> {
    /// minimum value in chunk
    pub min_value: Option<T>,
    /// maximum value in chunk
    pub max_value: Option<T>,
    /// number of values in chunk
    pub total_values: u64,
    /// size of chunk
    pub chunk_size: u64,
    /// number of chunks
    pub n_chunks: u64,
}

impl<T: std::cmp::Ord + ValueToString> ChunkStats<T> {
    /// reduce ChunkStats of two chunks into one
    pub fn fold(self, other: ChunkStats<T>) -> ChunkStats<T> {
        let min_value = std::cmp::min(self.min_value, other.min_value);
        let max_value = std::cmp::max(self.max_value, other.max_value);
        let total_values = self.total_values + other.total_values;
        let chunk_size = self.chunk_size;
        let n_chunks = self.n_chunks + other.n_chunks;
        ChunkStats { min_value, max_value, total_values, chunk_size, n_chunks }
    }

    /// get minimum value as string
    pub fn min_value_to_string(&self) -> Option<String> {
        self.min_value.as_ref().map(|v| v.to_value_string())
    }

    /// get minimum value as string
    pub fn max_value_to_string(&self) -> Option<String> {
        self.max_value.as_ref().map(|v| v.to_value_string())
    }
}

pub trait ValueToString {
    fn to_value_string(&self) -> String;
}

impl ValueToString for u64 {
    fn to_value_string(&self) -> String {
        self.separate_with_commas()
    }
}

impl ValueToString for Vec<u8> {
    fn to_value_string(&self) -> String {
        prefix_hex::encode(self.clone())
    }
}

impl<T: ChunkData> ChunkData for Vec<T> {
    type Inner = T::Inner;

    fn format_item(value: Self::Inner) -> Result<String, ChunkError> {
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

    fn values(&self) -> Vec<Self::Inner> {
        self.iter().flat_map(|chunk| chunk.values().into_iter()).collect::<Vec<Self::Inner>>()
    }

    fn stats(&self) -> ChunkStats<Self::Inner> {
        ChunkStats {
            min_value: self.min_value(),
            max_value: self.max_value(),
            total_values: self.size(),
            chunk_size: self.size(),
            n_chunks: self.len() as u64,
        }
    }
}

impl<T: ChunkData> ChunkData for &[T] {
    type Inner = T::Inner;

    fn format_item(value: Self::Inner) -> Result<String, ChunkError> {
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

    fn values(&self) -> Vec<Self::Inner> {
        self.iter().flat_map(|chunk| chunk.values().into_iter()).collect::<Vec<Self::Inner>>()
    }

    fn stats(&self) -> ChunkStats<Self::Inner> {
        ChunkStats {
            min_value: self.min_value(),
            max_value: self.max_value(),
            total_values: self.size(),
            chunk_size: self.size(),
            n_chunks: self.len() as u64,
        }
    }
}
