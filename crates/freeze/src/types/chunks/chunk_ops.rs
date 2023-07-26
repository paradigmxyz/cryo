use crate::{ChunkError, FileError, FileOutput};

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
    fn stub(&self) -> Result<String, ChunkError> {
        match (self.min_value(), self.max_value()) {
            (Some(min), Some(max)) => {
                Ok(format!("{}_to_{}", Self::format_item(min), Self::format_item(max),))
            }
            _ => Err(ChunkError::InvalidChunk),
        }
    }

    /// get filepath for chunk
    fn filepath(&self, name: &str, file_output: &FileOutput) -> Result<String, FileError> {
        let network_name = file_output.prefix.clone();
        let pieces: Vec<String> = match &file_output.suffix {
            Some(suffix) => vec![network_name, name.to_string(), self.stub()?, suffix.clone()],
            None => vec![network_name, name.to_string(), self.stub()?],
        };
        let filename = format!("{}.{}", pieces.join("__"), file_output.format.as_str());
        match file_output.output_dir.as_str() {
            "." => Ok(filename),
            output_dir => Ok(output_dir.to_string() + "/" + filename.as_str()),
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
