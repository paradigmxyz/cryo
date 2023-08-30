use crate::{ChunkError, Datatype, FileError, FileOutput};

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
                vec![network_name, datatype.dataset().name().to_string(), stub, suffix.clone()]
            }
            None => vec![network_name, datatype.dataset().name().to_string(), stub],
        };
        let filename = format!("{}.{}", pieces.join("__"), file_output.format.as_str());
        Ok(file_output.output_dir.join(filename))
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
