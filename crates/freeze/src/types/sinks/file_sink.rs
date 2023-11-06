use crate::*;
use polars::prelude::*;

/// file sink
pub struct FileSink(pub FileOutput);

impl Sink for FileSink {
    fn exists(&self, id: &str) -> bool {
        let path: std::path::PathBuf = id.into();
        path.exists()
    }

    fn get_id(
        &self,
        query: &Query,
        partition: &Partition,
        datatype: Datatype,
    ) -> Result<String, CollectError> {
        let FileSink(file_output) = self;
        file_output
            .get_path(query, partition, datatype)?
            .into_os_string()
            .into_string()
            .map_err(|_| err("could not convert path to string"))
    }

    fn sink_df(&self, dataframe: &mut DataFrame, id: &str) -> Result<(), CollectError> {
        let FileSink(file_output) = self;
        dataframes::df_to_file(dataframe, id, file_output)
            .map_err(|_| CollectError::CollectError("error writing file".to_string()))
    }

    fn overwrite(&self) -> bool {
        let FileSink(output) = self;
        output.overwrite
    }

    fn output_location(&self) -> Result<String, CollectError> {
        let FileSink(file_output) = self;
        file_output
            .output_dir
            .clone()
            .into_os_string()
            .into_string()
            .map_err(|_| err("could not convert output directory to string"))
    }

    fn output_format(&self) -> String {
        let FileSink(file_output) = self;
        file_output.format.as_str().to_string()
    }
}
