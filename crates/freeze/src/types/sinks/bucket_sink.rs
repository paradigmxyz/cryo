use crate::*;
use polars::prelude::*;

/// Bucket sink
pub struct BucketSink {
    /// file output
    pub file_output: FileOutput,
}

impl Sink for BucketSink {
    fn exists(&self, _id: &str) -> bool {
        todo!()
    }

    fn get_id(
        &self,
        _query: &Query,
        _partition: &Partition,
        _datatype: Datatype,
    ) -> Result<String, CollectError> {
        todo!()
    }

    fn sink_df(&self, _dataframe: &mut DataFrame, _id: &str) -> Result<(), CollectError> {
        todo!()
    }

    fn overwrite(&self) -> bool {
        todo!()
    }

    fn output_format(&self) -> String {
        todo!()
    }

    fn output_location(&self) -> Result<String, CollectError> {
        todo!()
    }
}
