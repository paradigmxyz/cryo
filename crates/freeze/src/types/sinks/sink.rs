use crate::*;
use polars::prelude::*;
use std::collections::HashMap;

/// data sink
pub trait Sink: Sync + Send {
    /// check whether data with id exists
    fn exists(&self, id: &str) -> bool;

    /// get id of data
    fn get_id(
        &self,
        query: &Query,
        partition: &Partition,
        datatype: Datatype,
    ) -> Result<String, CollectError>;

    /// send data to sink
    fn sink_df(&self, dataframe: &mut DataFrame, id: &str) -> Result<(), CollectError>;

    /// overwrite data if it already exists
    fn overwrite(&self) -> bool;

    /// String representation of output format (e.g. parquet, csv, json)
    fn output_format(&self) -> String;

    /// String representation of output location (e.g. root output directory)
    fn output_location(&self) -> Result<String, CollectError>;

    /// get multiple ids at once
    fn get_ids(
        &self,
        query: &Query,
        partition: &Partition,
        meta_datatypes: Option<Vec<MetaDatatype>>,
    ) -> Result<HashMap<Datatype, String>, CollectError> {
        let mut ids = HashMap::new();
        let meta_datatypes = if let Some(meta_datatypes) = meta_datatypes {
            meta_datatypes
        } else {
            query.datatypes.clone()
        };
        for meta_datatype in meta_datatypes.iter() {
            for datatype in meta_datatype.datatypes().into_iter() {
                ids.insert(datatype, self.get_id(query, partition, datatype)?);
            }
        }
        Ok(ids)
    }
}
