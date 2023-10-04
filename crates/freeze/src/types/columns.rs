use crate::ColumnType;
use crate::{CollectError, Datatype, Table};
use polars::prelude::*;
use std::collections::HashMap;

/// store values in columns if column present in schema
#[macro_export]
macro_rules! store {
    ($schema:expr, $columns:expr, $col_name:ident, $value:expr) => {
        if $schema.has_column(stringify!($col_name)) {
            $columns.$col_name.push($value);
        }
    };
}

/// container for a dataset partition
pub trait ColumnData: Default + crate::Dataset {
    /// column types
    fn column_types() -> HashMap<&'static str, ColumnType> {
        panic!("column_types() not implemented")
    }

    /// default columns extracted for Dataset
    fn base_default_columns() -> Vec<&'static str> {
        Self::column_types().keys().cloned().collect()
    }

    /// default blocks for dataset
    fn base_default_blocks() -> Option<String> {
        Self::default_blocks()
    }

    /// input arg aliases
    fn base_arg_aliases() -> HashMap<String, String> {
        match Self::arg_aliases() {
            Some(x) => x,
            None => HashMap::new(),
        }
    }
}

/// converts to dataframes
pub trait ToDataFrames: Sized {
    /// create dataframe from column data
    fn create_dfs(
        self,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError>;
}
