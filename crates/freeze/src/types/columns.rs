use crate::{CollectError, ColumnType, Datatype, Dim, Table};
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
    fn column_types() -> HashMap<&'static str, ColumnType>;

    /// default columns extracted for Dataset
    fn base_default_columns() -> Vec<&'static str> {
        match Self::default_columns() {
            Some(columns) => columns,
            None => Self::column_types().keys().cloned().collect(),
        }
    }

    /// default blocks for dataset
    fn base_default_blocks() -> Option<String> {
        Self::default_blocks()
    }

    /// default sort for dataset
    fn base_default_sort() -> Vec<String> {
        match Self::default_sort() {
            Some(sort) => sort.iter().map(|x| x.to_string()).collect(),
            None => {
                let mut sort = Vec::new();
                let columns = Self::column_types();
                for name in ["block_number", "transaction_index", "log_index"] {
                    if (name == "transaction_index") & columns.contains_key("log_index") {
                        continue
                    }
                    if columns.contains_key(name) {
                        sort.push(name.to_string())
                    }
                }
                sort
            }
        }
    }

    /// input arg aliases
    fn base_arg_aliases() -> HashMap<Dim, Dim> {
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

/// Dataset manages collection and management of a particular datatype
pub trait Dataset: Sync + Send {
    /// alias of Dataset
    fn aliases() -> Vec<&'static str> {
        vec![]
    }

    /// default sort order for dataset
    fn default_sort() -> Option<Vec<&'static str>> {
        None
    }

    /// default columns extracted for Dataset
    fn default_columns() -> Option<Vec<&'static str>> {
        None
    }

    /// optional parameters for dataset
    fn optional_parameters() -> Vec<Dim> {
        vec![]
    }

    /// required parameters for dataset
    fn required_parameters() -> Vec<Dim> {
        vec![]
    }

    /// default blocks for dataset
    fn default_blocks() -> Option<String> {
        None
    }

    /// whether to use block ranges instead of individual blocks
    fn use_block_ranges() -> bool {
        false
    }

    /// input arg aliases
    fn arg_aliases() -> Option<HashMap<Dim, Dim>> {
        None
    }
}
