use crate::{CollectError, Datatype, Table};
use polars::prelude::*;
use std::collections::HashMap;
use crate::ColumnType;

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
    /// datatypes of ColumnData
    fn datatypes() -> Vec<Datatype>;

    /// create dataframe from column data
    fn create_df(
        self,
        _schemas: &HashMap<Datatype, Table>,
        _chain_id: u64,
    ) -> Result<DataFrame, CollectError> {
        panic!("create_df() not implemented")
    }

    /// create dataframe from column data
    fn create_dfs(
        self,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        if Self::datatypes().len() > 1 {
            panic!("create_dfs() not implemented")
        } else if schemas.len() == 1 {
            let datatype = schemas.keys().next().expect("could not get datatype");
            let df = Self::create_df(self, schemas, chain_id)?;
            let output: HashMap<Datatype, DataFrame> = vec![(*datatype, df)].into_iter().collect();
            Ok(output)
        } else {
            Err(CollectError::CollectError("schema has more than one datatype".to_string()))
        }
    }

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

// /// converts to dataframes
// pub trait ToDataFrames: Sized {
//     // /// create dataframe from column data
//     // fn create_df(
//     //     self,
//     //     _schemas: &HashMap<Datatype, Table>,
//     //     _chain_id: u64,
//     // ) -> Result<DataFrame, CollectError> {
//     //     panic!("create_df() not implemented")
//     // }

//     /// create dataframe from column data
//     fn create_dfs(
//         self,
//         schemas: &HashMap<Datatype, Table>,
//         chain_id: u64,
//     ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
//         if Self::datatypes().len() > 1 {
//             panic!("create_dfs() not implemented")
//         } else if schemas.len() == 1 {
//             let datatype = schemas.keys().next().expect("could not get datatype");
//             let df = Self::create_df(self, schemas, chain_id)?;
//             let output: HashMap<Datatype, DataFrame> = vec![(*datatype, df)].into_iter().collect();
//             Ok(output)
//         } else {
//             Err(CollectError::CollectError("schema has more than one datatype".to_string()))
//         }
//     }

// }
