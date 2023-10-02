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
pub trait ColumnData: Default {
    /// datatypes of ColumnData
    fn datatypes() -> Vec<Datatype>;

    /// whether column data produces multiple dataframe types
    const MULTI: bool = false;

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
        if Self::MULTI {
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
    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        panic!("column_types() not implemented")
    }
}
