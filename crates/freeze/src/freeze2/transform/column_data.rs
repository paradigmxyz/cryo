use crate::{CollectError, Table};
use polars::prelude::*;

/// container for a dataset partition
pub trait ColumnData: Default {
    /// whether column data produces multiple dataframe types
    const MULTI: bool = false;

    /// create dataframe from column data
    fn create_df(self, _schema: &Table, _chain_id: u64) -> Result<DataFrame, CollectError> {
        panic!("create_df() not implemented")
    }

    /// create dataframe from column data
    fn create_dfs(self, schema: &Table, chain_id: u64) -> Result<Vec<DataFrame>, CollectError> {
        if Self::MULTI {
            panic!("create_dfs() not implemented")
        } else {
            Ok(vec![Self::create_df(self, schema, chain_id)?])
        }
    }
}
