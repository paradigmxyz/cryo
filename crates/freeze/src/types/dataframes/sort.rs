use polars::prelude::*;

use crate::types::{CollectError, Table};

pub(crate) trait SortableDataFrame {
    fn sort_by_schema(self, schema: &Table) -> Self;
}

impl SortableDataFrame for Result<DataFrame, CollectError> {
    fn sort_by_schema(self, schema: &Table) -> Self {
        match (self, &schema.sort_columns) {
            (Ok(df), Some(sort_columns)) => {
                df.sort(sort_columns, false).map_err(CollectError::PolarsError)
            }
            (df, _) => df,
        }
    }
}
