use polars::prelude::*;

use crate::types::{CollectError, Table};

pub(crate) trait SortableDataFrame {
    fn sort_by_schema(self, schema: &Table) -> Self;
}

impl SortableDataFrame for Result<DataFrame, CollectError> {
    fn sort_by_schema(self, schema: &Table) -> Self {
        match (self, &schema.sort_columns) {
            (Ok(df), Some(sort_columns)) => {
                // Filter sort columns to only those present in the DataFrame.
                // If a column is excluded via `exclude_columns`, it won't exist
                // in the DataFrame and polars would error (returning zero rows).
                let df_schema = df.schema();
                let present: Vec<String> = sort_columns
                    .iter()
                    .filter(|col| df_schema.contains(col.as_str()))
                    .cloned()
                    .collect();
                if present.is_empty() {
                    return Ok(df);
                }
                df.sort(present, false, false).map_err(CollectError::PolarsError)
            }
            (df, _) => df,
        }
    }
}
