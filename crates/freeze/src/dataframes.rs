use polars::prelude::*;

use crate::types::CollectError;
use crate::types::Table;

/// convert a Vec to Series and add to Vec<Series>
#[macro_export]
macro_rules! with_series {
    ($all_series:expr, $name:expr, $value:expr, $schema:expr) => {
        if $schema.has_column($name) {
            $all_series.push(Series::new($name, $value));
        }
    };
}

/// convert a Vec to Series, as hex if specified, and add to Vec<Series>
#[macro_export]
macro_rules! with_series_binary {
    ($all_series:expr, $name:expr, $value:expr, $schema:expr) => {
        if $schema.has_column($name) {
            if let Some(ColumnType::Hex) = $schema.column_type($name) {
                $all_series.push(Series::new($name, $value.to_vec_hex()));
            } else {
                $all_series.push(Series::new($name, $value));
            }
        }
    };
}

pub(crate) trait SortableDataFrame {
    fn sort_by_schema(self, schema: &Table) -> Self;
}

impl SortableDataFrame for Result<DataFrame, CollectError> {
    fn sort_by_schema(self, schema: &Table) -> Self {
        match (self, &schema.sort_columns) {
            (Ok(df), Some(sort_columns)) => df
                .sort(sort_columns, false)
                .map_err(CollectError::PolarsError),
            (df, _) => df,
        }
    }
}
