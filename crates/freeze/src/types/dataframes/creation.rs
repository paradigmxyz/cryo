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
