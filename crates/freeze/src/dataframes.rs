/// convert a Vec to Series and add to Vec<Series>
#[macro_export]
macro_rules! with_series {
    ($all_series:expr, $name:expr, $value:expr, $schema:expr) => {
        if $schema.contains_key($name) {
            $all_series.push(Series::new($name, $value));
        }
    };
}

/// convert a Vec to Series, as hex if specified, and add to Vec<Series>
#[macro_export]
macro_rules! with_series_binary {
    ($all_series:expr, $name:expr, $value:expr, $schema:expr) => {
        if $schema.contains_key($name) {
            if let Some(ColumnType::Hex) = $schema.get($name) {
                $all_series.push(Series::new($name, $value.to_vec_hex()));
            } else {
                $all_series.push(Series::new($name, $value));
            }
        }
    };
}

// if let Some(sort_keys) = opts.sort.get(&Datatype::Blocks) {
//     df.map(|x| x.sort(sort_keys, false))?
//         .map_err(CollectError::PolarsError)
// } else {
//     df
// }
