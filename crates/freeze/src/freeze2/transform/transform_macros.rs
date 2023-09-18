/// store values in columns if column present in schema
#[macro_export]
macro_rules! store {
    ($schema:expr, $columns:expr, $col_name:ident, $value:expr) => {
        if $schema.has_column(stringify!($col_name)) {
            $columns.$col_name.push($value);
        }
    };
}
