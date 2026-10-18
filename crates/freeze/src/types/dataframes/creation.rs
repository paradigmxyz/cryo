use alloy::dyn_abi::DynSolValue;
use polars::prelude::Column;

use crate::{err, CollectError, ColumnType, DynValues, RawBytes, TableConfig};

/// convert a Vec to Column and add to Vec<Column>
#[macro_export]
macro_rules! with_column_primitive {
    ($all_columns:expr, $name:expr, $value:expr, $schema:expr) => {
        if $schema.has_column($name) {
            $all_columns.push(Column::new($name.into(), $value));
        }
    };
}

/// convert a Vec to Column and add to Vec<Column>, using [`crate::DynValues`]
#[macro_export]
macro_rules! with_column {
    ($all_columns:expr, $name:expr, $value:expr, $schema:expr) => {
        if let Some(col_type) = $schema.column_type($name) {
            let cols = DynValues::from($value).into_columns(
                $name.to_string(),
                col_type,
                &$schema.config,
            )?;
            $all_columns.extend(cols);
        }
    };
}

/// parse a primitive type column from DataFrame and populate struct field
#[macro_export]
macro_rules! parse_column_primitive {
    // Special handling for String since we need to convert &str to String
    ($df:expr, $column_name:expr, str, $field:expr, $schema:expr) => {
        let option_vec = {
            let series = $df.column($column_name).map_err(CollectError::PolarsError)?;
            let str_array = series.str().map_err(CollectError::PolarsError)?;
            let values: Vec<Option<String>> =
                str_array.into_iter().map(|opt_str| opt_str.map(|s| s.to_string())).collect();

            OptionVec::Option(values)
        };
        $field = option_vec.try_into()?;
    };
    ($df:expr, $column_name:expr, $polars_type:ident, $field:expr, $schema:expr) => {
        let option_vec = {
            let series = $df.column($column_name).map_err(CollectError::PolarsError)?;
            let array = series.$polars_type().map_err(CollectError::PolarsError)?;
            let values: Vec<Option<_>> = array.into_iter().collect();

            OptionVec::Option(values)
        };
        $field = option_vec.try_into()?;
    };
}

/// parse a complex type column from DataFrame and populate struct field (for U256/I256)
#[macro_export]
macro_rules! parse_column {
    ($df:expr, $column_name:expr, U256, $field:expr, $schema:expr) => {
        // Try to read from <field_name>_u256binary first, then fall back to <field_name>_binary
        let u256_column_name = format!("{}_u256binary", $column_name);
        let binary_column_name = format!("{}_binary", $column_name);

        let column_result =
            $df.column(&u256_column_name).or_else(|_| $df.column(&binary_column_name));

        match column_result {
            Ok(series) => {
                let binary_data = series.binary().map_err(CollectError::PolarsError)?;
                let raw_data: Vec<Option<RawBytes>> =
                    binary_data.into_iter().map(|opt| opt.map(|bytes| bytes.to_vec())).collect();
                $field = $crate::FromBinaryVec::from_binary_vec(raw_data)?;
            }
            Err(_) => {
                $field = vec![];
            }
        }
    };
    ($df:expr, $column_name:expr, I256, $field:expr, $schema:expr) => {
        // Try to read from <field_name>_i256binary first, then fall back to <field_name>_binary
        let i256_column_name = format!("{}_i256binary", $column_name);
        let binary_column_name = format!("{}_binary", $column_name);

        let column_result =
            $df.column(&i256_column_name).or_else(|_| $df.column(&binary_column_name));

        match column_result {
            Ok(series) => {
                let binary_data = series.binary().map_err(CollectError::PolarsError)?;
                let raw_data: Vec<Option<RawBytes>> =
                    binary_data.into_iter().map(|opt| opt.map(|bytes| bytes.to_vec())).collect();
                $field = $crate::FromBinaryVec::from_binary_vec(raw_data)?;
            }
            Err(_) => {
                $field = vec![];
            }
        }
    };
}

impl ColumnType {
    /// data should never be mixed type, otherwise this will return inconsistent results
    pub fn create_column_from_values(
        self,
        name: String,
        data: Vec<DynSolValue>,
        chunk_len: usize,
        config: &TableConfig,
    ) -> Result<Vec<Column>, CollectError> {
        let values = DynValues::from_sol_values(data, config)?;
        let mixed_length_err = format!("could not parse column {name}, mixed type");

        if values.len() != chunk_len {
            return Err(err(&mixed_length_err))
        }
        values.into_columns(name, self, config)
    }

    /// data should never be mixed type, otherwise this will return inconsistent results
    pub fn create_empty_columns(self, name: &str, config: &TableConfig) -> Vec<Column> {
        if self.is_256() {
            return self.create_empty_u256_columns(name, config);
        }
        vec![self.create_single_empty_column(name)]
    }

    /// Create empty columns for U256 types
    pub fn create_empty_u256_columns(self, name: &str, config: &TableConfig) -> Vec<Column> {
        config
            .u256_types
            .iter()
            .map(|u256_type| {
                let new_type = u256_type.to_columntype(config.binary_type);
                let full_name = name.to_string() + u256_type.suffix(self).as_str();
                new_type.create_single_empty_column(&full_name)
            })
            .collect()
    }

    /// Create an empty column of the specified type
    pub fn create_single_empty_column(self, name: &str) -> Column {
        match self {
            ColumnType::Boolean => Column::new(name.into(), Vec::<bool>::new()),
            ColumnType::UInt32 => Column::new(name.into(), Vec::<u32>::new()),
            ColumnType::UInt64 => Column::new(name.into(), Vec::<u64>::new()),
            ColumnType::UInt256 => {
                Column::new(format!("{name}_u256binary").into(), Vec::<RawBytes>::new())
            }
            ColumnType::Int32 => Column::new(name.into(), Vec::<i32>::new()),
            ColumnType::Int64 => Column::new(name.into(), Vec::<i64>::new()),
            ColumnType::Int256 => {
                Column::new(format!("{name}_i256binary").into(), Vec::<RawBytes>::new())
            }
            ColumnType::Float32 => Column::new(name.into(), Vec::<f32>::new()),
            ColumnType::Float64 => Column::new(name.into(), Vec::<f64>::new()),
            ColumnType::Decimal128 => Column::new(name.into(), Vec::<RawBytes>::new()),
            ColumnType::String => Column::new(name.into(), Vec::<String>::new()),
            ColumnType::Binary => Column::new(name.into(), Vec::<RawBytes>::new()),
            ColumnType::Hex => Column::new(name.into(), Vec::<String>::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{I256, U256};
    use polars::prelude::DataType;

    use crate::{ColumnEncoding, U256Type};

    use super::*;

    fn get_config() -> TableConfig {
        TableConfig {
            hex_prefix: true,
            binary_type: ColumnEncoding::Binary,
            u256_types: vec![U256Type::NamedBinary, U256Type::String, U256Type::F64],
        }
    }

    fn get_config_hex() -> TableConfig {
        TableConfig {
            hex_prefix: true,
            binary_type: ColumnEncoding::Hex,
            u256_types: vec![U256Type::NamedBinary, U256Type::String, U256Type::F64],
        }
    }

    macro_rules! test_empty_column {
        ($($ty:ident)+) => {
            $({
                let cols = ColumnType::$ty
                    .create_empty_columns(stringify!($ty), &get_config());
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0].dtype(), &DataType::$ty);
                assert_eq!(cols[0].len(), 0);
            })+
        };
    }

    macro_rules! test_sol_column {
        ($($ty:ident => $exp:expr,)+) => {
            $({
                let len = $exp.len();
                let cols = ColumnType::$ty
                    .create_column_from_values(stringify!($ty).to_string(), $exp.clone().into_iter().map(|i| i.into()).collect(), len, &get_config())
                    .unwrap();
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0].dtype(), &DataType::$ty);
                assert_eq!(cols[0].len(), len);
            })+
        };
    }

    macro_rules! test_dyn_column {
        ($($ty:ident => $exp:expr,)+) => {
            $({
                let len = $exp.len();
                let cols = DynValues::from($exp).into_columns(stringify!($ty).to_string(), ColumnType::$ty, &get_config()).unwrap();
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0].dtype(), &DataType::$ty);
                assert_eq!(cols[0].len(), len);
            })+
        };
    }

    #[test]
    fn test_empty_column_creation() {
        test_empty_column!(Boolean UInt32 UInt64 Int32 Int64 Float32 Float64 String Binary);
        let cols = ColumnType::Hex.create_empty_columns("Hex", &get_config());
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].dtype(), &DataType::String);
        assert_eq!(cols[0].len(), 0);

        let cols = ColumnType::Decimal128.create_empty_columns("Decimal128", &get_config());
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].dtype(), &DataType::Binary);
        assert_eq!(cols[0].len(), 0);

        let cols = ColumnType::UInt256.create_empty_columns("UInt256", &get_config());
        assert_eq!(cols.len(), 3);
        assert_eq!(
            cols.iter().map(|c| c.dtype().clone()).collect::<Vec<_>>(),
            vec![DataType::Binary, DataType::String, DataType::Float64]
        );
        assert_eq!(
            cols.iter().map(|c| c.name().to_string()).collect::<Vec<_>>(),
            vec![
                "UInt256_u256binary".to_string(),
                "UInt256_string".to_string(),
                "UInt256_f64".to_string()
            ]
        );
        assert!(cols.iter().all(|c| c.is_empty()));

        let cols = ColumnType::Int256.create_empty_columns("Int256", &get_config());
        assert_eq!(cols.len(), 3);
        assert_eq!(
            cols.iter().map(|c| c.dtype().clone()).collect::<Vec<_>>(),
            vec![DataType::Binary, DataType::String, DataType::Float64]
        );
        assert_eq!(
            cols.iter().map(|c| c.name().to_string()).collect::<Vec<_>>(),
            vec![
                "Int256_i256binary".to_string(),
                "Int256_string".to_string(),
                "Int256_f64".to_string()
            ]
        );
        assert!(cols.iter().all(|c| c.is_empty()));
    }

    #[test]
    fn test_column_creation() {
        test_sol_column!(
            Boolean => vec![true, false, true],
            UInt32 => vec![1u32, 2, 3],
            UInt64 => vec![1u64, 2, 3],
            Int32 => vec![-1i32, 0, 1],
            Int64 => vec![-1i64, 0, 1],
            // Float32 => vec![1.0, 2.0, 3.0],
            // Float64 => vec![1.0, 2.0, 3.0],
            String => vec!["a".to_string(), "b".to_string(), "c".to_string()],
            Binary => vec![vec![1, 2], vec![3, 4], vec![5, 6]],
        );

        test_dyn_column!(
            Boolean => vec![true, false, true],
            UInt32 => vec![1u32, 2, 3],
            UInt64 => vec![1u64, 2, 3],
            Int32 => vec![-1i32, 0, 1],
            Int64 => vec![-1i64, 0, 1],
            Float32 => vec![1.0, 2.0, 3.0],
            Float64 => vec![1.0, 2.0, 3.0],
            String => vec!["a".to_string(), "b".to_string(), "c".to_string()],
            Binary => vec![vec![1, 2], vec![3, 4], vec![5, 6]],
        );

        let cols = DynValues::from(vec![vec![1, 2], vec![3, 4], vec![5, 6]])
            .into_columns("Hex".to_string(), ColumnType::Hex, &get_config_hex())
            .unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].dtype(), &DataType::String);
        assert_eq!(cols[0].len(), 3);

        let cols = DynValues::from(vec![U256::from(1), U256::from(2), U256::from(3)])
            .into_columns("UInt256".to_string(), ColumnType::UInt256, &get_config())
            .unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(
            cols.iter().map(|c| c.dtype().clone()).collect::<Vec<_>>(),
            vec![DataType::Binary, DataType::String, DataType::Float64]
        );
        assert_eq!(
            cols.iter().map(|c| c.name().to_string()).collect::<Vec<_>>(),
            vec![
                "UInt256_u256binary".to_string(),
                "UInt256_string".to_string(),
                "UInt256_f64".to_string()
            ]
        );
        assert!(cols.iter().all(|c| c.len() == 3));

        let cols = DynValues::from(vec![
            I256::try_from(-1).unwrap(),
            I256::try_from(0).unwrap(),
            I256::try_from(1).unwrap(),
        ])
        .into_columns("Int256".to_string(), ColumnType::Int256, &get_config())
        .unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(
            cols.iter().map(|c| c.dtype().clone()).collect::<Vec<_>>(),
            vec![DataType::Binary, DataType::String, DataType::Float64]
        );
        assert_eq!(
            cols.iter().map(|c| c.name().to_string()).collect::<Vec<_>>(),
            vec![
                "Int256_i256binary".to_string(),
                "Int256_string".to_string(),
                "Int256_f64".to_string()
            ]
        );
        assert!(cols.iter().all(|c| c.len() == 3));
    }
}
