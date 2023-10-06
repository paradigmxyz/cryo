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

/// convert a Vec<U256> to variety of u256 Series representations
#[macro_export]
macro_rules! with_series_u256 {
    ($all_series:expr, $name:expr, $value:expr, $schema:expr) => {
        if $schema.has_column($name) {
            // binary
            if $schema.u256_types.contains(&U256Type::Binary) {
                let name = $name.to_string() + U256Type::Binary.suffix().as_str();
                let name = name.as_str();

                let converted: Vec<Vec<u8>> = $value.iter().map(|v| v.to_vec_u8()).collect();
                if ColumnEncoding::Hex == $schema.binary_type {
                    $all_series.push(Series::new(name, converted.to_vec_hex()));
                } else {
                    $all_series.push(Series::new(name, converted));
                }
            }

            // string
            if $schema.u256_types.contains(&U256Type::String) {
                let name = $name.to_string() + U256Type::String.suffix().as_str();
                let name = name.as_str();

                let converted: Vec<String> = $value.iter().map(|v| v.to_string()).collect();
                $all_series.push(Series::new(name, converted));
            }

            // float32
            if $schema.u256_types.contains(&U256Type::F32) {
                let name = $name.to_string() + U256Type::F32.suffix().as_str();
                let name = name.as_str();

                let converted: Vec<Option<f32>> =
                    $value.iter().map(|v| v.to_string().parse::<f32>().ok()).collect();
                $all_series.push(Series::new(name, converted));
            }

            // float64
            if $schema.u256_types.contains(&U256Type::F64) {
                let name = $name.to_string() + U256Type::F64.suffix().as_str();
                let name = name.as_str();

                let converted: Vec<Option<f64>> =
                    $value.iter().map(|v| v.to_string().parse::<f64>().ok()).collect();
                $all_series.push(Series::new(name, converted));
            }

            // u32
            if $schema.u256_types.contains(&U256Type::U32) {
                let name = $name.to_string() + U256Type::U32.suffix().as_str();
                let name = name.as_str();

                let converted: Vec<u32> = $value.iter().map(|v| v.as_u32()).collect();
                $all_series.push(Series::new(name, converted));
            }

            // u64
            if $schema.u256_types.contains(&U256Type::U64) {
                let name = $name.to_string() + U256Type::U64.suffix().as_str();
                let name = name.as_str();

                let converted: Vec<u64> = $value.iter().map(|v| v.as_u64()).collect();
                $all_series.push(Series::new(name, converted));
            }

            // decimal128
            if $schema.u256_types.contains(&U256Type::Decimal128) {
                return Err(CollectError::CollectError("DECIMAL128 not implemented".to_string()))
            }
        }
    };
}

/// convert a Vec<Option<U256>> to variety of u256 Series representations
#[macro_export]
macro_rules! with_series_option_u256 {
    ($all_series:expr, $name:expr, $value:expr, $schema:expr) => {
        if $schema.has_column($name) {
            // binary
            if $schema.u256_types.contains(&U256Type::Binary) {
                let name = $name.to_string() + U256Type::Binary.suffix().as_str();
                let name = name.as_str();

                let converted: Vec<Option<Vec<u8>>> =
                    $value.iter().map(|v| v.map(|x| x.to_vec_u8())).collect();
                if ColumnEncoding::Hex == $schema.binary_type {
                    $all_series.push(Series::new(name, converted.to_vec_hex()));
                } else {
                    $all_series.push(Series::new(name, converted));
                }
            }

            // string
            if $schema.u256_types.contains(&U256Type::String) {
                let name = $name.to_string() + U256Type::String.suffix().as_str();
                let name = name.as_str();

                let converted: Vec<Option<String>> =
                    $value.iter().map(|v| v.map(|x| x.to_string())).collect();
                $all_series.push(Series::new(name, converted));
            }

            // float32
            if $schema.u256_types.contains(&U256Type::F32) {
                let name = $name.to_string() + U256Type::F32.suffix().as_str();
                let name = name.as_str();

                let converted: Vec<Option<f32>> = $value
                    .iter()
                    .map(|v| v.map(|x| x.to_string().parse::<f32>().ok()).flatten())
                    .collect();
                $all_series.push(Series::new(name, converted));
            }

            // float64
            if $schema.u256_types.contains(&U256Type::F64) {
                let name = $name.to_string() + U256Type::F64.suffix().as_str();
                let name = name.as_str();

                let converted: Vec<Option<f64>> = $value
                    .iter()
                    .map(|v| v.map(|x| x.to_string().parse::<f64>().ok()).flatten())
                    .collect();
                $all_series.push(Series::new(name, converted));
            }

            // u32
            if $schema.u256_types.contains(&U256Type::U32) {
                let name = $name.to_string() + U256Type::U32.suffix().as_str();
                let name = name.as_str();

                let converted: Vec<Option<u32>> =
                    $value.iter().map(|v| v.map(|x| x.as_u32())).collect();
                $all_series.push(Series::new(name, converted));
            }

            // u64
            if $schema.u256_types.contains(&U256Type::U64) {
                let name = $name.to_string() + U256Type::U64.suffix().as_str();
                let name = name.as_str();

                let converted: Vec<Option<u64>> =
                    $value.iter().map(|v| v.map(|x| x.as_u64())).collect();
                $all_series.push(Series::new(name, converted));
            }

            // decimal128
            if $schema.u256_types.contains(&U256Type::Decimal128) {
                return Err(CollectError::CollectError("DECIMAL128 not implemented".to_string()))
            }
        }
    };
}
