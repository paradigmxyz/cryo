use crate::*;
use alloy::primitives::{I256, U256};
use polars::prelude::*;

/// Converts a Vec of U256-like data into a polars Series
pub trait ToU256Series {
    /// convert a Vec of U256-like data into a polars Series
    fn to_u256_series(
        &self,
        name: String,
        dtype: U256Type,
        config: &TableConfig,
    ) -> Result<Column, CollectError>;
}

impl ToU256Series for OptionVec<U256> {
    fn to_u256_series(
        &self,
        name: String,
        dtype: U256Type,
        config: &TableConfig,
    ) -> Result<Column, CollectError> {
        match self {
            OptionVec::Some(v) => v.to_u256_series(name, dtype, config),
            OptionVec::Option(v) => v.to_u256_series(name, dtype, config),
        }
    }
}

impl ToU256Series for OptionVec<I256> {
    fn to_u256_series(
        &self,
        name: String,
        dtype: U256Type,
        config: &TableConfig,
    ) -> Result<Column, CollectError> {
        match self {
            OptionVec::Some(v) => v.to_u256_series(name, dtype, config),
            OptionVec::Option(v) => v.to_u256_series(name, dtype, config),
        }
    }
}

impl ToU256Series for Vec<U256> {
    fn to_u256_series(
        &self,
        name: String,
        dtype: U256Type,
        config: &TableConfig,
    ) -> Result<Column, CollectError> {
        let name = name + dtype.suffix(ColumnType::UInt256).as_str();
        let name = PlSmallStr::from_string(name);

        match dtype {
            U256Type::Binary | U256Type::NamedBinary => {
                let converted: Vec<RawBytes> = self.iter().map(|v| v.to_vec_u8()).collect();
                match config.binary_type {
                    ColumnEncoding::Hex => {
                        Ok(Column::new(name, converted.to_vec_hex(config.hex_prefix)))
                    }
                    ColumnEncoding::Binary => Ok(Column::new(name, converted)),
                }
            }
            U256Type::String => {
                let converted: Vec<String> = self.iter().map(|v| v.to_string()).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::F32 => {
                let converted: Vec<Option<f32>> =
                    self.iter().map(|v| v.to_string().parse::<f32>().ok()).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::F64 => {
                let converted: Vec<Option<f64>> =
                    self.iter().map(|v| v.to_string().parse::<f64>().ok()).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::U32 => {
                let converted: Vec<u32> = self.iter().map(|v| v.wrapping_to::<u32>()).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::U64 => {
                let converted: Vec<u64> = self.iter().map(|v| v.wrapping_to::<u64>()).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::Decimal128 => {
                Err(CollectError::CollectError("DECIMAL128 not implemented".to_string()))
            }
        }
    }
}

impl ToU256Series for Vec<Option<U256>> {
    fn to_u256_series(
        &self,
        name: String,
        dtype: U256Type,
        config: &TableConfig,
    ) -> Result<Column, CollectError> {
        let name = name + dtype.suffix(ColumnType::UInt256).as_str();
        let name = PlSmallStr::from_string(name);

        match dtype {
            U256Type::Binary | U256Type::NamedBinary => {
                let converted: Vec<Option<RawBytes>> =
                    self.iter().map(|v| v.map(|x| x.to_vec_u8())).collect();
                match config.binary_type {
                    ColumnEncoding::Hex => {
                        Ok(Column::new(name, converted.to_vec_hex(config.hex_prefix)))
                    }
                    ColumnEncoding::Binary => Ok(Column::new(name, converted)),
                }
            }
            U256Type::String => {
                let converted: Vec<Option<String>> =
                    self.iter().map(|v| v.map(|x| x.to_string())).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::F32 => {
                let converted: Vec<Option<f32>> = self
                    .iter()
                    .map(|v| v.map(|x| x.to_string().parse::<f32>().ok()).flatten())
                    .collect();
                Ok(Column::new(name, converted))
            }
            U256Type::F64 => {
                let converted: Vec<Option<f64>> = self
                    .iter()
                    .map(|v| v.map(|x| x.to_string().parse::<f64>().ok()).flatten())
                    .collect();
                Ok(Column::new(name, converted))
            }
            U256Type::U32 => {
                let converted: Vec<Option<u32>> =
                    self.iter().map(|v| v.map(|x| x.wrapping_to::<u32>())).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::U64 => {
                let converted: Vec<Option<u64>> =
                    self.iter().map(|v| v.map(|x| x.wrapping_to::<u64>())).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::Decimal128 => {
                Err(CollectError::CollectError("DECIMAL128 not implemented".to_string()))
            }
        }
    }
}

impl ToU256Series for Vec<I256> {
    fn to_u256_series(
        &self,
        name: String,
        dtype: U256Type,
        config: &TableConfig,
    ) -> Result<Column, CollectError> {
        let name = name + dtype.suffix(ColumnType::Int256).as_str();
        let name = PlSmallStr::from_string(name);

        match dtype {
            U256Type::Binary | U256Type::NamedBinary => {
                let converted: Vec<RawBytes> = self.iter().map(|v| v.to_vec_u8()).collect();
                match config.binary_type {
                    ColumnEncoding::Hex => {
                        Ok(Column::new(name, converted.to_vec_hex(config.hex_prefix)))
                    }
                    ColumnEncoding::Binary => Ok(Column::new(name, converted)),
                }
            }
            U256Type::String => {
                let converted: Vec<String> = self.iter().map(|v| v.to_string()).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::F32 => {
                let converted: Vec<Option<f32>> =
                    self.iter().map(|v| v.to_string().parse::<f32>().ok()).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::F64 => {
                let converted: Vec<Option<f64>> =
                    self.iter().map(|v| v.to_string().parse::<f64>().ok()).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::U32 => {
                let converted: Vec<u32> = self.iter().map(|v| v.as_u32()).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::U64 => {
                let converted: Vec<u64> = self.iter().map(|v| v.as_u64()).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::Decimal128 => {
                Err(CollectError::CollectError("DECIMAL128 not implemented".to_string()))
            }
        }
    }
}

impl ToU256Series for Vec<Option<I256>> {
    fn to_u256_series(
        &self,
        name: String,
        dtype: U256Type,
        config: &TableConfig,
    ) -> Result<Column, CollectError> {
        let name = name + dtype.suffix(ColumnType::Int256).as_str();
        let name = PlSmallStr::from_string(name);

        match dtype {
            U256Type::Binary | U256Type::NamedBinary => {
                let converted: Vec<Option<RawBytes>> =
                    self.iter().map(|v| v.map(|x| x.to_vec_u8())).collect();
                match config.binary_type {
                    ColumnEncoding::Hex => {
                        Ok(Column::new(name, converted.to_vec_hex(config.hex_prefix)))
                    }
                    ColumnEncoding::Binary => Ok(Column::new(name, converted)),
                }
            }
            U256Type::String => {
                let converted: Vec<Option<String>> =
                    self.iter().map(|v| v.map(|x| x.to_string())).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::F32 => {
                let converted: Vec<Option<f32>> = self
                    .iter()
                    .map(|v| v.map(|x| x.to_string().parse::<f32>().ok()).flatten())
                    .collect();
                Ok(Column::new(name, converted))
            }
            U256Type::F64 => {
                let converted: Vec<Option<f64>> = self
                    .iter()
                    .map(|v| v.map(|x| x.to_string().parse::<f64>().ok()).flatten())
                    .collect();
                Ok(Column::new(name, converted))
            }
            U256Type::U32 => {
                let converted: Vec<Option<u32>> =
                    self.iter().map(|v| v.map(|x| x.as_u32())).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::U64 => {
                let converted: Vec<Option<u64>> =
                    self.iter().map(|v| v.map(|x| x.as_u64())).collect();
                Ok(Column::new(name, converted))
            }
            U256Type::Decimal128 => {
                Err(CollectError::CollectError("DECIMAL128 not implemented".to_string()))
            }
        }
    }
}
