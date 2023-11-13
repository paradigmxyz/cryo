use crate::*;
use ethers::prelude::*;
use polars::prelude::*;

/// Converts a Vec of U256-like data into a polars Series
pub trait ToU256Series {
    /// convert a Vec of U256-like data into a polars Series
    fn to_u256_series(
        &self,
        name: String,
        dtype: U256Type,
        column_encoding: &ColumnEncoding,
    ) -> Result<Series, CollectError>;
}

impl ToU256Series for Vec<U256> {
    fn to_u256_series(
        &self,
        name: String,
        dtype: U256Type,
        column_encoding: &ColumnEncoding,
    ) -> Result<Series, CollectError> {
        let name = name + dtype.suffix().as_str();
        let name = name.as_str();

        match dtype {
            U256Type::Binary => {
                let converted: Vec<Vec<u8>> = self.iter().map(|v| v.to_vec_u8()).collect();
                match column_encoding {
                    ColumnEncoding::Hex => Ok(Series::new(name, converted.to_vec_hex())),
                    ColumnEncoding::Binary => Ok(Series::new(name, converted)),
                }
            }
            U256Type::String => {
                let converted: Vec<String> = self.iter().map(|v| v.to_string()).collect();
                Ok(Series::new(name, converted))
            }
            U256Type::F32 => {
                let converted: Vec<Option<f32>> =
                    self.iter().map(|v| v.to_string().parse::<f32>().ok()).collect();
                Ok(Series::new(name, converted))
            }
            U256Type::F64 => {
                let converted: Vec<Option<f64>> =
                    self.iter().map(|v| v.to_string().parse::<f64>().ok()).collect();
                Ok(Series::new(name, converted))
            }
            U256Type::U32 => {
                let converted: Vec<u32> = self.iter().map(|v| v.as_u32()).collect();
                Ok(Series::new(name, converted))
            }
            U256Type::U64 => {
                let converted: Vec<u64> = self.iter().map(|v| v.as_u64()).collect();
                Ok(Series::new(name, converted))
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
        column_encoding: &ColumnEncoding,
    ) -> Result<Series, CollectError> {
        let name = name + dtype.suffix().as_str();
        let name = name.as_str();

        match dtype {
            U256Type::Binary => {
                let converted: Vec<Option<Vec<u8>>> =
                    self.iter().map(|v| v.map(|x| x.to_vec_u8())).collect();
                match column_encoding {
                    ColumnEncoding::Hex => Ok(Series::new(name, converted.to_vec_hex())),
                    ColumnEncoding::Binary => Ok(Series::new(name, converted)),
                }
            }
            U256Type::String => {
                let converted: Vec<Option<String>> =
                    self.iter().map(|v| v.map(|x| x.to_string())).collect();
                Ok(Series::new(name, converted))
            }
            U256Type::F32 => {
                let converted: Vec<Option<f32>> = self
                    .iter()
                    .map(|v| v.map(|x| x.to_string().parse::<f32>().ok()).flatten())
                    .collect();
                Ok(Series::new(name, converted))
            }
            U256Type::F64 => {
                let converted: Vec<Option<f64>> = self
                    .iter()
                    .map(|v| v.map(|x| x.to_string().parse::<f64>().ok()).flatten())
                    .collect();
                Ok(Series::new(name, converted))
            }
            U256Type::U32 => {
                let converted: Vec<Option<u32>> =
                    self.iter().map(|v| v.map(|x| x.as_u32())).collect();
                Ok(Series::new(name, converted))
            }
            U256Type::U64 => {
                let converted: Vec<Option<u64>> =
                    self.iter().map(|v| v.map(|x| x.as_u64())).collect();
                Ok(Series::new(name, converted))
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
        column_encoding: &ColumnEncoding,
    ) -> Result<Series, CollectError> {
        let name = name + dtype.suffix().as_str();
        let name = name.as_str();

        match dtype {
            U256Type::Binary => {
                let converted: Vec<Vec<u8>> = self.iter().map(|v| v.to_vec_u8()).collect();
                match column_encoding {
                    ColumnEncoding::Hex => Ok(Series::new(name, converted.to_vec_hex())),
                    ColumnEncoding::Binary => Ok(Series::new(name, converted)),
                }
            }
            U256Type::String => {
                let converted: Vec<String> = self.iter().map(|v| v.to_string()).collect();
                Ok(Series::new(name, converted))
            }
            U256Type::F32 => {
                let converted: Vec<Option<f32>> =
                    self.iter().map(|v| v.to_string().parse::<f32>().ok()).collect();
                Ok(Series::new(name, converted))
            }
            U256Type::F64 => {
                let converted: Vec<Option<f64>> =
                    self.iter().map(|v| v.to_string().parse::<f64>().ok()).collect();
                Ok(Series::new(name, converted))
            }
            U256Type::U32 => {
                let converted: Vec<u32> = self.iter().map(|v| v.as_u32()).collect();
                Ok(Series::new(name, converted))
            }
            U256Type::U64 => {
                let converted: Vec<u64> = self.iter().map(|v| v.as_u64()).collect();
                Ok(Series::new(name, converted))
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
        column_encoding: &ColumnEncoding,
    ) -> Result<Series, CollectError> {
        let name = name + dtype.suffix().as_str();
        let name = name.as_str();

        match dtype {
            U256Type::Binary => {
                let converted: Vec<Option<Vec<u8>>> =
                    self.iter().map(|v| v.map(|x| x.to_vec_u8())).collect();
                match column_encoding {
                    ColumnEncoding::Hex => Ok(Series::new(name, converted.to_vec_hex())),
                    ColumnEncoding::Binary => Ok(Series::new(name, converted)),
                }
            }
            U256Type::String => {
                let converted: Vec<Option<String>> =
                    self.iter().map(|v| v.map(|x| x.to_string())).collect();
                Ok(Series::new(name, converted))
            }
            U256Type::F32 => {
                let converted: Vec<Option<f32>> = self
                    .iter()
                    .map(|v| v.map(|x| x.to_string().parse::<f32>().ok()).flatten())
                    .collect();
                Ok(Series::new(name, converted))
            }
            U256Type::F64 => {
                let converted: Vec<Option<f64>> = self
                    .iter()
                    .map(|v| v.map(|x| x.to_string().parse::<f64>().ok()).flatten())
                    .collect();
                Ok(Series::new(name, converted))
            }
            U256Type::U32 => {
                let converted: Vec<Option<u32>> =
                    self.iter().map(|v| v.map(|x| x.as_u32())).collect();
                Ok(Series::new(name, converted))
            }
            U256Type::U64 => {
                let converted: Vec<Option<u64>> =
                    self.iter().map(|v| v.map(|x| x.as_u64())).collect();
                Ok(Series::new(name, converted))
            }
            U256Type::Decimal128 => {
                Err(CollectError::CollectError("DECIMAL128 not implemented".to_string()))
            }
        }
    }
}
