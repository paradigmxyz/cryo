use alloy::{
    dyn_abi::DynSolValue,
    primitives::{I256, U256},
};
use polars::{
    prelude::{Column, NamedFrom},
    series::Series,
};

use crate::{
    err, CollectError, ColumnType, RawBytes, TableConfig, ToU256Series, ToVecHex, U256Type,
};

/// A vector that can hold either values or optional values.
pub enum OptionVec<T> {
    /// A vector of values
    Some(Vec<T>),
    /// A vector of optional values
    Option(Vec<Option<T>>),
}

impl<T> OptionVec<T> {
    /// Returns whether the vector is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            OptionVec::Some(v) => v.is_empty(),
            OptionVec::Option(v) => v.is_empty(),
        }
    }

    /// Get the length of the vector
    pub fn len(&self) -> usize {
        match self {
            OptionVec::Some(v) => v.len(),
            OptionVec::Option(v) => v.len(),
        }
    }

    /// Map the elements of the vector
    pub fn map<U>(self, mut f: impl FnMut(T) -> U) -> OptionVec<U> {
        match self {
            OptionVec::Some(v) => OptionVec::Some(v.into_iter().map(f).collect()),
            OptionVec::Option(v) => {
                OptionVec::Option(v.into_iter().map(|opt| opt.map(&mut f)).collect())
            }
        }
    }

    /// Convert into a polars Column
    pub fn into_column<P1: ?Sized, P2: ?Sized>(self, name: String) -> Column
    where
        Series: NamedFrom<Vec<T>, P1>,
        Series: NamedFrom<Vec<Option<T>>, P2>,
    {
        match self {
            OptionVec::Some(v) => Column::new(name.into(), v),
            OptionVec::Option(v) => Column::new(name.into(), v),
        }
    }

    /// Convert into a variety of u256 Column representations
    pub fn into_u256_columns(
        self,
        name: String,
        config: &TableConfig,
    ) -> Result<Vec<Column>, CollectError>
    where
        Vec<T>: ToU256Series,
        Vec<Option<T>>: ToU256Series,
    {
        match self {
            OptionVec::Some(v) => {
                let mut series_vec = Vec::new();
                for u256_type in config.u256_types.iter() {
                    series_vec.push(v.to_u256_series(
                        name.to_string(),
                        u256_type.clone(),
                        config,
                    )?)
                }
                Ok(series_vec)
            }
            OptionVec::Option(v) => {
                let mut series_vec = Vec::new();
                for u256_type in config.u256_types.iter() {
                    series_vec.push(v.to_u256_series(
                        name.to_string(),
                        u256_type.clone(),
                        config,
                    )?)
                }
                Ok(series_vec)
            }
        }
    }
}

impl<T> TryInto<Vec<T>> for OptionVec<T>
where
    T: Default,
{
    type Error = crate::CollectError;

    fn try_into(self) -> Result<Vec<T>, Self::Error> {
        match self {
            OptionVec::Some(v) => Ok(v),
            OptionVec::Option(v) => v
                .into_iter()
                .enumerate()
                .map(|(i, opt)| {
                    opt.ok_or_else(|| {
                        crate::CollectError::CollectError(format!("Missing value at index {i}"))
                    })
                })
                .collect::<Result<Vec<_>, _>>(),
        }
    }
}

impl<T> TryInto<Vec<Option<T>>> for OptionVec<T> {
    type Error = crate::CollectError;

    fn try_into(self) -> Result<Vec<Option<T>>, Self::Error> {
        match self {
            OptionVec::Some(v) => Ok(v.into_iter().map(Some).collect()),
            OptionVec::Option(v) => Ok(v),
        }
    }
}

/// A collection of dynamic values that can be used in a DataFrame column.
pub enum DynValues {
    /// int
    Ints(OptionVec<i64>),
    /// uint
    UInts(OptionVec<u64>),
    /// u256
    U256s(OptionVec<U256>),
    /// i256
    I256s(OptionVec<I256>),
    /// f64
    F64s(OptionVec<f64>),
    /// bytes
    Bytes(OptionVec<RawBytes>),
    /// bool
    Bools(OptionVec<bool>),
    /// string
    Strings(OptionVec<String>),
}

macro_rules! impl_from_vec {
    ($($ty:ty $(, $ty2:ty)* => $variant:ident,)+) => {
        $(impl From<Vec<$ty>> for DynValues {
            fn from(value: Vec<$ty>) -> Self {
                DynValues::$variant(OptionVec::Some(value))
            }
        }
        impl From<Vec<Option<$ty>>> for DynValues {
            fn from(value: Vec<Option<$ty>>) -> Self {
                DynValues::$variant(OptionVec::Option(value))
            }
        }

        $(impl From<Vec<$ty2>> for DynValues {
            fn from(value: Vec<$ty2>) -> Self {
                DynValues::$variant(OptionVec::Some(value).map(|v| v.into()))
            }
        }
        impl From<Vec<Option<$ty2>>> for DynValues {
            fn from(value: Vec<Option<$ty2>>) -> Self {
                DynValues::$variant(OptionVec::Option(value).map(|v| v.into()))
            }
        })*
        )+
    };
}

impl_from_vec! {
    i64, i32 => Ints,
    u64, u32 => UInts,
    U256 => U256s,
    I256 => I256s,
    f64, f32 => F64s,
    RawBytes => Bytes,
    String => Strings,
    bool => Bools,
}

impl DynValues {
    /// Returns the data type as a string.
    pub fn dtype_str(&self) -> &'static str {
        match self {
            DynValues::Ints(_) => "Int64",
            DynValues::UInts(_) => "UInt64",
            DynValues::U256s(_) => "UInt256",
            DynValues::I256s(_) => "Int256",
            DynValues::F64s(_) => "Float64",
            DynValues::Bytes(_) => "Binary",
            DynValues::Bools(_) => "Boolean",
            DynValues::Strings(_) => "String",
        }
    }

    /// Create a new `DynValues` instance from a vector of `DynSolValue`s.
    pub fn from_sol_values(
        data: Vec<DynSolValue>,
        _config: &TableConfig,
    ) -> Result<Self, CollectError> {
        // This is a smooth brain way of doing this, but I can't think of a better way right now
        let mut ints: Vec<i64> = vec![];
        let mut uints: Vec<u64> = vec![];
        let mut u256s: Vec<U256> = vec![];
        let mut i256s: Vec<I256> = vec![];
        let mut bytes: Vec<RawBytes> = vec![];
        let mut bools: Vec<bool> = vec![];
        let mut strings: Vec<String> = vec![];
        // TODO: support array & tuple types

        for token in data {
            match token {
                DynSolValue::Address(a) => {
                    bytes.push(a.to_vec());
                }
                DynSolValue::FixedBytes(b, _) => {
                    bytes.push(b.to_vec());
                }
                DynSolValue::Bytes(b) => {
                    bytes.push(b);
                }
                DynSolValue::Uint(i, size) => {
                    if size <= 64 {
                        uints.push(i.wrapping_to::<u64>())
                    } else {
                        u256s.push(i)
                    }
                }
                DynSolValue::Int(i, size) => {
                    if size <= 64 {
                        ints.push(i.unchecked_into());
                    } else {
                        i256s.push(i);
                    }
                }
                DynSolValue::Bool(b) => bools.push(b),
                DynSolValue::String(s) => strings.push(s),
                DynSolValue::Array(_) | DynSolValue::FixedArray(_) => {}
                DynSolValue::Tuple(_) => {}
                DynSolValue::Function(_) => {}
            }
        }

        let result = if !ints.is_empty() {
            DynValues::Ints(OptionVec::Some(ints))
        } else if !i256s.is_empty() {
            DynValues::I256s(OptionVec::Some(i256s))
        } else if !u256s.is_empty() {
            DynValues::U256s(OptionVec::Some(u256s))
        } else if !uints.is_empty() {
            DynValues::UInts(OptionVec::Some(uints))
        } else if !bytes.is_empty() {
            DynValues::Bytes(OptionVec::Some(bytes))
        } else if !bools.is_empty() {
            DynValues::Bools(OptionVec::Some(bools))
        } else if !strings.is_empty() {
            DynValues::Strings(OptionVec::Some(strings))
        } else {
            return Err(err("could not parse column, mixed type"))
        };
        Ok(result)
    }

    /// Returns whether the underlying data is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the length of the underlying data.
    pub fn len(&self) -> usize {
        match self {
            DynValues::Ints(ints) => ints.len(),
            DynValues::I256s(i256s) => i256s.len(),
            DynValues::U256s(u256s) => u256s.len(),
            DynValues::UInts(uints) => uints.len(),
            DynValues::F64s(f64s) => f64s.len(),
            DynValues::Bytes(bytes) => bytes.len(),
            DynValues::Bools(bools) => bools.len(),
            DynValues::Strings(strings) => strings.len(),
        }
    }

    /// Converts the `DynValues` into `Vec<Column>`.
    pub fn into_columns(
        self,
        name: String,
        col_type: ColumnType,
        config: &TableConfig,
    ) -> Result<Vec<Column>, CollectError> {
        let mixed_type_err = format!(
            "could not parse column {name}, mixed type expect {col_type:?}, actually {}",
            self.dtype_str(),
        );
        match self {
            Self::Ints(ints) => match col_type {
                ColumnType::Int256 => {
                    ints.map(|v| I256::try_from(v).unwrap()).into_u256_columns(name, config)
                }
                ColumnType::Int32 => Ok(vec![ints.map(|v| v as i32).into_column(name)]),
                ColumnType::Int64 => Ok(vec![ints.into_column(name)]),
                ColumnType::Float32 => Ok(vec![ints.map(|v| v as f32).into_column(name)]),
                ColumnType::Float64 => Ok(vec![ints.map(|v| v as f64).into_column(name)]),
                _ => Err(err(&mixed_type_err)),
            },
            Self::UInts(uints) => match col_type {
                ColumnType::UInt256 => uints.map(|v| U256::from(v)).into_u256_columns(name, config),
                ColumnType::UInt32 => Ok(vec![uints.map(|v| v as u32).into_column(name)]),
                ColumnType::UInt64 => Ok(vec![uints.into_column(name)]),
                ColumnType::Float32 => Ok(vec![uints.map(|v| v as f32).into_column(name)]),
                ColumnType::Float64 => Ok(vec![uints.map(|v| v as f64).into_column(name)]),
                _ => Err(err(&mixed_type_err)),
            },
            Self::I256s(i256s) => match col_type {
                ColumnType::Int256 => i256s.into_u256_columns(name, config),
                ColumnType::Float32 => {
                    Ok(vec![i256s.to_u256_series(name, U256Type::F32, config)?])
                }
                ColumnType::Float64 => {
                    Ok(vec![i256s.to_u256_series(name, U256Type::F64, config)?])
                }
                ColumnType::Binary => {
                    Ok(vec![i256s.to_u256_series(name, U256Type::Binary, config)?])
                }
                _ => Err(err(&mixed_type_err)),
            },
            Self::U256s(u256s) => match col_type {
                ColumnType::UInt256 => u256s.into_u256_columns(name, config),
                ColumnType::Float32 => {
                    Ok(vec![u256s.to_u256_series(name, U256Type::F32, config)?])
                }
                ColumnType::Float64 => {
                    Ok(vec![u256s.to_u256_series(name, U256Type::F64, config)?])
                }
                ColumnType::Binary => {
                    Ok(vec![u256s.to_u256_series(name, U256Type::Binary, config)?])
                }
                _ => Err(err(&mixed_type_err)),
            },
            Self::F64s(f64s) => match col_type {
                ColumnType::Float32 => Ok(vec![f64s.map(|v| v as f32).into_column(name)]),
                ColumnType::Float64 => Ok(vec![f64s.into_column(name)]),
                _ => Err(err(&mixed_type_err)),
            },
            Self::Bytes(bytes) => match col_type {
                ColumnType::Binary => Ok(vec![bytes.into_column(name)]),
                ColumnType::Hex => Ok(vec![bytes.to_vec_hex(config.hex_prefix).into_column(name)]),
                _ => Err(err(&mixed_type_err)),
            },
            Self::Bools(bools) => Ok(vec![bools.into_column(name)]),
            Self::Strings(strings) => Ok(vec![strings.into_column(name)]),
        }
    }
}
