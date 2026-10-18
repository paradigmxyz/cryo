use crate::{CollectError, OptionVec, RawBytes};
/// conversion operations
use alloy::primitives::{Bytes, I256, U256};

/// convert Bytes to u32
pub fn bytes_to_u32(value: Bytes) -> Result<u32, CollectError> {
    let v = value.to_vec();
    if v.len() == 32 && v[0..28].iter().all(|b| *b == 0) {
        Ok(u32::from_be_bytes([v[28], v[29], v[30], v[31]]))
    } else {
        Err(CollectError::CollectError("could not convert bytes to u32".to_string()))
    }
}

/// Converts data to Vec<u8>
pub trait ToVecU8 {
    /// Convert to Vec<u8>
    fn to_vec_u8(&self) -> Vec<u8>;
}

impl ToVecU8 for U256 {
    fn to_vec_u8(&self) -> Vec<u8> {
        self.to_be_bytes_vec()
    }
}

impl ToVecU8 for I256 {
    fn to_vec_u8(&self) -> Vec<u8> {
        self.into_raw().to_vec_u8()
    }
}

impl ToVecU8 for Vec<I256> {
    fn to_vec_u8(&self) -> Vec<u8> {
        self.iter().map(|x| x.into_raw()).collect::<Vec<_>>().to_vec_u8()
    }
}

impl ToVecU8 for Vec<U256> {
    fn to_vec_u8(&self) -> Vec<u8> {
        let mut vec = Vec::new();
        for value in self {
            vec.extend_from_slice(&value.to_be_bytes_vec())
        }
        vec
    }
}

// pub trait ToVecHex {
//     fn to_vec_hex(&self) -> Vec<String>;
// }

// impl ToVecHex for Vec<Vec<u8>> {
//     fn to_vec_hex(&self) -> Vec<String> {
//         self.iter().map(|v| prefix_hex::encode(v.clone())).collect()
//     }
// }

/// Encodes data as Vec of hex String
pub trait ToVecHex {
    /// Output type
    type Output;

    /// Convert to Vec of hex String
    fn to_vec_hex(&self, with_prefix: bool) -> Self::Output;
}

fn encode_hex<T: AsRef<[u8]>>(data: T, with_prefix: bool) -> String {
    if with_prefix {
        alloy::hex::encode_prefixed(data)
    } else {
        alloy::hex::encode(data)
    }
}

impl ToVecHex for Vec<RawBytes> {
    type Output = Vec<String>;

    fn to_vec_hex(&self, with_prefix: bool) -> Self::Output {
        self.iter().map(|v| encode_hex(v, with_prefix)).collect()
    }
}

impl ToVecHex for Vec<Option<RawBytes>> {
    type Output = Vec<Option<String>>;

    fn to_vec_hex(&self, with_prefix: bool) -> Self::Output {
        self.iter().map(|opt| opt.as_ref().map(|v| encode_hex(v, with_prefix))).collect()
    }
}

impl ToVecHex for OptionVec<RawBytes> {
    type Output = OptionVec<String>;

    fn to_vec_hex(&self, with_prefix: bool) -> Self::Output {
        match self {
            OptionVec::Some(v) => OptionVec::Some(v.to_vec_hex(with_prefix)),
            OptionVec::Option(v) => OptionVec::Option(v.to_vec_hex(with_prefix)),
        }
    }
}

/// Trait for converting single binary value to specific types
pub trait FromBinary {
    /// Convert from RawBytes to Self
    fn from_binary(value: RawBytes) -> Result<Self, CollectError>
    where
        Self: Sized;
}

/// Implementation for U256
impl FromBinary for U256 {
    fn from_binary(value: RawBytes) -> Result<Self, CollectError> {
        if value.len() >= 32 {
            Ok(U256::from_be_bytes(value[..32].try_into().unwrap_or([0u8; 32])))
        } else {
            // Pad with zeros if needed
            let mut padded = [0u8; 32];
            let start_idx = 32 - value.len();
            padded[start_idx..].copy_from_slice(&value);
            Ok(U256::from_be_bytes(padded))
        }
    }
}

/// Implementation for I256
impl FromBinary for I256 {
    fn from_binary(value: RawBytes) -> Result<Self, CollectError> {
        if value.len() >= 32 {
            let u256_bytes = value[..32].try_into().unwrap_or([0u8; 32]);
            let u256_val = U256::from_be_bytes(u256_bytes);
            Ok(I256::from_raw(u256_val))
        } else {
            // Pad with 0xff for signed integers (negative extension)
            let mut padded = [0xffu8; 32];
            let start_idx = 32 - value.len();
            padded[start_idx..].copy_from_slice(&value);
            let u256_val = U256::from_be_bytes(padded);
            Ok(I256::from_raw(u256_val))
        }
    }
}

/// Trait for converting binary data to specific types
pub trait FromBinaryVec {
    /// Convert from Vec<Option<RawBytes>> to Self
    fn from_binary_vec(data: Vec<Option<RawBytes>>) -> Result<Self, CollectError>
    where
        Self: Sized;
}

/// Implementation for Vec<Option<U256>>
impl FromBinaryVec for Vec<Option<U256>> {
    fn from_binary_vec(data: Vec<Option<RawBytes>>) -> Result<Self, CollectError> {
        let mut result = Vec::with_capacity(data.len());
        for opt_bytes in data {
            match opt_bytes {
                Some(bytes) => {
                    result.push(Some(U256::from_binary(bytes)?));
                }
                None => result.push(None),
            }
        }
        Ok(result)
    }
}

/// Implementation for Vec<U256>
impl FromBinaryVec for Vec<U256> {
    fn from_binary_vec(data: Vec<Option<RawBytes>>) -> Result<Self, CollectError> {
        let mut result = Vec::with_capacity(data.len());
        for (i, opt_bytes) in data.into_iter().enumerate() {
            match opt_bytes {
                Some(bytes) => {
                    result.push(U256::from_binary(bytes)?);
                }
                None => {
                    return Err(CollectError::CollectError(format!(
                        "Missing binary value at index {i}"
                    )))
                }
            }
        }
        Ok(result)
    }
}

/// Implementation for Vec<Option<I256>>
impl FromBinaryVec for Vec<Option<I256>> {
    fn from_binary_vec(data: Vec<Option<RawBytes>>) -> Result<Self, CollectError> {
        let mut result = Vec::with_capacity(data.len());
        for opt_bytes in data {
            match opt_bytes {
                Some(bytes) => {
                    result.push(Some(I256::from_binary(bytes)?));
                }
                None => result.push(None),
            }
        }
        Ok(result)
    }
}

/// Implementation for Vec<I256>
impl FromBinaryVec for Vec<I256> {
    fn from_binary_vec(data: Vec<Option<RawBytes>>) -> Result<Self, CollectError> {
        let mut result = Vec::with_capacity(data.len());
        for (i, opt_bytes) in data.into_iter().enumerate() {
            match opt_bytes {
                Some(bytes) => {
                    result.push(I256::from_binary(bytes)?);
                }
                None => {
                    return Err(CollectError::CollectError(format!(
                        "Missing binary value at index {i}"
                    )))
                }
            }
        }
        Ok(result)
    }
}
