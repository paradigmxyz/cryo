use crate::CollectError;
/// conversion operations
use alloy::primitives::{Bytes, I256, U256};
use prefix_hex;

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
    fn to_vec_hex(&self) -> Self::Output;
}

impl ToVecHex for Vec<Vec<u8>> {
    type Output = Vec<String>;

    fn to_vec_hex(&self) -> Self::Output {
        self.iter().map(|v| prefix_hex::encode(v.clone())).collect()
    }
}

impl ToVecHex for Vec<Option<Vec<u8>>> {
    type Output = Vec<Option<String>>;

    fn to_vec_hex(&self) -> Self::Output {
        self.iter().map(|opt| opt.as_ref().map(|v| prefix_hex::encode(v.clone()))).collect()
    }
}
