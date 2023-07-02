use ethers::prelude::*;
use prefix_hex;

pub trait ToVecU8 {
    fn to_vec_u8(&self) -> Vec<u8>;
}

impl ToVecU8 for U256 {
    fn to_vec_u8(&self) -> Vec<u8> {
        let mut vec = Vec::new();
        for &number in self.0.iter() {
            vec.extend_from_slice(&number.to_ne_bytes());
        }
        vec
    }
}

impl ToVecU8 for Vec<U256> {
    fn to_vec_u8(&self) -> Vec<u8> {
        let mut vec = Vec::new();
        for value in self {
            for &number in value.0.iter() {
                vec.extend_from_slice(&number.to_ne_bytes());
            }
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

pub trait ToVecHex {
    type Output;

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

