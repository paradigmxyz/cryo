use ethers::prelude::*;

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
