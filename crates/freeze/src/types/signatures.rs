use ethers::prelude::*;

lazy_static::lazy_static! {

    /// function signature of FUNCTION_ERC20_NAME
    pub static ref FUNCTION_ERC20_NAME: Vec<u8> = prefix_hex::decode("0x06fdde03").expect("Decoding failed");

    /// function signature of FUNCTION_ERC20_SYMBOL
    pub static ref FUNCTION_ERC20_SYMBOL: Vec<u8> = prefix_hex::decode("0x95d89b41").expect("Decoding failed");

    /// function signature of FUNCTION_ERC20_DECIMALS
    pub static ref FUNCTION_ERC20_DECIMALS: Vec<u8> = prefix_hex::decode("0x313ce567").expect("Decoding failed");

    /// function signature of FUNCTION_ERC20_BALANCE_OF
    pub static ref FUNCTION_ERC20_BALANCE_OF: Vec<u8> = prefix_hex::decode("0x70a08231").expect("Decoding failed");

    /// function signature of FUNCTION_ERC20_TOTAL_SUPPLY
    pub static ref FUNCTION_ERC20_TOTAL_SUPPLY: Vec<u8> = prefix_hex::decode("0x18160ddd").expect("Decoding failed");

    /// event hash of EVENT_ERC20_TRANSFER
    pub static ref EVENT_ERC20_TRANSFER: H256 = H256(
        prefix_hex::decode("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
            .expect("Decoding failed"),
    );

    /// event hash of EVENT_ERC721_TRANSFER
    pub static ref EVENT_ERC721_TRANSFER: H256 = H256(
        prefix_hex::decode("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
            .expect("Decoding failed"),
    );
}
