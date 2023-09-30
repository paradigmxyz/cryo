use ethers::prelude::*;

lazy_static::lazy_static! {
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

