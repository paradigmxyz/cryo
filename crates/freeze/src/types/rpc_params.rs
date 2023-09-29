use ethers::prelude::*;

/// represents parameters for a single rpc call
#[derive(Default, Clone)]
pub struct RpcParams {
    /// block number
    pub block_number: Option<u64>,
    /// block range
    pub block_range: Option<(u64, u64)>,
    /// transaction
    pub transaction_hash: Option<Vec<u8>>,
    /// call data
    pub call_data: Option<Vec<u8>>,
    /// address
    pub address: Option<Vec<u8>>,
    /// contract
    pub contract: Option<Vec<u8>>,
    /// to address
    pub to_address: Option<Vec<u8>>,
    /// slot
    pub slot: Option<Vec<u8>>,
    /// topic0
    pub topic0: Option<Vec<u8>>,
    /// topic1
    pub topic1: Option<Vec<u8>>,
    /// topic2
    pub topic2: Option<Vec<u8>>,
    /// topic3
    pub topic3: Option<Vec<u8>>,
}

impl RpcParams {
    /// block number
    pub fn block_number(&self) -> u64 {
        self.block_number.expect("block_number not specified")
    }

    /// block range
    pub fn block_range(&self) -> (u64, u64) {
        self.block_range.expect("block_range not specified")
    }

    /// transaction
    pub fn transaction_hash(&self) -> Vec<u8> {
        self.transaction_hash.clone().expect("transaction not specified")
    }

    /// address
    pub fn address(&self) -> Vec<u8> {
        self.address.clone().expect("address not specified")
    }

    //
    // ethers versions
    //

    /// ethers block number
    pub fn ethers_block_number(&self) -> BlockNumber {
        self.block_number().into()
    }

    /// ethers transaction
    pub fn ethers_transaction_hash(&self) -> H256 {
        H256::from_slice(&self.transaction_hash())
    }

    /// ethers address
    pub fn ethers_address(&self) -> H160 {
        H160::from_slice(&self.address())
    }

    /// log filter
    pub fn ethers_log_filter(&self) -> Filter {
        let (start, end) = self.block_range();
        let block_option =
            FilterBlockOption::Range { from_block: Some(start.into()), to_block: Some(end.into()) };
        Filter {
            block_option,
            address: self.address.clone().map(|x| ValueOrArray::Value(H160::from_slice(&x))),
            topics: [
                self.topic0.clone().map(|x| ValueOrArray::Value(Some(H256::from_slice(&x)))),
                self.topic1.clone().map(|x| ValueOrArray::Value(Some(H256::from_slice(&x)))),
                self.topic2.clone().map(|x| ValueOrArray::Value(Some(H256::from_slice(&x)))),
                self.topic3.clone().map(|x| ValueOrArray::Value(Some(H256::from_slice(&x)))),
            ],
        }
    }
}
