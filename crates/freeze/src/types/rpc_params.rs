use crate::{err, CollectError};
use ethers::prelude::*;

/// represents parameters for a single rpc call
#[derive(Default, Clone, Debug)]
pub struct Params {
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

impl Params {
    /// block number
    pub fn block_number(&self) -> Result<u64, CollectError> {
        self.block_number.ok_or(err("block_number not specified"))
    }

    /// block range
    pub fn block_range(&self) -> Result<(u64, u64), CollectError> {
        self.block_range.ok_or(err("block_range not specified"))
    }

    /// transaction
    pub fn transaction_hash(&self) -> Result<Vec<u8>, CollectError> {
        self.transaction_hash.clone().ok_or(err("transaction not specified"))
    }

    /// address
    pub fn address(&self) -> Result<Vec<u8>, CollectError> {
        self.address.clone().ok_or(err("address not specified"))
    }

    /// contract
    pub fn contract(&self) -> Result<Vec<u8>, CollectError> {
        self.contract.clone().ok_or(err("contract not specified"))
    }

    /// slot
    pub fn slot(&self) -> Result<Vec<u8>, CollectError> {
        self.slot.clone().ok_or(err("slot not specified"))
    }

    /// call_data
    pub fn call_data(&self) -> Result<Vec<u8>, CollectError> {
        self.call_data.clone().ok_or(err("call_data not specified"))
    }

    //
    // ethers versions
    //

    /// ethers block number
    pub fn ethers_block_number(&self) -> Result<BlockNumber, CollectError> {
        Ok(self.block_number()?.into())
    }

    /// ethers transaction
    pub fn ethers_transaction_hash(&self) -> Result<H256, CollectError> {
        Ok(H256::from_slice(&self.transaction_hash()?))
    }

    /// ethers address
    pub fn ethers_address(&self) -> Result<H160, CollectError> {
        Ok(H160::from_slice(&self.address()?))
    }

    /// ethers contract
    pub fn ethers_contract(&self) -> Result<H160, CollectError> {
        Ok(H160::from_slice(&self.contract()?))
    }

    /// log filter
    pub fn ethers_log_filter(&self) -> Result<Filter, CollectError> {
        let (start, end) = self.block_range()?;
        let block_option =
            FilterBlockOption::Range { from_block: Some(start.into()), to_block: Some(end.into()) };
        let filter = Filter {
            block_option,
            address: self.address.clone().map(|x| ValueOrArray::Value(H160::from_slice(&x))),
            topics: [
                self.topic0.clone().map(|x| ValueOrArray::Value(Some(H256::from_slice(&x)))),
                self.topic1.clone().map(|x| ValueOrArray::Value(Some(H256::from_slice(&x)))),
                self.topic2.clone().map(|x| ValueOrArray::Value(Some(H256::from_slice(&x)))),
                self.topic3.clone().map(|x| ValueOrArray::Value(Some(H256::from_slice(&x)))),
            ],
        };
        Ok(filter)
    }
}
