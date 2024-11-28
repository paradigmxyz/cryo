use crate::{err, CollectError};
use alloy::{
    primitives::{Address, BlockNumber, B256},
    rpc::types::{Filter, FilterBlockOption},
};

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
    /// from address
    pub from_address: Option<Vec<u8>>,
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
        self.block_number()
    }

    /// ethers transaction
    pub fn ethers_transaction_hash(&self) -> Result<B256, CollectError> {
        Ok(B256::from_slice(&self.transaction_hash()?))
    }

    /// ethers address
    pub fn ethers_address(&self) -> Result<Address, CollectError> {
        Ok(Address::from_slice(&self.address()?))
    }

    /// ethers contract
    pub fn ethers_contract(&self) -> Result<Address, CollectError> {
        Ok(Address::from_slice(&self.contract()?))
    }

    /// log filter
    pub fn ethers_log_filter(&self) -> Result<Filter, CollectError> {
        let (start, end) = self.block_range()?;
        let block_option =
            FilterBlockOption::Range { from_block: Some(start.into()), to_block: Some(end.into()) };
        // let filter = Filter {
        //     block_option,
        //     address: self.address.clone().map(|x| ValueOrArray::Value(Address::from_slice(&x))),
        //     topics: [
        //         self.topic0.clone().map(|x| ValueOrArray::Value(Some(B256::from_slice(&x)))),
        //         self.topic1.clone().map(|x| ValueOrArray::Value(Some(B256::from_slice(&x)))),
        //         self.topic2.clone().map(|x| ValueOrArray::Value(Some(B256::from_slice(&x)))),
        //         self.topic3.clone().map(|x| ValueOrArray::Value(Some(B2::from_slice(&x)))),
        //     ],
        // };
        let mut filter = Filter::new();
        filter.block_option = block_option;
        if self.address.is_some() {
            filter = filter.address(Address::from_slice(&self.address.clone().unwrap()));
        }
        if self.topic0.is_some() {
            filter = filter.event_signature(B256::from_slice(&self.topic0.clone().unwrap()));
        }
        if self.topic1.is_some() {
            filter = filter.topic1(B256::from_slice(&self.topic1.clone().unwrap()));
        }
        if self.topic2.is_some() {
            filter = filter.topic2(B256::from_slice(&self.topic2.clone().unwrap()));
        }
        if self.topic3.is_some() {
            filter = filter.topic3(B256::from_slice(&self.topic3.clone().unwrap()));
        }
        Ok(filter)
    }
}
