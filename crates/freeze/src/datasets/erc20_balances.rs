use crate::*;
use alloy::{primitives::U256, sol_types::SolCall};
use polars::prelude::*;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct Erc20Balances {
    n_rows: u64,
    block_number: Vec<u32>,
    erc20: Vec<RawBytes>,
    address: Vec<RawBytes>,
    balance: Vec<Option<U256>>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Erc20Balances {
    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Contract, Dim::Address]
    }
}

#[async_trait::async_trait]
impl CollectByBlock for Erc20Balances {
    type Response = (u32, RawBytes, RawBytes, Option<U256>);

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let signature = ERC20::balanceOfCall::SELECTOR;
        let mut call_data = signature.clone().to_vec();
        call_data.extend(vec![0; 12]);
        call_data.extend(request.address()?);
        let block_number = request.ethers_block_number()?;
        let contract = request.ethers_contract()?;
        let balance = source.call2(contract, call_data, block_number).await.ok();
        let balance = balance.map(|x| U256::from_be_slice(x.as_ref()));
        Ok((request.block_number()? as u32, request.contract()?, request.address()?, balance))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Erc20Balances)?;
        let (block, erc20, address, balance) = response;
        columns.n_rows += 1;
        store!(schema, columns, block_number, block);
        store!(schema, columns, erc20, erc20);
        store!(schema, columns, address, address);
        store!(schema, columns, balance, balance);
        Ok(())
    }
}

impl CollectByTransaction for Erc20Balances {
    type Response = ();
}
