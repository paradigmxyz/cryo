use crate::*;
use ethers::prelude::*;
use polars::prelude::*;

/// columns for balances
#[cryo_to_df::to_df(Datatype::Balances)]
#[derive(Default)]
pub struct Balances {
    n_rows: usize,
    block_number: Vec<u32>,
    address: Vec<Vec<u8>>,
    balance: Vec<U256>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Balances {
    fn default_sort() -> Option<Vec<&'static str>> {
        Some(vec!["block_number", "address"])
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Address]
    }

    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }
}

type BlockTxAddressOutput = (u32, Option<Vec<u8>>, Vec<u8>, U256);

#[async_trait::async_trait]
impl CollectByBlock for Balances {
    type Response = BlockTxAddressOutput;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let address = request.address()?;
        let block_number = request.block_number()? as u32;
        let balance =
            source.fetcher.get_balance(H160::from_slice(&address), block_number.into()).await?;
        Ok((block_number, None, address, balance))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get(&Datatype::Balances).ok_or(err("schema not provided"))?;
        process_balance(columns, response, schema)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Balances {
    type Response = ();
}

fn process_balance(columns: &mut Balances, data: BlockTxAddressOutput, schema: &Table) -> R<()> {
    let (block, _tx, address, balance) = data;
    columns.n_rows += 1;
    store!(schema, columns, block_number, block);
    store!(schema, columns, address, address);
    store!(schema, columns, balance, balance);
    Ok(())
}
