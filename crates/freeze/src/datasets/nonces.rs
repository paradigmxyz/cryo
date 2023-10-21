use crate::*;
use ethers::prelude::*;
use polars::prelude::*;

/// columns for balances
#[cryo_to_df::to_df(Datatype::Nonces)]
#[derive(Default)]
pub struct Nonces {
    n_rows: usize,
    block_number: Vec<u32>,
    address: Vec<Vec<u8>>,
    nonce: Vec<u64>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Nonces {
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

type BlockTxAddressOutput = (u32, Option<Vec<u8>>, Vec<u8>, u64);

#[async_trait::async_trait]
impl CollectByBlock for Nonces {
    type Response = BlockTxAddressOutput;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let address = request.address()?;
        let block_number = request.block_number()? as u32;
        let output = source
            .fetcher
            .get_transaction_count(H160::from_slice(&address), block_number.into())
            .await?;
        Ok((block_number, None, address, output.as_u64()))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Nonces)?;
        process_nonce(columns, response, schema)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Nonces {
    type Response = ();
}

fn process_nonce(columns: &mut Nonces, data: BlockTxAddressOutput, schema: &Table) -> R<()> {
    let (block, _tx, address, output) = data;
    columns.n_rows += 1;
    store!(schema, columns, block_number, block);
    store!(schema, columns, address, address);
    store!(schema, columns, nonce, output);
    Ok(())
}
