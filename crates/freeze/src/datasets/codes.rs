use crate::*;
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for balances
#[cryo_to_df::to_df(Datatype::Codes)]
#[derive(Default)]
pub struct Codes {
    n_rows: usize,
    block_number: Vec<u32>,
    address: Vec<Vec<u8>>,
    code: Vec<Vec<u8>>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Codes {
    fn name() -> &'static str {
        "codes"
    }

    fn default_sort() -> Vec<String> {
        vec!["block_number".to_string(), "address".to_string()]
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Address]
    }

    fn arg_aliases() -> Option<HashMap<Dim, Dim>> {
        Some([(Dim::Contract, Dim::Address)].into_iter().collect())
    }

    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;
type BlockTxAddressOutput = (u32, Option<Vec<u8>>, Vec<u8>, Vec<u8>);

#[async_trait::async_trait]
impl CollectByBlock for Codes {
    type Response = BlockTxAddressOutput;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        _schemas: Schemas,
    ) -> Result<Self::Response> {
        let address = request.address()?;
        let block_number = request.block_number()? as u32;
        let output =
            source.fetcher.get_code(H160::from_slice(&address), block_number.into()).await?;
        Ok((block_number, None, address, output.to_vec()))
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        let schema = schemas.get(&Datatype::Codes).ok_or(err("schema not provided"))?;
        process_nonce(columns, response, schema)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Codes {
    type Response = ();
}

fn process_nonce(columns: &mut Codes, data: BlockTxAddressOutput, schema: &Table) -> Result<()> {
    let (block, _tx, address, output) = data;
    columns.n_rows += 1;
    store!(schema, columns, block_number, block);
    store!(schema, columns, address, address);
    store!(schema, columns, code, output);
    Ok(())
}
