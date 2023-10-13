use crate::*;
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Erc721Transfers)]
#[derive(Default)]
pub struct Erc721Transfers {
    n_rows: u64,
    block_number: Vec<u32>,
    transaction_index: Vec<u32>,
    log_index: Vec<u32>,
    transaction_hash: Vec<Vec<u8>>,
    erc20: Vec<Vec<u8>>,
    from_address: Vec<Vec<u8>>,
    to_address: Vec<Vec<u8>>,
    token_id: Vec<U256>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Erc721Transfers {
    fn name() -> &'static str {
        "erc721_transfers"
    }

    fn default_sort() -> Vec<String> {
        vec!["block_number".to_string(), "log_index".to_string()]
    }

    fn optional_parameters() -> Vec<Dim> {
        vec![Dim::Contract]
    }

    fn use_block_ranges() -> bool {
        true
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;

#[async_trait::async_trait]
impl CollectByBlock for Erc721Transfers {
    type Response = Vec<Log>;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        _schemas: Schemas,
    ) -> Result<Self::Response> {
        let topics = [Some(ValueOrArray::Value(Some(*EVENT_ERC721_TRANSFER))), None, None, None];
        let filter = Filter { topics, ..request.ethers_log_filter()? };
        let logs = source.fetcher.get_logs(&filter).await?;
        Ok(logs.into_iter().filter(|x| x.topics.len() == 4 && x.data.len() == 0).collect())
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        let schema = schemas.get(&Datatype::Erc721Transfers).ok_or(err("schema not provided"))?;
        process_erc721_transfers(response, columns, schema)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Erc721Transfers {
    type Response = Vec<Log>;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        _schemas: Schemas,
    ) -> Result<Self::Response> {
        let logs = source.fetcher.get_transaction_logs(request.transaction_hash()?).await?;
        Ok(logs.into_iter().filter(is_erc721_transfer).collect())
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        let schema = schemas.get(&Datatype::Erc721Transfers).ok_or(err("schema not provided"))?;
        process_erc721_transfers(response, columns, schema)
    }
}

fn is_erc721_transfer(log: &Log) -> bool {
    log.topics.len() == 4 && log.data.len() == 0 && log.topics[0] == *EVENT_ERC721_TRANSFER
}

/// process block into columns
fn process_erc721_transfers(
    logs: Vec<Log>,
    columns: &mut Erc721Transfers,
    schema: &Table,
) -> Result<()> {
    for log in logs.iter() {
        if let (Some(bn), Some(tx), Some(ti), Some(li)) =
            (log.block_number, log.transaction_hash, log.transaction_index, log.log_index)
        {
            columns.n_rows += 1;
            store!(schema, columns, block_number, bn.as_u32());
            store!(schema, columns, transaction_index, ti.as_u32());
            store!(schema, columns, log_index, li.as_u32());
            store!(schema, columns, transaction_hash, tx.as_bytes().to_vec());
            store!(schema, columns, erc20, log.address.as_bytes().to_vec());
            store!(schema, columns, from_address, log.topics[1].as_bytes()[12..].to_vec());
            store!(schema, columns, to_address, log.topics[2].as_bytes()[12..].to_vec());
            store!(schema, columns, token_id, log.topics[3].as_bytes().into());
        }
    }
    Ok(())
}
