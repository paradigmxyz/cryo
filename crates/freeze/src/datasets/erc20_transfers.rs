use crate::*;
use ethers::prelude::*;
use polars::prelude::*;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Erc20Transfers)]
#[derive(Default)]
pub struct Erc20Transfers {
    n_rows: u64,
    block_number: Vec<u32>,
    transaction_index: Vec<u32>,
    log_index: Vec<u32>,
    transaction_hash: Vec<Vec<u8>>,
    erc20: Vec<Vec<u8>>,
    from_address: Vec<Vec<u8>>,
    to_address: Vec<Vec<u8>>,
    value: Vec<U256>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Erc20Transfers {
    fn optional_parameters() -> Vec<Dim> {
        vec![Dim::Contract, Dim::Topic0, Dim::Topic1, Dim::Topic2]
    }

    fn use_block_ranges() -> bool {
        true
    }
}

#[async_trait::async_trait]
impl CollectByBlock for Erc20Transfers {
    type Response = Vec<Log>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let topics = [Some(ValueOrArray::Value(Some(*EVENT_ERC20_TRANSFER))), None, None, None];
        let filter = Filter { topics, ..request.ethers_log_filter()? };
        let logs = source.fetcher.get_logs(&filter).await?;
        Ok(logs.into_iter().filter(|x| x.topics.len() == 3 && x.data.len() == 32).collect())
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Erc20Transfers)?;
        process_erc20_transfers(response, columns, schema)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Erc20Transfers {
    type Response = Vec<Log>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let logs = source.fetcher.get_transaction_logs(request.transaction_hash()?).await?;
        Ok(logs.into_iter().filter(is_erc20_transfer).collect())
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Erc20Transfers)?;
        process_erc20_transfers(response, columns, schema)
    }
}

fn is_erc20_transfer(log: &Log) -> bool {
    log.topics.len() == 3 && log.data.len() == 32 && log.topics[0] == *EVENT_ERC20_TRANSFER
}

/// process block into columns
fn process_erc20_transfers(logs: Vec<Log>, columns: &mut Erc20Transfers, schema: &Table) -> R<()> {
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
            store!(schema, columns, value, log.data.to_vec().as_slice().into());
        }
    }
    Ok(())
}
