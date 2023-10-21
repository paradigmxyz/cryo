use crate::*;
use ethers::prelude::*;
use polars::prelude::*;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Logs)]
#[derive(Default)]
pub struct Logs {
    n_rows: u64,
    block_number: Vec<u32>,
    transaction_index: Vec<u32>,
    log_index: Vec<u32>,
    transaction_hash: Vec<Vec<u8>>,
    address: Vec<Vec<u8>>,
    topic0: Vec<Option<Vec<u8>>>,
    topic1: Vec<Option<Vec<u8>>>,
    topic2: Vec<Option<Vec<u8>>>,
    topic3: Vec<Option<Vec<u8>>>,
    data: Vec<Vec<u8>>,
    event_cols: std::collections::HashMap<String, Vec<ethers_core::abi::Token>>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Logs {
    fn aliases() -> Vec<&'static str> {
        vec!["events"]
    }

    fn optional_parameters() -> Vec<Dim> {
        vec![Dim::Contract, Dim::Topic0, Dim::Topic1, Dim::Topic2, Dim::Topic3]
    }

    fn use_block_ranges() -> bool {
        true
    }
}

#[async_trait::async_trait]
impl CollectByBlock for Logs {
    type Response = Vec<Log>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        source.fetcher.get_logs(&request.ethers_log_filter()?).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Logs)?;
        process_logs(response, columns, schema)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Logs {
    type Response = Vec<Log>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        source.fetcher.get_transaction_logs(request.transaction_hash()?).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Logs)?;
        process_logs(response, columns, schema)
    }
}

/// process block into columns
fn process_logs(logs: Vec<Log>, columns: &mut Logs, schema: &Table) -> R<()> {
    for log in logs.iter() {
        if let (Some(bn), Some(tx), Some(ti), Some(li)) =
            (log.block_number, log.transaction_hash, log.transaction_index, log.log_index)
        {
            columns.n_rows += 1;
            store!(schema, columns, block_number, bn.as_u32());
            store!(schema, columns, transaction_index, ti.as_u32());
            store!(schema, columns, log_index, li.as_u32());
            store!(schema, columns, transaction_hash, tx.as_bytes().to_vec());
            store!(schema, columns, address, log.address.as_bytes().to_vec());
            store!(schema, columns, data, log.data.to_vec());

            // topics
            for i in 0..4 {
                let topic = if i < log.topics.len() {
                    Some(log.topics[i].as_bytes().to_vec())
                } else {
                    None
                };
                match i {
                    0 => store!(schema, columns, topic0, topic),
                    1 => store!(schema, columns, topic1, topic),
                    2 => store!(schema, columns, topic2, topic),
                    3 => store!(schema, columns, topic3, topic),
                    _ => {}
                }
            }
        }
    }

    // add decoded event logs
    let decoder = schema.log_decoder.clone();
    if let Some(decoder) = decoder {
        decoder.parse_log_from_event(logs).into_iter().for_each(|(k, v)| {
            columns.event_cols.entry(k).or_default().extend(v);
        });
    }

    Ok(())
}
