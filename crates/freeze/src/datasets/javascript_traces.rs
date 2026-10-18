use crate::*;
use polars::prelude::*;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct JavascriptTraces {
    pub(crate) n_rows: u64,
    pub(crate) block_number: Vec<Option<u32>>,
    pub(crate) transaction_index: Vec<Option<u32>>,
    pub(crate) transaction_hash: Vec<Option<RawBytes>>,
    pub(crate) output: Vec<String>,
    pub(crate) chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for JavascriptTraces {
    fn aliases() -> Vec<&'static str> {
        vec!["js_traces"]
    }
}

type BlockTxsTraces = (Option<u32>, Vec<Option<RawBytes>>, Vec<serde_json::Value>);

#[async_trait::async_trait]
impl CollectByBlock for JavascriptTraces {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema =
            query.schemas.get(&Datatype::JavascriptTraces).ok_or(err("schema not provided"))?;
        let include_txs = schema.has_column("transaction_hash");
        let block = request.block_number()? as u32;
        if let Some(js_tracer) = &query.js_tracer {
            source
                .geth_debug_trace_block_javascript_traces(js_tracer.clone(), block, include_txs)
                .await
        } else {
            Err(err("javascript tracer not provided (try --js_tracer <TRACER>)"))
        }
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_javascript_traces(&response, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for JavascriptTraces {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema =
            query.schemas.get(&Datatype::JavascriptTraces).ok_or(err("schema not provided"))?;
        let include_block_number = schema.has_column("block_number");
        let tx = request.transaction_hash()?;
        if let Some(js_tracer) = &query.js_tracer {
            source
                .geth_debug_trace_transaction_javascript_traces(
                    js_tracer.clone(),
                    tx,
                    include_block_number,
                )
                .await
        } else {
            Err(err("javascript tracer not provided (try --js_tracer <TRACER>)"))
        }
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_javascript_traces(&response, columns, &query.schemas)
    }
}

pub(crate) fn process_javascript_traces(
    response: &BlockTxsTraces,
    columns: &mut JavascriptTraces,
    schemas: &Schemas,
) -> R<()> {
    let schema = schemas.get(&Datatype::JavascriptTraces).ok_or(err("schema not provided"))?;
    let (block_number, txs, traces) = response;
    for (index, (value, tx)) in traces.iter().zip(txs).enumerate() {
        columns.n_rows += 1;
        store!(schema, columns, block_number, *block_number);
        store!(schema, columns, transaction_index, Some(index as u32));
        store!(schema, columns, transaction_hash, tx.clone());
        store!(schema, columns, output, value.to_string());
    }
    Ok(())
}
