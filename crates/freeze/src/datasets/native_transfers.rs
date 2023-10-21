use crate::*;
use ethers::prelude::*;
use polars::prelude::*;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::NativeTransfers)]
#[derive(Default)]
pub struct NativeTransfers {
    n_rows: u64,
    block_number: Vec<u32>,
    transaction_index: Vec<Option<u32>>,
    transfer_index: Vec<u32>,
    transaction_hash: Vec<Option<Vec<u8>>>,
    from_address: Vec<Vec<u8>>,
    to_address: Vec<Vec<u8>>,
    value: Vec<U256>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for NativeTransfers {}

#[async_trait::async_trait]
impl CollectByBlock for NativeTransfers {
    type Response = Vec<Trace>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        source.fetcher.trace_block(request.block_number()?.into()).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let traces =
            if query.exclude_failed { traces::filter_failed_traces(response) } else { response };
        process_native_transfers(&traces, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for NativeTransfers {
    type Response = Vec<Trace>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        source.fetcher.trace_transaction(request.ethers_transaction_hash()?).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let traces =
            if query.exclude_failed { traces::filter_failed_traces(response) } else { response };
        process_native_transfers(&traces, columns, &query.schemas)
    }
}

/// process block into columns
pub(crate) fn process_native_transfers(
    traces: &[Trace],
    columns: &mut NativeTransfers,
    schemas: &Schemas,
) -> R<()> {
    let schema = schemas.get(&Datatype::NativeTransfers).ok_or(err("schema not provided"))?;
    for (transfer_index, trace) in traces.iter().enumerate() {
        columns.n_rows += 1;
        store!(schema, columns, block_number, trace.block_number as u32);
        store!(schema, columns, transaction_index, trace.transaction_position.map(|x| x as u32));
        store!(schema, columns, transfer_index, transfer_index as u32);
        store!(
            schema,
            columns,
            transaction_hash,
            trace.transaction_hash.map(|x| x.as_bytes().to_vec())
        );

        match &trace.action {
            Action::Call(action) => {
                store!(schema, columns, from_address, action.from.as_bytes().to_vec());
                store!(schema, columns, to_address, action.to.as_bytes().to_vec());
                store!(schema, columns, value, action.value);
            }
            Action::Create(action) => {
                store!(schema, columns, from_address, action.from.as_bytes().to_vec());
                match &trace.result.as_ref() {
                    Some(Res::Create(res)) => {
                        store!(schema, columns, to_address, res.address.0.into())
                    }
                    _ => store!(schema, columns, to_address, vec![0; 32]),
                }
                store!(schema, columns, value, action.value);
            }
            Action::Suicide(action) => {
                store!(schema, columns, from_address, action.address.as_bytes().to_vec());
                store!(schema, columns, to_address, action.refund_address.as_bytes().to_vec());
                store!(schema, columns, value, action.balance);
            }
            Action::Reward(action) => {
                store!(schema, columns, from_address, vec![0; 20]);
                store!(schema, columns, to_address, action.author.as_bytes().to_vec());
                store!(schema, columns, value, action.value);
            }
        }
    }
    Ok(())
}
