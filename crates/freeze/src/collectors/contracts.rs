use super::traces;
use crate::{
    conversions::ToVecHex, dataframes::SortableDataFrame, store, with_series, with_series_binary,
    CollectByBlock, CollectByTransaction, CollectError, ColumnData, ColumnType, Contracts,
    Datatype, RpcParams, Source, Table,
};
use ethers::prelude::*;
use ethers_core::utils::keccak256;
use polars::prelude::*;
use std::collections::HashMap;

#[async_trait::async_trait]
impl CollectByBlock for Contracts {
    type BlockResponse = Vec<Trace>;

    type BlockColumns = TraceColumns;

//     async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
//         source.fetcher.trace_block(request.ethers_block_number()).await
//     }

//     fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
//         let schema = schemas.get(&Datatype::Traces).expect("schema not provided");
//         process_contracts(response, columns, schema)
//     }

    async fn extract_by_block(
        request: RpcParams,
        source: Source,
        _schemas: HashMap<Datatype, Table>,
    ) -> Result<Self::BlockResponse, CollectError> {
        source.fetcher.trace_block(request.ethers_block_number()).await
    }

    fn transform_by_block(
        response: Self::BlockResponse,
        columns: &mut Self::BlockColumns,
        schemas: &HashMap<Datatype, Table>,
    ) {
        let schema = schemas.get(&Datatype::Traces).expect("schema not provided");
        process_contracts(response, columns, schema)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Contracts {
    type TransactionResponse = Vec<Trace>;

    type TransactionColumns = TraceColumns;

    async fn extract_by_transaction(
        request: RpcParams,
        source: Source,
        _schemas: HashMap<Datatype, Table>,
    ) -> Result<Self::TransactionResponse, CollectError> {
        source.fetcher.trace_transaction(request.ethers_transaction_hash()).await
    }

    fn transform_by_transaction(
        response: Self::TransactionResponse,
        columns: &mut Self::TransactionColumns,
        schemas: &HashMap<Datatype, Table>,
    ) {
        let schema = schemas.get(&Datatype::Traces).expect("schema not provided");
        process_contracts(response, columns, schema)
    }
}

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Traces)]
#[derive(Default)]
pub struct TraceColumns {
    n_rows: u64,
    block_number: Vec<u32>,
    create_index: Vec<u32>,
    transaction_hash: Vec<Option<Vec<u8>>>,
    contract_address: Vec<Vec<u8>>,
    deployer: Vec<Vec<u8>>,
    factory: Vec<Vec<u8>>,
    init_code: Vec<Vec<u8>>,
    code: Vec<Vec<u8>>,
    init_code_hash: Vec<Vec<u8>>,
    code_hash: Vec<Vec<u8>>,
    chain_id: Vec<u64>,
}

/// process block into columns
pub fn process_contracts(traces: Vec<Trace>, columns: &mut TraceColumns, schema: &Table) {
    let traces = traces::filter_failed_traces(traces);
    let mut deployer = H160([0; 20]);
    let mut create_index = 0;
    for trace in traces.iter() {
        if trace.trace_address.is_empty() {
            deployer = match &trace.action {
                Action::Call(call) => call.from,
                Action::Create(create) => create.from,
                Action::Suicide(suicide) => suicide.refund_address,
                Action::Reward(reward) => reward.author,
            };
        };

        if let (Action::Create(create), Some(Res::Create(result))) = (&trace.action, &trace.result)
        {
            columns.n_rows += 1;
            store!(schema, columns, block_number, trace.block_number as u32);
            store!(schema, columns, create_index, create_index);
            create_index += 1;
            let tx = trace.transaction_hash;
            store!(schema, columns, transaction_hash, tx.map(|x| x.as_bytes().to_vec()));
            store!(schema, columns, contract_address, result.address.as_bytes().into());
            store!(schema, columns, deployer, deployer.as_bytes().into());
            store!(schema, columns, factory, create.from.as_bytes().into());
            store!(schema, columns, init_code, create.init.to_vec());
            store!(schema, columns, code, result.code.to_vec());
            store!(schema, columns, code, keccak256(create.init.clone()).into());
            store!(schema, columns, code, keccak256(result.code.clone()).into());
        }
    }
}
