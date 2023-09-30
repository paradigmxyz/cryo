use super::traces;
use crate::{
    conversions::ToVecHex, dataframes::SortableDataFrame, with_series, with_series_binary,
    CollectByBlock, CollectError, ColumnData, ColumnType, Datatype, Params,
    Schemas, Source, Table, TraceCalls, store,
};
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

type Result<T> = ::core::result::Result<T, CollectError>;
type ContractCallDataTraces = (Vec<u8>, Vec<u8>, Vec<TransactionTrace>);

#[async_trait::async_trait]
impl CollectByBlock for TraceCalls {
    type Response = ContractCallDataTraces;

    type Columns = TraceCallsColumns;

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let transaction = TransactionRequest {
            to: Some(request.ethers_contract().into()),
            data: Some(request.call_data().into()),
            ..Default::default()
        };
        let trace_type = vec![TraceType::Trace];
        let output = source.fetcher.trace_call(transaction, trace_type, Some(request.ethers_block_number())).await?;
        if let Some(traces) = output.trace {
            Ok((request.contract(), request.call_data(), traces))
        } else {
            Err(CollectError::CollectError("traces missing".to_string()))
        }
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::TraceCalls).expect("schema not provided");
        process_trace_call_extras(&response, &mut columns.extra_columns, schema);
        // let (_, _, traces) = response;
        // traces::process_traces(traces, &mut columns.trace_columns, schema);
        todo!()
    }
}

/// columns for trace calls
#[derive(Default)]
pub struct TraceCallsColumns {
    trace_columns: traces::TraceColumns,
    extra_columns: TraceCallExtraColumns,
}

/// extra columns for trace calls
#[cryo_to_df::to_df(Datatype::TraceCalls)]
#[derive(Default)]
pub struct TraceCallExtraColumns {
    n_rows: u64,
    tx_to_address: Vec<Vec<u8>>,
    tx_call_data: Vec<Vec<u8>>,
}

impl ColumnData for TraceCallsColumns {
    fn datatypes() -> Vec<Datatype> {
        vec![Datatype::TraceCalls]
    }

    fn create_df(self, schemas: &HashMap<Datatype, Table>, chain_id: u64) -> Result<DataFrame> {
        let df = self.trace_columns.create_df(schemas, chain_id)?;
        let extras = self.extra_columns.create_df(schemas, chain_id)?;
        df.hstack(extras.get_columns())
            .map_err(|_| CollectError::CollectError("cannot concatentate dfs".to_string()))
    }
}

fn process_trace_call_extras(response: &ContractCallDataTraces, columns: &mut TraceCallExtraColumns, schema: &Table) {
    let (contract, call_data, traces) = response;
    for _ in traces.iter() {
        columns.n_rows += 1;
        store!(schema, columns, tx_to_address, contract.clone());
        store!(schema, columns, tx_call_data, call_data.clone());
    }
}

