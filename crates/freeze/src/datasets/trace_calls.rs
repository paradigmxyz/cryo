use super::traces;
use crate::{
    conversions::ToVecHex, dataframes::SortableDataFrame, store, with_series, with_series_binary,
    CollectByBlock, CollectError, ColumnData, ColumnType, Dataset, Datatype, Params, Schemas,
    Source, Table, TraceCalls,
};
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for trace calls
#[derive(Default)]
pub struct TraceCallsColumns {
    trace_columns: traces::TraceColumns,
    extra_columns: TraceCallExtraColumns,
}

/// extra columns for trace calls
#[cryo_to_df::to_df(Datatype::TraceCalls)]
#[derive(Default)]
struct TraceCallExtraColumns {
    n_rows: u64,
    tx_to_address: Vec<Vec<u8>>,
    tx_call_data: Vec<Vec<u8>>,
}

#[async_trait::async_trait]
impl Dataset for TraceCalls {
    fn datatype(&self) -> Datatype {
        Datatype::TraceCalls
    }

    fn name(&self) -> &'static str {
        "trace_calls"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        todo!()
        // let mut types = Traces.column_types();
        // types.insert("tx_to_address", ColumnType::Binary);
        // types.insert("tx_call_data", ColumnType::Binary);
        // types
    }

    fn default_columns(&self) -> Vec<&'static str> {
        todo!()
    }

    fn default_sort(&self) -> Vec<String> {
        todo!()
    }
}

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
        let output = source
            .fetcher
            .trace_call(transaction, trace_type, Some(request.ethers_block_number()))
            .await?;
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

fn process_trace_call_extras(
    response: &ContractCallDataTraces,
    columns: &mut TraceCallExtraColumns,
    schema: &Table,
) {
    let (contract, call_data, traces) = response;
    for _ in traces.iter() {
        columns.n_rows += 1;
        store!(schema, columns, tx_to_address, contract.clone());
        store!(schema, columns, tx_call_data, call_data.clone());
    }
}
