use crate::*;
use alloy::{
    dyn_abi::{DynSolValue, EventExt},
    rpc::types::Log,
};
use polars::prelude::*;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct Logs {
    n_rows: u64,
    block_number: Vec<u32>,
    block_hash: Vec<Option<RawBytes>>,
    transaction_index: Vec<u32>,
    log_index: Vec<u32>,
    transaction_hash: Vec<RawBytes>,
    address: Vec<RawBytes>,
    topic0: Vec<Option<RawBytes>>,
    topic1: Vec<Option<RawBytes>>,
    topic2: Vec<Option<RawBytes>>,
    topic3: Vec<Option<RawBytes>>,
    data: Vec<RawBytes>,
    n_data_bytes: Vec<u32>,
    #[to_df(flatten = "extract_event_cols")]
    event_cols: indexmap::IndexMap<String, Vec<DynSolValue>>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Logs {
    fn aliases() -> Vec<&'static str> {
        vec!["events"]
    }

    fn default_columns() -> Option<Vec<&'static str>> {
        Some(vec![
            "block_number",
            // "block_hash",
            "transaction_index",
            "log_index",
            "transaction_hash",
            "address",
            "topic0",
            "topic1",
            "topic2",
            "topic3",
            "data",
            "n_data_bytes",
            // "event_cols",
            "chain_id",
        ])
    }

    fn optional_parameters() -> Vec<Dim> {
        vec![Dim::Address, Dim::Topic0, Dim::Topic1, Dim::Topic2, Dim::Topic3]
    }

    fn use_block_ranges() -> bool {
        true
    }

    fn arg_aliases() -> Option<std::collections::HashMap<Dim, Dim>> {
        Some([(Dim::Contract, Dim::Address)].into_iter().collect())
    }
}

#[async_trait::async_trait]
impl CollectByBlock for Logs {
    type Response = Vec<Log>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        source.get_logs(&request.ethers_log_filter()?).await
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
        source.get_transaction_logs(request.transaction_hash()?).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Logs)?;
        process_logs(response, columns, schema)
    }
}

/// process block into columns
fn process_logs(logs: Vec<Log>, columns: &mut Logs, schema: &Table) -> R<()> {
    // let decode_keys = match &schema.log_decoder {
    //     None => None,
    //     Some(decoder) => {
    //         let keys = decoder
    //             .event
    //             .inputs
    //             .clone()
    //             .into_iter()
    //             .map(|i| i.name)
    //             .collect::<std::collections::HashSet<String>>();
    //         Some(keys)
    //     }
    // };
    let (indexed_keys, body_keys) = match &schema.log_decoder {
        None => (None, None),
        Some(decoder) => {
            let indexed = decoder.indexed_names();
            let body = decoder.body_names();
            (Some(indexed), Some(body))
        }
    };

    for log in logs.iter() {
        if let (Some(bn), Some(tx), Some(ti), Some(li)) =
            (log.block_number, log.transaction_hash, log.transaction_index, log.log_index)
        {
            // decode event
            if let (Some(decoder), Some(indexed_keys), Some(body_keys)) =
                (&schema.log_decoder, &indexed_keys, &body_keys)
            {
                match decoder.event.decode_log(&log.inner.data) {
                    Ok(log) => {
                        // for param in log.indexed {
                        //     if decode_keys.contains(param.name.as_str()) {
                        //         columns.event_cols.entry(param.name).or_default().push(param.
                        // value);     }
                        // }
                        for (indexed_param, name) in log.indexed.into_iter().zip(indexed_keys) {
                            if schema.has_column(&format!("event__{name}")) {
                                columns
                                    .event_cols
                                    .entry(name.clone())
                                    .or_default()
                                    .push(indexed_param);
                            }
                        }
                        for (body_param, name) in log.body.into_iter().zip(body_keys) {
                            if schema.has_column(&format!("event__{name}")) {
                                columns
                                    .event_cols
                                    .entry(name.clone())
                                    .or_default()
                                    .push(body_param);
                            }
                        }
                    }
                    Err(_) => continue,
                }
            };

            columns.n_rows += 1;
            store!(schema, columns, block_number, bn as u32);
            store!(schema, columns, block_hash, log.block_hash.map(|bh| bh.to_vec()));
            store!(schema, columns, transaction_index, ti as u32);
            store!(schema, columns, log_index, li as u32);
            store!(schema, columns, transaction_hash, tx.to_vec());
            store!(schema, columns, address, log.address().to_vec());
            store!(schema, columns, data, log.data().data.to_vec());
            store!(schema, columns, n_data_bytes, log.data().data.len() as u32);

            // topics
            for i in 0..4 {
                let topic =
                    if i < log.topics().len() { Some(log.topics()[i].to_vec()) } else { None };
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

    Ok(())
}

fn extract_event_cols(
    cols: &mut Vec<Column>,
    _field_name: &str,
    values: indexmap::IndexMap<String, Vec<DynSolValue>>,
    chunk_len: usize,
    schema: &Table,
) -> Result<(), CollectError> {
    if let Some(decoder) = &schema.log_decoder {
        // Write columns even if there are no values decoded - indicates empty dataframe
        if values.is_empty() {
            for name in decoder.field_names() {
                let name = format!("event__{name}");
                let name = PlSmallStr::from_string(name);
                if let Some(col_type) = schema.column_type(&name) {
                    cols.extend(col_type.create_empty_columns(&name, &schema.config));
                }
            }
        } else {
            for (name, data) in values {
                let name = format!("event__{name}");
                if let Some(col_type) = schema.column_type(&name) {
                    let series_vec = col_type.create_column_from_values(
                        name,
                        data,
                        chunk_len,
                        &schema.config,
                    )?;
                    cols.extend(series_vec);
                }
            }
        }
    }
    Ok(())
}
