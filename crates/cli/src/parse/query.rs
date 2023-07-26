use std::{collections::HashMap, sync::Arc};

use ethers::prelude::*;
use hex::FromHex;

use cryo_freeze::{ColumnEncoding, Datatype, FileFormat, MultiQuery, ParseError, RowFilter, Table};

use super::{blocks, file_output};
use crate::args::Args;

pub(crate) async fn parse_query(
    args: &Args,
    provider: Arc<Provider<Http>>,
) -> Result<MultiQuery, ParseError> {
    let chunks = blocks::parse_blocks(args, provider).await?;

    // process schemas
    let schemas = parse_schemas(args)?;

    // build row filters
    let contract = parse_address(&args.contract);
    let topics = [
        parse_topic(&args.topic0),
        parse_topic(&args.topic1),
        parse_topic(&args.topic2),
        parse_topic(&args.topic3),
    ];
    let row_filter = RowFilter { address: contract, topics };
    let mut row_filters: HashMap<Datatype, RowFilter> = HashMap::new();
    row_filters.insert(Datatype::Logs, row_filter);

    let query = MultiQuery { schemas, chunks, row_filters };
    Ok(query)
}

fn parse_datatypes(raw_inputs: &Vec<String>) -> Result<Vec<Datatype>, ParseError> {
    let mut datatypes = Vec::new();

    for raw_input in raw_inputs {
        match raw_input.as_str() {
            "state_diffs" => {
                datatypes.push(Datatype::BalanceDiffs);
                datatypes.push(Datatype::CodeDiffs);
                datatypes.push(Datatype::NonceDiffs);
                datatypes.push(Datatype::StorageDiffs);
            }
            datatype => {
                let datatype = match datatype {
                    "balance_diffs" => Datatype::BalanceDiffs,
                    "blocks" => Datatype::Blocks,
                    "code_diffs" => Datatype::CodeDiffs,
                    "logs" => Datatype::Logs,
                    "events" => Datatype::Logs,
                    "nonce_diffs" => Datatype::NonceDiffs,
                    "storage_diffs" => Datatype::StorageDiffs,
                    "transactions" => Datatype::Transactions,
                    "txs" => Datatype::Transactions,
                    "traces" => Datatype::Traces,
                    "vm_traces" => Datatype::VmTraces,
                    "opcode_traces" => Datatype::VmTraces,
                    _ => {
                        return Err(ParseError::ParseError(format!("invalid datatype {}", datatype)))
                    }
                };
                datatypes.push(datatype)
            }
        }
    }
    Ok(datatypes)
}

fn parse_schemas(args: &Args) -> Result<HashMap<Datatype, Table>, ParseError> {
    let datatypes = parse_datatypes(&args.datatype)?;
    let output_format = file_output::parse_output_format(args)?;
    let binary_column_format = match args.hex | (output_format != FileFormat::Parquet) {
        true => ColumnEncoding::Hex,
        false => ColumnEncoding::Binary,
    };

    let sort = parse_sort(&args.sort, &datatypes)?;
    let schemas: Result<HashMap<Datatype, Table>, ParseError> = datatypes
        .iter()
        .map(|datatype| {
            datatype
                .table_schema(
                    &binary_column_format,
                    &args.include_columns,
                    &args.exclude_columns,
                    &args.columns,
                    sort[datatype].clone(),
                )
                .map(|schema| (*datatype, schema))
                .map_err(|_e| {
                    ParseError::ParseError(format!(
                        "Failed to get schema for datatype: {:?}",
                        datatype
                    ))
                })
        })
        .collect();
    schemas
}

fn parse_sort(
    raw_sort: &Option<Vec<String>>,
    datatypes: &Vec<Datatype>,
) -> Result<HashMap<Datatype, Option<Vec<String>>>, ParseError> {
    match raw_sort {
        None => Ok(HashMap::from_iter(
            datatypes.iter().map(|datatype| (*datatype, Some(datatype.dataset().default_sort()))),
        )),
        Some(raw_sort) => {
            if (raw_sort.len() == 1) && (raw_sort[0] == "none") {
                Ok(HashMap::from_iter(datatypes.iter().map(|datatype| (*datatype, None))))
            } else if raw_sort.is_empty() {
                Err(ParseError::ParseError(
                    "must specify columns to sort by, use `none` to disable sorting".to_string(),
                ))
            } else if datatypes.len() > 1 {
                Err(ParseError::ParseError(
                    "custom sort not supported for multiple datasets".to_string(),
                ))
            } else {
                match datatypes.iter().next() {
                    Some(datatype) => Ok(HashMap::from_iter([(*datatype, Some(raw_sort.clone()))])),
                    None => Err(ParseError::ParseError("schemas map is empty".to_string())),
                }
            }
        }
    }
}

fn parse_address(input: &Option<String>) -> Option<ValueOrArray<H160>> {
    input.as_ref().and_then(|data| {
        <[u8; 20]>::from_hex(data.as_str().chars().skip(2).collect::<String>().as_str())
            .ok()
            .map(H160)
            .map(ValueOrArray::Value)
    })
}

fn parse_topic(input: &Option<String>) -> Option<ValueOrArray<Option<H256>>> {
    let value = input.as_ref().and_then(|data| {
        <[u8; 32]>::from_hex(data.as_str().chars().skip(2).collect::<String>().as_str())
            .ok()
            .map(H256)
    });

    value.map(|inner| ValueOrArray::Value(Some(inner)))
}
