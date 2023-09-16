use std::{collections::HashMap, sync::Arc};

use ethers::prelude::*;
use hex::FromHex;

use cryo_freeze::{
    AddressChunk, CallDataChunk, Datatype, Fetcher, MultiQuery, ParseError, RowFilter, SlotChunk,
};

use super::{
    blocks, parse_schemas,
    parse_utils::{hex_string_to_binary, hex_strings_to_binary, parse_binary_arg},
    transactions,
};
use crate::args::Args;

pub(crate) async fn parse_query<P: JsonRpcClient>(
    args: &Args,
    fetcher: Arc<Fetcher<P>>,
) -> Result<MultiQuery, ParseError> {
    // process schemas
    let schemas = parse_schemas(args)?;

    // process chunks
    let chunks = match (&args.blocks, &args.txs) {
        (Some(_), None) => blocks::parse_blocks(args, fetcher).await?,
        (None, Some(txs)) => transactions::parse_transactions(txs)?,
        (None, None) => blocks::get_default_block_chunks(args, fetcher, &schemas).await?,
        (Some(_), Some(_)) => {
            return Err(ParseError::ParseError("specify only one of --blocks or --txs".to_string()))
        }
    };

    // deprecated
    let address = if let Some(contract) = &args.contract {
        parse_address(&Some(contract[0].clone()))
    } else {
        None
    };

    // build row filters
    let call_data_chunks = parse_call_datas(&args.call_data, &args.function, &args.inputs)?;
    let address_chunks = parse_address_chunks(&args.address, "address")?;
    let contract_chunks = parse_address_chunks(&args.contract, "contract_address")?;
    let to_address_chunks = parse_address_chunks(&args.to_address, "to_address")?;
    let slot_chunks = parse_slot_chunks(&args.slots, "slot")?;
    let topics = [
        parse_topic(&args.topic0),
        parse_topic(&args.topic1),
        parse_topic(&args.topic2),
        parse_topic(&args.topic3),
    ];
    let row_filter = RowFilter {
        address,
        topics,
        address_chunks,
        contract_chunks,
        to_address_chunks,
        slot_chunks,
        call_data_chunks,
    };
    let mut row_filters: HashMap<Datatype, RowFilter> = HashMap::new();
    for datatype in schemas.keys() {
        let row_filter = row_filter.apply_arg_aliases(datatype.dataset().arg_aliases());
        row_filters.insert(*datatype, row_filter);
    }

    let query = MultiQuery { schemas, chunks, row_filters };
    Ok(query)
}

fn parse_call_datas(
    call_datas: &Option<Vec<String>>,
    function: &Option<Vec<String>>,
    inputs: &Option<Vec<String>>,
) -> Result<Option<Vec<CallDataChunk>>, ParseError> {
    let call_datas = match (call_datas, function, inputs) {
        (None, None, None) => return Ok(None),
        (Some(call_data), None, None) => hex_strings_to_binary(call_data)?,
        (None, Some(function), None) => hex_strings_to_binary(function)?,
        (None, Some(function), Some(inputs)) => {
            let mut call_datas = Vec::new();
            for f in function.iter() {
                for i in inputs.iter() {
                    let mut call_data = hex_string_to_binary(f)?.clone();
                    call_data.extend(hex_string_to_binary(i)?);
                    call_datas.push(call_data);
                }
            }
            call_datas
        }
        (None, None, Some(_)) => {
            let message = "must specify function if specifying inputs";
            return Err(ParseError::ParseError(message.to_string()))
        }
        (Some(_), Some(_), None) => {
            let message = "cannot specify both call_data and function";
            return Err(ParseError::ParseError(message.to_string()))
        }
        (Some(_), None, Some(_)) => {
            let message = "cannot specify both call_data and inputs";
            return Err(ParseError::ParseError(message.to_string()))
        }
        (Some(_), Some(_), Some(_)) => {
            let message = "cannot specify both call_data and function";
            return Err(ParseError::ParseError(message.to_string()))
        }
    };
    Ok(Some(vec![CallDataChunk::Values(call_datas)]))
}

pub(crate) fn parse_address_chunks(
    address: &Option<Vec<String>>,
    default_column: &str,
) -> Result<Option<Vec<AddressChunk>>, ParseError> {
    if let Some(address) = address {
        let chunks = parse_binary_arg(address, default_column)?
            .values()
            .map(|a| AddressChunk::Values(a.clone()))
            .collect();
        Ok(Some(chunks))
    } else {
        Ok(None)
    }
}

pub(crate) fn parse_slot_chunks(
    slots: &Option<Vec<String>>,
    default_column: &str,
) -> Result<Option<Vec<SlotChunk>>, ParseError> {
    if let Some(values) = slots {
        let chunks = parse_binary_arg(values, default_column)?
            .values()
            .map(|a| SlotChunk::Values(a.clone()))
            .collect();
        Ok(Some(chunks))
    } else {
        Ok(None)
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
