use super::{
    blocks,
    parse_utils::{hex_string_to_binary, hex_strings_to_binary, parse_binary_arg},
};
use crate::args::Args;
use cryo_freeze::{
    AddressChunk, CallDataChunk, Datatype, Dim, Fetcher, ParseError, Partition, PartitionLabels,
    SlotChunk, Table, TimeDimension, TopicChunk, TransactionChunk,
};
use ethers::prelude::*;
use std::{collections::HashMap, str::FromStr, sync::Arc};

type ChunkLabels = Vec<Option<String>>;

pub(crate) async fn parse_partitions<P: JsonRpcClient>(
    args: &Args,
    fetcher: Arc<Fetcher<P>>,
    schemas: &HashMap<Datatype, Table>,
) -> Result<(Vec<Partition>, Vec<Dim>, TimeDimension), ParseError> {
    // TODO: if wanting to chunk these non-block dimensions, do it in parse_binary_arg()

    // parse chunk data
    let (block_number_labels, block_numbers) = blocks::parse_blocks(args, fetcher.clone()).await?;
    let (transaction_hash_labels, transactions) =
        parse_transaction_chunks(&args.txs, "transaction_hash")?;
    let call_datas = parse_call_datas(&args.call_data, &args.function, &args.inputs)?;
    let call_data_labels = None;
    let (address_labels, addresses) = parse_address_chunks(&args.address, "address")?;
    let (contract_labels, contracts) = parse_address_chunks(&args.contract, "contract_address")?;
    let (to_address_labels, to_addresses) = parse_address_chunks(&args.to_address, "to_address")?;
    let (slot_labels, slots) = parse_slot_chunks(&args.slot, "slot")?;
    let (topic0_labels, topic0s) = parse_topic(&args.topic0, "topic0")?;
    let (topic1_labels, topic1s) = parse_topic(&args.topic1, "topic1")?;
    let (topic2_labels, topic2s) = parse_topic(&args.topic2, "topic2")?;
    let (topic3_labels, topic3s) = parse_topic(&args.topic3, "topic3")?;

    // set default blocks
    let block_numbers = if block_numbers.is_none() && transactions.is_none() {
        Some(blocks::get_default_block_chunks(args, fetcher, schemas).await?)
    } else {
        block_numbers
    };

    // aggregate chunk data
    let chunk = Partition {
        label: None,
        block_numbers,
        transactions,
        addresses,
        contracts,
        to_addresses,
        slots,
        call_datas,
        topic0s,
        topic1s,
        topic2s,
        topic3s,
    };
    let labels = PartitionLabels {
        block_number_labels,
        transaction_hash_labels,
        call_data_labels,
        address_labels,
        contract_labels,
        to_address_labels,
        slot_labels,
        topic0_labels,
        topic1_labels,
        topic2_labels,
        topic3_labels,
    };
    let time_dimension = parse_time_dimension(&chunk);

    let partition_by = match args.partition_by.clone() {
        Some(dim_names) => {
            let dims: Result<Vec<_>, _> =
                dim_names.into_iter().map(|x| Dim::from_str(&x)).collect();
            dims?
        }
        None => {
            let multichunk_dims: Vec<Dim> = Dim::all_dims()
                .iter()
                .filter(|dim| labels.dim_labeled(dim) && chunk.n_chunks(dim) > 1)
                .cloned()
                .collect();
            if args.txs.is_some() {
                vec![Dim::TransactionHash]
            } else if multichunk_dims.is_empty() {
                vec![Dim::BlockNumber]
            } else {
                multichunk_dims
            }
        }
    };
    let partitions = chunk
        .partition_with_labels(labels, partition_by.clone())
        .map_err(|e| ParseError::ParseError(format!("could not partition labels ({})", e)));
    Ok((partitions?, partition_by, time_dimension))
}

fn parse_time_dimension(partition: &Partition) -> TimeDimension {
    if partition.transactions.is_some() {
        TimeDimension::Transactions
    } else {
        TimeDimension::Blocks
    }
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

pub(crate) fn parse_transaction_chunks(
    input: &Option<Vec<String>>,
    default_column: &str,
) -> Result<(Option<ChunkLabels>, Option<Vec<TransactionChunk>>), ParseError> {
    if let Some(input) = input {
        let parsed = parse_binary_arg(input, default_column)?;
        let labels: Vec<Option<String>> = parsed.keys().map(|x| x.clone().to_label()).collect();
        let chunks = parsed.values().map(|a| TransactionChunk::Values(a.clone())).collect();
        Ok((Some(labels), Some(chunks)))
    } else {
        Ok((None, None))
    }
}

pub(crate) fn parse_address_chunks(
    input: &Option<Vec<String>>,
    default_column: &str,
) -> Result<(Option<ChunkLabels>, Option<Vec<AddressChunk>>), ParseError> {
    if let Some(input) = input {
        let parsed = parse_binary_arg(input, default_column)?;
        let labels: Vec<Option<String>> = parsed.keys().map(|x| x.clone().to_label()).collect();
        let chunks = parsed.values().map(|a| AddressChunk::Values(a.clone())).collect();
        Ok((Some(labels), Some(chunks)))
    } else {
        Ok((None, None))
    }
}

pub(crate) fn parse_slot_chunks(
    input: &Option<Vec<String>>,
    default_column: &str,
) -> Result<(Option<ChunkLabels>, Option<Vec<SlotChunk>>), ParseError> {
    if let Some(input) = input {
        let parsed = parse_binary_arg(input, default_column)?;
        let labels: Vec<Option<String>> = parsed.keys().map(|x| x.clone().to_label()).collect();
        let chunks = parsed.values().map(|a| SlotChunk::Values(a.clone())).collect();
        Ok((Some(labels), Some(chunks)))
    } else {
        Ok((None, None))
    }
}

fn parse_topic(
    input: &Option<Vec<String>>,
    default_column: &str,
) -> Result<(Option<ChunkLabels>, Option<Vec<TopicChunk>>), ParseError> {
    if let Some(input) = input {
        let parsed = parse_binary_arg(input, default_column)?;
        let labels: Vec<Option<String>> = parsed.keys().map(|x| x.clone().to_label()).collect();
        let chunks = parsed.values().map(|a| TopicChunk::Values(a.clone())).collect();
        Ok((Some(labels), Some(chunks)))
    } else {
        Ok((None, None))
    }
}
