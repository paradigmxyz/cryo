use cryo_freeze::{BlockChunk, Fetcher, ParseError};
use ethers::prelude::*;
use polars::prelude::*;

use crate::{
    parse::blocks::{block_range_to_block_chunk, postprocess_block_chunks},
    Args,
};

pub(crate) async fn parse_timestamp<P: JsonRpcClient>(
    args: &Args,
    fetcher: Arc<Fetcher<P>>,
) -> Result<(Option<Vec<Option<String>>>, Option<Vec<BlockChunk>>), ParseError> {
    let (files, explicit_numbers): (Vec<&String>, Vec<&String>) = match &args.timestamp {
        Some(timestamp) => timestamp.iter().partition(|tx| std::path::Path::new(tx).exists()),
        None => return Ok((None, None)),
    };

    let (file_labels, file_chunks) = if !files.is_empty() {
        let mut file_labels = Vec::new();
        let mut file_chunks = Vec::new();
        for path in files {
            let column = if path.contains(':') {
                path.split(':')
                    .last()
                    .ok_or(ParseError::ParseError("could not parse txs path column".to_string()))?
            } else {
                "timestamp"
            };
            let integers = read_integer_column(path, column)
                .map_err(|_e| ParseError::ParseError("could not read input".to_string()))?;
            let chunk = BlockChunk::Numbers(integers);
            let chunk_label = path
                .split("__")
                .last()
                .and_then(|s| s.strip_suffix(".parquet").map(|s| s.to_string()));
            file_labels.push(chunk_label);
            file_chunks.push(chunk);
        }
        (Some(file_labels), Some(file_chunks))
    } else {
        (None, None)
    };

    let explicit_chunks = if !explicit_numbers.is_empty() {
        // parse inputs into BlockChunks
        let mut block_chunks = Vec::new();
        for explicit_number in explicit_numbers {
            let outputs = parse_timestamp_inputs(explicit_number, &fetcher).await?;
            block_chunks.extend(outputs);
        }
        postprocess_block_chunks(block_chunks, args, fetcher).await?
    } else {
        Vec::new()
    };

    let mut block_chunks = Vec::new();
    let labels = match (file_labels, file_chunks) {
        (Some(file_labels), Some(file_chunks)) => {
            let mut labels = Vec::new();
            labels.extend(file_labels);
            block_chunks.extend(file_chunks);
            labels.extend(vec![None; explicit_chunks.len()]);
            Some(labels)
        }
        _ => None,
    };
    block_chunks.extend(explicit_chunks);
    Ok((labels, Some(block_chunks)))
}

fn read_integer_column(path: &str, column: &str) -> Result<Vec<u64>, ParseError> {
    let file = std::fs::File::open(path)
        .map_err(|_e| ParseError::ParseError("could not open file path".to_string()))?;

    let df = ParquetReader::new(file)
        .with_columns(Some(vec![column.to_string()]))
        .finish()
        .map_err(|_e| ParseError::ParseError("could not read data from column".to_string()))?;

    let series = df
        .column(column)
        .map_err(|_e| ParseError::ParseError("could not get column".to_string()))?
        .unique()
        .map_err(|_e| ParseError::ParseError("could not get column".to_string()))?;

    println!("{:?}", series);
    match series.u32() {
        Ok(ca) => ca
            .into_iter()
            .map(|v| {
                v.ok_or_else(|| ParseError::ParseError("timestamp missing".to_string()))
                    .map(|data| data.into())
            })
            .collect(),
        Err(_e) => match series.u64() {
            Ok(ca) => ca
                .into_iter()
                .map(|v| v.ok_or_else(|| ParseError::ParseError("timestamp missing".to_string())))
                .collect(),
            Err(_e) => {
                Err(ParseError::ParseError("could not convert to integer column".to_string()))
            }
        },
    }
}

/// parse block numbers to freeze
async fn parse_timestamp_inputs<P: JsonRpcClient>(
    inputs: &str,
    fetcher: &Fetcher<P>,
) -> Result<Vec<BlockChunk>, ParseError> {
    let parts: Vec<&str> = inputs.split(' ').collect();
    match parts.len() {
        1 => {
            let first_input = parts.first().ok_or_else(|| {
                ParseError::ParseError("Failed to get the first input".to_string())
            })?;
            parse_timestamp_token(first_input, true, fetcher).await.map(|x| vec![x])
        }
        _ => {
            let mut chunks = Vec::new();
            for part in parts {
                chunks.push(parse_timestamp_token(part, false, fetcher).await?);
            }
            Ok(chunks)
        }
    }
}
enum RangePosition {
    First,
    Last,
    None,
}

async fn parse_timestamp_token<P: JsonRpcClient>(
    s: &str,
    as_range: bool,
    fetcher: &Fetcher<P>,
) -> Result<BlockChunk, ParseError> {
    let s = s.replace('_', "");

    let parts: Vec<&str> = s.split(':').collect();
    match parts.as_slice() {
        [block_ref] => {
            let block =
                parse_timestamp_number_to_block_number(block_ref, RangePosition::None, fetcher)
                    .await?;
            Ok(BlockChunk::Numbers(vec![block]))
        }
        [first_ref, second_ref] => {
            let parts: Vec<_> = second_ref.split('/').collect();
            let (second_ref, n_keep) = if parts.len() == 2 {
                let n_keep = parts[1].parse::<u32>().map_err(|_| {
                    ParseError::ParseError("cannot parse timestamp interval size".to_string())
                })?;
                (parts[0], Some(n_keep))
            } else {
                (*second_ref, None)
            };

            let (start_block, end_block) =
                parse_timestamp_range_to_block_number_range(first_ref, second_ref, fetcher).await?;
            block_range_to_block_chunk(start_block, end_block, as_range, None, n_keep)
        }
        [first_ref, second_ref, third_ref] => {
            let (start_block, end_block) =
                parse_timestamp_range_to_block_number_range(first_ref, second_ref, fetcher).await?;
            let range_size = third_ref
                .parse::<u32>()
                .map_err(|_e| ParseError::ParseError("start_block parse error".to_string()))?;
            block_range_to_block_chunk(start_block, end_block, false, Some(range_size), None)
        }
        _ => Err(ParseError::ParseError(
            "timestamps must be in format timestamp or start_timestamp:end_timestamp".to_string(),
        )),
    }
}

async fn parse_timestamp_range_to_block_number_range<P>(
    first_ref: &str,
    second_ref: &str,
    fetcher: &Fetcher<P>,
) -> Result<(u64, u64), ParseError>
where
    P: JsonRpcClient,
{
    let (start_block, end_block) = match (first_ref, second_ref) {
        _ if first_ref.starts_with('-') => {
            let end_block =
                parse_timestamp_number_to_block_number(second_ref, RangePosition::Last, fetcher)
                    .await?;
            let start_block = end_block
                .checked_sub(first_ref[1..].parse::<u64>().map_err(|_e| {
                    ParseError::ParseError("start_timestamp parse error".to_string())
                })?)
                .ok_or_else(|| ParseError::ParseError("start_timestamp underflow".to_string()))?;
            (start_block, end_block)
        }
        _ if second_ref.starts_with('+') => {
            let start_block =
                parse_timestamp_number_to_block_number(first_ref, RangePosition::First, fetcher)
                    .await?;
            let end_block = start_block
                .checked_add(second_ref[1..].parse::<u64>().map_err(|_e| {
                    ParseError::ParseError("start_timestamp parse error".to_string())
                })?)
                .ok_or_else(|| ParseError::ParseError("end_timestamp underflow".to_string()))?;
            (start_block, end_block)
        }
        _ => {
            let start_block =
                parse_timestamp_number_to_block_number(first_ref, RangePosition::First, fetcher)
                    .await?;
            let end_block =
                parse_timestamp_number_to_block_number(second_ref, RangePosition::Last, fetcher)
                    .await?;
            (start_block, end_block)
        }
    };

    let end_block =
        if second_ref != "latest" && !second_ref.is_empty() && !first_ref.starts_with('-') {
            end_block - 1
        } else {
            end_block
        };

    let start_block = if first_ref.starts_with('-') { start_block + 1 } else { start_block };

    Ok((start_block, end_block))
}

// parse timestamp numbers into block_numbers
async fn parse_timestamp_number_to_block_number<P: JsonRpcClient>(
    timestamp_ref: &str,
    range_position: RangePosition,
    fetcher: &Fetcher<P>,
) -> Result<u64, ParseError> {
    match (timestamp_ref, range_position) {
        ("latest", _) => get_latest_block_number(fetcher).await,
        ("", RangePosition::First) => Ok(0),
        ("", RangePosition::Last) => get_latest_block_number(fetcher).await,
        ("", RangePosition::None) => Err(ParseError::ParseError("invalid input".to_string())),
        _ if timestamp_ref.ends_with('B') | timestamp_ref.ends_with('b') => {
            timestamp_str_with_metric_unit_to_block_number(timestamp_ref, 1e9, fetcher).await
        }
        _ if timestamp_ref.ends_with('M') | timestamp_ref.ends_with('m') => {
            timestamp_str_with_metric_unit_to_block_number(timestamp_ref, 1e6, fetcher).await
        }
        _ if timestamp_ref.ends_with('K') | timestamp_ref.ends_with('k') => {
            timestamp_str_with_metric_unit_to_block_number(timestamp_ref, 1e3, fetcher).await
        }
        _ => timestamp_ref
            .parse::<f64>()
            .map_err(|_e| ParseError::ParseError("Error parsing timestamp ref".to_string()))
            .map(|x| x as u64),
    }
}

async fn timestamp_str_with_metric_unit_to_block_number<P: JsonRpcClient>(
    timestamp_ref: &str,
    metric_scale: f64,
    fetcher: &Fetcher<P>,
) -> Result<u64, ParseError> {
    let s = &timestamp_ref[..timestamp_ref.len() - 1];
    let timestamp = s
        .parse::<f64>()
        .map(|n| (metric_scale * n) as u64)
        .map_err(|_e| ParseError::ParseError("Error parsing timestamp ref".to_string()))?;

    return timestamp_to_block_number(timestamp, fetcher).await;
}

async fn timestamp_to_block_number<P: JsonRpcClient>(
    timestamp: u64,
    fetcher: &Fetcher<P>,
) -> Result<u64, ParseError> {
    let latest_block_number = get_latest_block_number(fetcher).await?;

    // perform binary search to determine the block number for a given timestamp.
    // If the exact timestamp is not found, we return the closest block number.
    let mut l = 0;
    let mut r = latest_block_number;
    let mut mid = (l + r) / 2;
    let mut block = fetcher
        .get_block(mid)
        .await
        .map_err(|_e| ParseError::ParseError("Error fetching block for timestamp".to_string()))?
        .unwrap();

    while l <= r {
        mid = (l + r) / 2;
        block = fetcher
            .get_block(mid)
            .await
            .map_err(|_e| ParseError::ParseError("Error fetching block for timestamp".to_string()))?
            .unwrap();

        if block.timestamp == timestamp.into() {
            return Ok(mid);
        } else if block.timestamp < timestamp.into() {
            l = mid + 1;
        } else {
            r = mid - 1;
        }
    }

    return Ok(mid);
}

async fn get_latest_block_number<P: JsonRpcClient>(
    fetcher: &Fetcher<P>,
) -> Result<u64, ParseError> {
    return fetcher
        .get_block_number()
        .await
        .map(|n| n.as_u64())
        .map_err(|_e| ParseError::ParseError("Error retrieving latest block number".to_string()));
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use governor::{Quota, RateLimiter};

    use super::*;

    #[tokio::test]
    async fn test_timestamp_to_block_number() {
        // Setup
        let rpc_url = String::from("https://eth.llamarpc.com");
        let max_retry = 5;
        let initial_backoff = 500;
        let max_concurrent_requests = 100;
        let provider =
            Provider::<RetryClient<Http>>::new_client(&rpc_url, max_retry, initial_backoff)
                .map_err(|_e| ParseError::ParseError("could not connect to provider".to_string()))
                .unwrap();
        let rate_limiter = None;

        let semaphore = tokio::sync::Semaphore::new(max_concurrent_requests as usize);

        let fetcher = Fetcher { provider, semaphore: Some(semaphore), rate_limiter };

        // Genesis block
        assert!(timestamp_to_block_number(1438269973, &fetcher).await.unwrap() == 0);

        // Block 1000, and the timestamp surrounding block 1020
        assert!(timestamp_to_block_number(1438272177, &fetcher).await.unwrap() == 1020);
        assert!(timestamp_to_block_number(1438272178, &fetcher).await.unwrap() == 1020);

        // Timestamp 1438272176 is 1 seconds after block 1019 and 1 second before block 1020. Higher block is returned
        assert!(timestamp_to_block_number(1438272176, &fetcher).await.unwrap() == 1020);
        
        // Timestamp 1438272169 is 4 seconds after block 1016 and 4 seconds before block 1017. Higher block is returned
        assert!(timestamp_to_block_number(1438272169, &fetcher).await.unwrap() == 1017);

        // Timestamp 1438272187 is 1 seconds after block 1024 and 1 second before block 1025. Lower block is returned
        assert!(timestamp_to_block_number(1438272187, &fetcher).await.unwrap() == 1024);
    }
}
