use cryo_freeze::{BlockChunk, Fetcher, ParseError};
use ethers::prelude::*;
use polars::prelude::*;

use crate::{
    parse::blocks::{block_range_to_block_chunk, postprocess_block_chunks},
    Args,
};

use super::blocks::get_latest_block_number;

pub(crate) async fn parse_timestamps<P: JsonRpcClient>(
    args: &Args,
    fetcher: Arc<Fetcher<P>>,
) -> Result<(Option<Vec<Option<String>>>, Option<Vec<BlockChunk>>), ParseError> {
    let (files, explicit_numbers): (Vec<&String>, Vec<&String>) = match &args.timestamps {
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

/// parse timestamp numbers to freeze
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
        [timestamp_ref] => {
            let timestamp =
                parse_timestamp_number(timestamp_ref, RangePosition::None, fetcher).await?;
            let block = timestamp_to_block_number(timestamp, fetcher).await?;

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

            let (start_timestamp, end_timestamp) =
                parse_timestamp_range(first_ref, second_ref, fetcher).await?;
            let (start_block, end_block) = (
                timestamp_to_block_number(start_timestamp, fetcher).await?,
                timestamp_to_block_number(end_timestamp, fetcher).await?,
            );
            block_range_to_block_chunk(start_block, end_block, as_range, None, n_keep)
        }
        _ => Err(ParseError::ParseError(
            "timestamps must be in format timestamp or start_timestamp:end_timestamp".to_string(),
        )),
    }
}

async fn parse_timestamp_range<P>(
    first_ref: &str,
    second_ref: &str,
    fetcher: &Fetcher<P>,
) -> Result<(u64, u64), ParseError>
where
    P: JsonRpcClient,
{
    let (start_timestamp, end_timestamp) = match (first_ref, second_ref) {
        _ if first_ref.starts_with('-') => {
            let end_timestamp =
                parse_timestamp_number(second_ref, RangePosition::Last, fetcher).await?;

            let start_timestamp = end_timestamp
                .checked_sub(
                    parse_timestamp_number(&first_ref[1..], RangePosition::None, fetcher).await?,
                )
                .ok_or_else(|| ParseError::ParseError("start_timestamp underflow".to_string()))?;

            (start_timestamp, end_timestamp)
        }
        _ if second_ref.starts_with('+') => {
            let start_timestamp =
                parse_timestamp_number(first_ref, RangePosition::First, fetcher).await?;

            let end_timestamp = start_timestamp
                .checked_add(
                    parse_timestamp_number(&second_ref[1..], RangePosition::None, fetcher).await?,
                )
                .ok_or_else(|| ParseError::ParseError("end_timestamp overflow".to_string()))?;

            (start_timestamp, end_timestamp)
        }
        _ => {
            let start_timestamp =
                parse_timestamp_number(first_ref, RangePosition::First, fetcher).await?;

            let end_timestamp =
                parse_timestamp_number(second_ref, RangePosition::Last, fetcher).await?;

            (start_timestamp, end_timestamp)
        }
    };

    let end_timestamp =
        if second_ref != "latest" && !second_ref.is_empty() && !first_ref.starts_with('-') {
            end_timestamp - 1
        } else {
            end_timestamp
        };

    Ok((start_timestamp, end_timestamp))
}

async fn parse_timestamp_number<P: JsonRpcClient>(
    timestamp_ref: &str,
    range_position: RangePosition,
    fetcher: &Fetcher<P>,
) -> Result<u64, ParseError> {
    match (timestamp_ref, range_position) {
        ("latest", _) => get_latest_timestamp(fetcher).await,
        ("", RangePosition::First) => Ok(0),
        ("", RangePosition::Last) => get_latest_timestamp(fetcher).await,
        ("", RangePosition::None) => Err(ParseError::ParseError("invalid input".to_string())),
        _ if timestamp_ref.ends_with('m') => scale_timestamp_str_by_metric_unit(timestamp_ref, 60),
        _ if timestamp_ref.ends_with('h') => {
            scale_timestamp_str_by_metric_unit(timestamp_ref, 3600)
        }
        _ if timestamp_ref.ends_with('d') => {
            scale_timestamp_str_by_metric_unit(timestamp_ref, 86400)
        }
        _ if timestamp_ref.ends_with('w') => {
            scale_timestamp_str_by_metric_unit(timestamp_ref, 86400 * 7)
        }
        _ if timestamp_ref.ends_with('M') => {
            scale_timestamp_str_by_metric_unit(timestamp_ref, 86400 * 30)
        }
        _ if timestamp_ref.ends_with('y') => {
            scale_timestamp_str_by_metric_unit(timestamp_ref, 86400 * 365)
        }
        _ => timestamp_ref
            .parse::<f64>()
            .map_err(|_e| ParseError::ParseError("Error parsing timestamp ref".to_string()))
            .map(|x| x as u64),
    }
}

fn scale_timestamp_str_by_metric_unit(
    timestamp_ref: &str,
    metric_scale: u64,
) -> Result<u64, ParseError> {
    let s = &timestamp_ref[..timestamp_ref.len() - 1];
    let timestamp = s
        .parse::<f64>()
        .map(|n| (metric_scale as f64 * n) as u64)
        .map_err(|_e| ParseError::ParseError("Error parsing timestamp ref".to_string()));

    return timestamp;
}

// perform binary search to determine the closest block number smaller than or equal to a given
// timestamp
async fn timestamp_to_block_number<P: JsonRpcClient>(
    timestamp: u64,
    fetcher: &Fetcher<P>,
) -> Result<u64, ParseError> {
    let latest_block_number = get_latest_block_number(fetcher).await?;

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

    // If timestamp is between two different blocks, return the lower block.
    if mid > 0 && block.timestamp > ethers::types::U256::from(timestamp) {
        return Ok(mid - 1);
    } else {
        return Ok(mid);
    }
}

async fn get_latest_timestamp<P: JsonRpcClient>(fetcher: &Fetcher<P>) -> Result<u64, ParseError> {
    let latest_block_number = get_latest_block_number(fetcher).await?;
    let latest_block = fetcher
        .get_block(latest_block_number)
        .await
        .map_err(|_e| ParseError::ParseError("Error fetching latest block".to_string()))?
        .unwrap();

    return Ok(latest_block.timestamp.as_u64());
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use governor::{Quota, RateLimiter};

    use super::*;

    async fn setup_fetcher() -> Fetcher<RetryClient<Http>> {
        let rpc_url = String::from("https://eth.merkle.io");
        let max_retry = 5;
        let initial_backoff = 500;
        let max_concurrent_requests = 100;
        let provider =
            Provider::<RetryClient<Http>>::new_client(&rpc_url, max_retry, initial_backoff)
                .map_err(|_e| ParseError::ParseError("could not connect to provider".to_string()))
                .unwrap();

        let quota = Quota::per_second(NonZeroU32::new(15).unwrap())
            .allow_burst(NonZeroU32::new(1).unwrap());
        let rate_limiter = Some(RateLimiter::direct(quota));
        let semaphore = tokio::sync::Semaphore::new(max_concurrent_requests as usize);

        Fetcher { provider, semaphore: Some(semaphore), rate_limiter }
    }

    #[tokio::test]
    async fn test_extrema_timestamp_to_block_number() {
        let fetcher = setup_fetcher().await;

        // Before genesis block
        assert!(timestamp_to_block_number(1438260000, &fetcher).await.unwrap() == 0);
    }

    #[tokio::test]
    async fn test_latest_timestamp_to_block_number() {
        let fetcher = setup_fetcher().await;
        let latest_block_number = get_latest_block_number(&fetcher).await.unwrap();
        let latest_block = fetcher.get_block(latest_block_number).await.unwrap().unwrap();
        let latest_timestamp = latest_block.timestamp.as_u64();

        assert_eq!(
            timestamp_to_block_number(latest_timestamp, &fetcher).await.unwrap(),
            latest_block_number
        );
    }

    #[tokio::test]
    async fn test_timestamp_between_blocks() {
        let fetcher = setup_fetcher().await;

        // Block 1000, and the timestamp surrounding block 1020
        assert!(timestamp_to_block_number(1438272177, &fetcher).await.unwrap() == 1020);
        assert!(timestamp_to_block_number(1438272178, &fetcher).await.unwrap() == 1020);

        // Timestamp 1438272176 is 1 seconds after block 1019 and 1 second before block 1020. Lower
        // block is returned
        assert!(timestamp_to_block_number(1438272176, &fetcher).await.unwrap() == 1019);

        // Timestamp 1438272187 is 1 seconds after block 1024 and 1 second before block 1025. Lower
        // block is returned
        assert!(timestamp_to_block_number(1438272187, &fetcher).await.unwrap() == 1024);

        // Timestamp 1438272169 is 4 seconds after block 1016 and 4 seconds before block 1017. Lower
        // block is returned
        assert!(timestamp_to_block_number(1438272169, &fetcher).await.unwrap() == 1016);
    }

    #[tokio::test]
    async fn test_parse_timestamp_number() {
        let fetcher = setup_fetcher().await;
        let latest_timestamp =
            parse_timestamp_number("latest", RangePosition::None, &fetcher).await.unwrap();
        assert_eq!(latest_timestamp, get_latest_timestamp(&fetcher).await.unwrap());

        assert_eq!(parse_timestamp_number("", RangePosition::First, &fetcher).await.unwrap(), 0);

        assert_eq!(
            parse_timestamp_number("", RangePosition::Last, &fetcher).await.unwrap(),
            get_latest_timestamp(&fetcher).await.unwrap()
        );

        assert_eq!(
            parse_timestamp_number("1700000000", RangePosition::None, &fetcher).await.unwrap(),
            1700000000
        );

        assert_eq!(parse_timestamp_number("1m", RangePosition::None, &fetcher).await.unwrap(), 60);

        assert_eq!(
            parse_timestamp_number("8760h", RangePosition::None, &fetcher).await.unwrap(),
            8760 * 3600
        );

        assert_eq!(
            parse_timestamp_number("365d", RangePosition::None, &fetcher).await.unwrap(),
            365 * 86400
        );

        assert_eq!(
            parse_timestamp_number("52w", RangePosition::None, &fetcher).await.unwrap(),
            52 * 86400 * 7
        );

        assert_eq!(
            parse_timestamp_number("12M", RangePosition::None, &fetcher).await.unwrap(),
            12 * 86400 * 30
        );

        assert_eq!(
            parse_timestamp_number("1y", RangePosition::None, &fetcher).await.unwrap(),
            86400 * 365
        );
    }

    #[tokio::test]
    async fn test_parse_timestamp_range_to_block_number_range() {
        let fetcher = setup_fetcher().await;

        let (start_timestamp, end_timestamp) =
            parse_timestamp_range("1700000000", "1700000015", &fetcher).await.unwrap();
        assert_eq!(
            (
                timestamp_to_block_number(start_timestamp, &fetcher).await.unwrap(),
                timestamp_to_block_number(end_timestamp, &fetcher).await.unwrap()
            ),
            (18573050, 18573051)
        );

        let (start_timestamp, end_timestamp) =
            parse_timestamp_range("-15", "1700000015", &fetcher).await.unwrap();
        assert_eq!(
            (
                timestamp_to_block_number(start_timestamp, &fetcher).await.unwrap(),
                timestamp_to_block_number(end_timestamp, &fetcher).await.unwrap()
            ),
            (18573050, 18573052)
        );

        let (start_timestamp, end_timestamp) =
            parse_timestamp_range("1700000000", "+15", &fetcher).await.unwrap();
        assert_eq!(
            (
                timestamp_to_block_number(start_timestamp, &fetcher).await.unwrap(),
                timestamp_to_block_number(end_timestamp, &fetcher).await.unwrap()
            ),
            (18573050, 18573051)
        );
    }
}
