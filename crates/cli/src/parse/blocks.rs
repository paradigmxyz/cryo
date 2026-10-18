use polars::prelude::*;
use std::collections::HashMap;

use cryo_freeze::{BlockChunk, ChunkData, Datatype, ParseError, Source, Subchunk, Table};

use crate::args::Args;

pub(crate) async fn parse_blocks(
    args: &Args,
    source: Arc<Source>,
) -> Result<(Option<Vec<Option<String>>>, Option<Vec<BlockChunk>>), ParseError> {
    let (files, explicit_numbers): (Vec<&String>, Vec<&String>) = match &args.blocks {
        Some(blocks) => blocks.iter().partition(|tx| std::path::Path::new(tx).exists()),
        None => return Ok((None, None)),
    };

    let (file_labels, file_chunks) = if !files.is_empty() {
        let mut file_labels = Vec::new();
        let mut file_chunks = Vec::new();
        for path in files {
            let column = if path.contains(':') {
                path.split(':')
                    .next_back()
                    .ok_or(ParseError::ParseError("could not parse txs path column".to_string()))?
            } else {
                "block_number"
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
            let outputs = parse_block_inputs(explicit_number, source.clone()).await?;
            block_chunks.extend(outputs);
        }
        postprocess_block_chunks(block_chunks, args, source).await?
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
                v.ok_or_else(|| ParseError::ParseError("block number missing".to_string()))
                    .map(|data| data.into())
            })
            .collect(),
        Err(_e) => match series.u64() {
            Ok(ca) => ca
                .into_iter()
                .map(|v| {
                    v.ok_or_else(|| ParseError::ParseError("block number missing".to_string()))
                })
                .collect(),
            Err(_e) => {
                Err(ParseError::ParseError("could not convert to integer column".to_string()))
            }
        },
    }
}

pub(crate) async fn postprocess_block_chunks(
    block_chunks: Vec<BlockChunk>,
    args: &Args,
    source: Arc<Source>,
) -> Result<Vec<BlockChunk>, ParseError> {
    // align
    let block_chunks = if args.align {
        block_chunks.into_iter().filter_map(|x| x.align(args.chunk_size)).collect()
    } else {
        block_chunks
    };

    // split block range into chunks
    let block_chunks = match args.n_chunks {
        Some(n_chunks) => block_chunks.subchunk_by_count(&n_chunks),
        None => block_chunks.subchunk_by_size(&args.chunk_size),
    };

    // apply reorg buffer
    let block_chunks = apply_reorg_buffer(block_chunks, args.reorg_buffer, source).await?;

    Ok(block_chunks)
}

pub(crate) async fn get_default_block_chunks(
    args: &Args,
    source: Arc<Source>,
    schemas: &HashMap<Datatype, Table>,
) -> Result<Vec<BlockChunk>, ParseError> {
    let default_blocks = match schemas
        .keys()
        .map(|datatype| datatype.default_blocks())
        .find(|blocks| !blocks.is_none())
    {
        Some(Some(blocks)) => blocks,
        _ => "0:latest".to_string(),
    };
    let block_chunks = parse_block_inputs(&default_blocks, source.clone()).await?;
    postprocess_block_chunks(block_chunks, args, source).await
}

/// parse block numbers to freeze
async fn parse_block_inputs(
    inputs: &str,
    source: Arc<Source>,
) -> Result<Vec<BlockChunk>, ParseError> {
    let parts: Vec<&str> = inputs.split(' ').collect();
    match parts.len() {
        1 => {
            let first_input = parts.first().ok_or_else(|| {
                ParseError::ParseError("Failed to get the first input".to_string())
            })?;
            parse_block_token(first_input, true, source).await.map(|x| vec![x])
        }
        _ => {
            let mut chunks = Vec::new();
            for part in parts {
                chunks.push(parse_block_token(part, false, source.clone()).await?);
            }
            Ok(chunks)
        }
    }
}

#[derive(Clone, Debug)]
enum RangePosition {
    First,
    Last,
    None,
}

async fn parse_block_token(
    s: &str,
    as_range: bool,
    source: Arc<Source>,
) -> Result<BlockChunk, ParseError> {
    let s = s.replace('_', "");

    let parts: Vec<&str> = s.split(':').collect();
    match parts.as_slice() {
        [block_ref] => {
            let block = parse_block_number(block_ref, RangePosition::None, source).await?;
            Ok(BlockChunk::Numbers(vec![block]))
        }
        [first_ref, second_ref] => {
            let parts: Vec<_> = second_ref.split('/').collect();
            let (second_ref, n_keep) = if parts.len() == 2 {
                let n_keep = parts[1].parse::<u32>().map_err(|_| {
                    ParseError::ParseError("cannot parse block interval size".to_string())
                })?;
                (parts[0], Some(n_keep))
            } else {
                (*second_ref, None)
            };

            let (start_block, end_block) = parse_block_range(first_ref, second_ref, source).await?;
            block_range_to_block_chunk(start_block, end_block, as_range, None, n_keep)
        }
        [first_ref, second_ref, third_ref] => {
            let (start_block, end_block) = parse_block_range(first_ref, second_ref, source).await?;
            let range_size = third_ref
                .parse::<u32>()
                .map_err(|_e| ParseError::ParseError("start_block parse error".to_string()))?;
            block_range_to_block_chunk(start_block, end_block, false, Some(range_size), None)
        }
        _ => Err(ParseError::ParseError(
            "blocks must be in format block_number or start_block:end_block".to_string(),
        )),
    }
}

pub(crate) fn block_range_to_block_chunk(
    start_block: u64,
    end_block: u64,
    as_range: bool,
    skip: Option<u32>,
    n_blocks: Option<u32>,
) -> Result<BlockChunk, ParseError> {
    if end_block < start_block {
        Err(ParseError::ParseError("end_block should not be less than start_block".to_string()))
    } else if let Some(n_blocks) = n_blocks {
        let blocks = evenly_spaced_subset((start_block..=end_block).collect(), n_blocks as usize);
        Ok(BlockChunk::Numbers(blocks))
    } else if as_range {
        Ok(BlockChunk::Range(start_block, end_block))
    } else {
        let blocks = match skip {
            Some(skip) => (start_block..=end_block)
                .enumerate()
                .filter(|(idx, _)| idx % (skip as usize) == 0)
                .map(|(_, value)| value)
                .collect(),
            None => match n_blocks {
                Some(n_blocks) => {
                    evenly_spaced_subset((start_block..=end_block).collect(), n_blocks as usize)
                }
                None => (start_block..=end_block).collect(),
            },
        };
        Ok(BlockChunk::Numbers(blocks))
    }
}

fn evenly_spaced_subset<T: Clone>(items: Vec<T>, subset_length: usize) -> Vec<T> {
    if subset_length == 0 || items.is_empty() {
        return Vec::new();
    }

    if subset_length >= items.len() {
        return items.to_vec();
    }

    let original_length = items.len();
    let interval = (original_length - 1) as f64 / (subset_length - 1) as f64;

    let mut accumulator: f64 = 0.0;
    let mut subset = Vec::with_capacity(subset_length);

    for _ in 0..subset_length {
        let index = accumulator.floor() as usize;
        subset.push(items[index].clone());
        accumulator += interval;
    }

    subset
}

async fn parse_block_range(
    first_ref: &str,
    second_ref: &str,
    source: Arc<Source>,
) -> Result<(u64, u64), ParseError> {
    let (start_block, end_block) = match (first_ref, second_ref) {
        _ if first_ref.starts_with('-') => {
            let end_block = parse_block_number(second_ref, RangePosition::Last, source).await?;
            let start_block =
                end_block
                    .checked_sub(first_ref[1..].parse::<u64>().map_err(|_e| {
                        ParseError::ParseError("start_block parse error".to_string())
                    })?)
                    .ok_or_else(|| ParseError::ParseError("start_block underflow".to_string()))?;
            (start_block, end_block)
        }
        _ if second_ref.starts_with('+') => {
            let start_block = parse_block_number(first_ref, RangePosition::First, source).await?;
            let end_block =
                start_block
                    .checked_add(second_ref[1..].parse::<u64>().map_err(|_e| {
                        ParseError::ParseError("start_block parse error".to_string())
                    })?)
                    .ok_or_else(|| ParseError::ParseError("end_block underflow".to_string()))?;
            (start_block, end_block)
        }
        _ => {
            let start_block =
                parse_block_number(first_ref, RangePosition::First, source.clone()).await?;
            let end_block = parse_block_number(second_ref, RangePosition::Last, source).await?;
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

async fn parse_block_number(
    block_ref: &str,
    range_position: RangePosition,
    source: Arc<Source>,
) -> Result<u64, ParseError> {
    match (block_ref, range_position) {
        ("latest", _) => source.get_block_number().await.map_err(|_e| {
            ParseError::ParseError("Error retrieving latest block number".to_string())
        }),
        ("", RangePosition::First) => Ok(0),
        ("", RangePosition::Last) => source
            .get_block_number()
            .await
            .map_err(|_e| ParseError::ParseError("Error retrieving last block number".to_string())),
        ("", RangePosition::None) => Err(ParseError::ParseError("invalid input".to_string())),
        _ if block_ref.ends_with('B') | block_ref.ends_with('b') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e9 * n).round() as u64)
                .map_err(|_e| ParseError::ParseError("Error parsing block ref".to_string()))
        }
        _ if block_ref.ends_with('M') | block_ref.ends_with('m') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e6 * n).round() as u64)
                .map_err(|_e| ParseError::ParseError("Error parsing block ref".to_string()))
        }
        _ if block_ref.ends_with('K') | block_ref.ends_with('k') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e3 * n).round() as u64)
                .map_err(|_e| ParseError::ParseError("Error parsing block ref".to_string()))
        }
        _ => block_ref
            .parse::<f64>()
            .map_err(|_e| ParseError::ParseError("Error parsing block ref".to_string()))
            .map(|x| x as u64),
    }
}

async fn apply_reorg_buffer(
    block_chunks: Vec<BlockChunk>,
    reorg_filter: u64,
    source: Arc<Source>,
) -> Result<Vec<BlockChunk>, ParseError> {
    match reorg_filter {
        0 => Ok(block_chunks),
        reorg_filter => {
            let latest_block = match source.get_block_number().await {
                Ok(result) => result,
                Err(_e) => {
                    return Err(ParseError::ParseError("reorg buffer parse error".to_string()))
                }
            };
            let max_allowed = latest_block - reorg_filter;
            Ok(block_chunks
                .into_iter()
                .filter_map(|x| match x.max_value() {
                    Some(max_block) if max_block <= max_allowed => Some(x),
                    _ => None,
                })
                .collect())
        }
    }
}

pub(crate) async fn get_latest_block_number(source: Arc<Source>) -> Result<u64, ParseError> {
    source
        .get_block_number()
        .await
        .map_err(|_e| ParseError::ParseError("Error retrieving latest block number".to_string()))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use alloy::{
        providers::{Provider, ProviderBuilder},
        transports::ipc::{IpcConnect, MockIpcServer},
    };

    use super::*;

    #[derive(Clone, Debug)]
    enum BlockTokenTest<'a> {
        WithoutMock((&'a str, BlockChunk)),   // Token | Expected
        WithMock((&'a str, BlockChunk, u64)), // Token | Expected | Mock Block Response
    }

    async fn block_token_test_helper(
        tests: Vec<(BlockTokenTest<'_>, bool)>,
        mock_ipc_path: PathBuf,
    ) {
        let ipc = IpcConnect::new(mock_ipc_path);
        let provider = ProviderBuilder::new().connect_ipc(ipc).await.unwrap().erased();
        let source = Source {
            provider,
            semaphore: Arc::new(None),
            rate_limiter: Arc::new(None),
            chain_id: 1,
            inner_request_size: 1,
            max_concurrent_chunks: None,
            rpc_url: "".to_string(),
            jwt: None,
            labels: cryo_freeze::SourceLabels::default(),
        };
        let source = Arc::new(source);
        for (test, res) in tests {
            match test {
                BlockTokenTest::WithMock((token, expected, _latest)) => {
                    assert_eq!(
                        block_token_test_executor(token, expected, source.clone()).await,
                        res
                    );
                }
                BlockTokenTest::WithoutMock((token, expected)) => {
                    assert_eq!(
                        block_token_test_executor(token, expected, source.clone()).await,
                        res
                    );
                }
            }
        }
    }

    async fn block_token_test_executor(
        token: &str,
        expected: BlockChunk,
        source: Arc<Source>,
    ) -> bool {
        match expected {
            BlockChunk::Numbers(expected_block_numbers) => {
                let block_chunks = parse_block_token(token, false, source).await.unwrap();
                assert!(matches!(block_chunks, BlockChunk::Numbers { .. }));
                let BlockChunk::Numbers(block_numbers) = block_chunks else {
                    panic!("Unexpected shape")
                };
                block_numbers == expected_block_numbers
            }
            BlockChunk::Range(expected_range_start, expected_range_end) => {
                let block_chunks = parse_block_token(token, true, source).await.unwrap();
                assert!(matches!(block_chunks, BlockChunk::Range { .. }));
                let BlockChunk::Range(range_start, range_end) = block_chunks else {
                    panic!("Unexpected shape")
                };
                expected_range_start == range_start && expected_range_end == range_end
            }
        }
    }

    #[derive(Clone, Debug)]
    enum BlockInputTest<'a> {
        WithoutMock((&'a String, Vec<BlockChunk>)), // Token | Expected
        WithMock((&'a String, Vec<BlockChunk>, u64)), // Token | Expected | Mock Block Response
    }

    async fn block_input_test_helper(
        tests: Vec<(BlockInputTest<'_>, bool)>,
        mock_ipc_path: PathBuf,
    ) {
        let ipc = IpcConnect::new(mock_ipc_path);
        let provider = ProviderBuilder::new().connect_ipc(ipc).await.unwrap().erased();
        let source = Arc::new(Source {
            provider,
            chain_id: 1,
            rpc_url: "".to_string(),
            inner_request_size: 1,
            semaphore: Arc::new(None),
            max_concurrent_chunks: Some(1),
            rate_limiter: Arc::new(None),
            jwt: None,
            labels: cryo_freeze::SourceLabels::default(),
        });
        for (test, res) in tests {
            match test {
                BlockInputTest::WithMock((inputs, expected, _latest)) => {
                    assert_eq!(
                        block_input_test_executor(inputs, expected, source.clone()).await,
                        res
                    );
                }
                BlockInputTest::WithoutMock((inputs, expected)) => {
                    println!("RES {res:?}");
                    println!("inputs {inputs:?}");
                    println!("EXPECTED {expected:?}");
                    let actual = block_input_test_executor(inputs, expected, source.clone()).await;
                    println!("ACTUAL {actual:?}");
                    assert_eq!(actual, res);
                }
            }
        }
    }

    async fn block_input_test_executor(
        inputs: &str,
        expected: Vec<BlockChunk>,
        source: Arc<Source>,
    ) -> bool {
        let block_chunks = parse_block_inputs(inputs, source).await.unwrap();
        assert_eq!(block_chunks.len(), expected.len());
        for (i, block_chunk) in block_chunks.iter().enumerate() {
            let expected_chunk = &expected[i];
            println!("BLOCK_CHUNK {block_chunk:?}");
            println!("EXCPECTED_CHUNK {expected_chunk:?}");
            match expected_chunk {
                BlockChunk::Numbers(expected_block_numbers) => {
                    assert!(matches!(block_chunk, BlockChunk::Numbers { .. }));
                    let BlockChunk::Numbers(block_numbers) = block_chunk else {
                        panic!("Unexpected shape")
                    };
                    if expected_block_numbers != block_numbers {
                        return false;
                    }
                }
                BlockChunk::Range(expected_range_start, expected_range_end) => {
                    assert!(matches!(block_chunk, BlockChunk::Range { .. }));
                    let BlockChunk::Range(range_start, range_end) = block_chunk else {
                        panic!("Unexpected shape")
                    };
                    if expected_range_start != range_start || expected_range_end != range_end {
                        return false;
                    }
                }
            }
        }
        true
    }

    #[derive(Clone, Debug)]
    enum BlockNumberTest<'a> {
        WithoutMock((&'a str, RangePosition, u64)),
        WithMock((&'a str, RangePosition, u64, u64)),
    }

    async fn block_number_test_helper(
        tests: Vec<(BlockNumberTest<'_>, bool)>,
        mock_ipc_path: PathBuf,
    ) {
        let provider = ProviderBuilder::new()
            .connect_ipc(IpcConnect::new(mock_ipc_path))
            .await
            .unwrap()
            .erased();
        let source = Source {
            provider,
            semaphore: Arc::new(None),
            rate_limiter: Arc::new(None),
            chain_id: 1,
            inner_request_size: 1,
            max_concurrent_chunks: Some(1),
            rpc_url: "".to_string(),
            jwt: None,
            labels: cryo_freeze::SourceLabels::default(),
        };
        let source = Arc::new(source);
        for (test, res) in tests {
            match test {
                BlockNumberTest::WithMock((block_ref, range_position, expected, _latest)) => {
                    assert_eq!(
                        block_number_test_executor(
                            block_ref,
                            range_position,
                            expected,
                            source.clone()
                        )
                        .await,
                        res
                    );
                }
                BlockNumberTest::WithoutMock((block_ref, range_position, expected)) => {
                    assert_eq!(
                        block_number_test_executor(
                            block_ref,
                            range_position,
                            expected,
                            source.clone()
                        )
                        .await,
                        res
                    );
                }
            }
        }
    }

    async fn block_number_test_executor(
        block_ref: &str,
        range_position: RangePosition,
        expected: u64,
        source: Arc<Source>,
    ) -> bool {
        let block_number = parse_block_number(block_ref, range_position, source).await.unwrap();
        block_number == expected
    }

    #[tokio::test]
    async fn block_token_parsing() {
        // Ranges
        let tests: Vec<(BlockTokenTest<'_>, bool)> = vec![
            // Range Type
            (BlockTokenTest::WithoutMock((r"1:2", BlockChunk::Range(1, 1))), true), /* Single block range */
            (BlockTokenTest::WithoutMock((r"0:2", BlockChunk::Range(0, 1))), true), /* Implicit start */
            (BlockTokenTest::WithoutMock((r"-10:100", BlockChunk::Range(91, 100))), true), /* Relative negative */
            (BlockTokenTest::WithoutMock((r"10:+100", BlockChunk::Range(10, 109))), true), /* Relative positive */
            (BlockTokenTest::WithMock((r"1:latest", BlockChunk::Range(1, 12), 12)), true), /* Explicit latest */
            (BlockTokenTest::WithMock((r"1:", BlockChunk::Range(1, 12), 12)), true), /* Implicit latest */
            // Number type
            (BlockTokenTest::WithoutMock((r"1", BlockChunk::Numbers(vec![1]))), true), /* Single block */
        ];
        let mut mock_server = MockIpcServer::new();
        let mock_ipc_path = mock_server.path().clone();
        for (test, _) in tests.clone().into_iter() {
            match test {
                BlockTokenTest::WithoutMock(_) => {}
                BlockTokenTest::WithMock((_, _, mock_response)) => {
                    mock_server.add_reply(mock_response)
                }
            }
        }
        mock_server.spawn().await;
        block_token_test_helper(tests, mock_ipc_path).await;
    }

    #[tokio::test]
    async fn block_inputs_parsing() {
        // Ranges
        let block_inputs_single = String::from(r"1:2");
        let block_inputs_multiple = String::from(r"1 2");
        let block_inputs_latest = String::from(r"1:latest");
        let block_inputs_multiple_complex = String::from(r"15M:+1 1000:1002 -3:1b 2000");
        let tests: Vec<(BlockInputTest<'_>, bool)> = vec![
            // Range Type
            (
                BlockInputTest::WithoutMock((&block_inputs_single, vec![BlockChunk::Range(1, 1)])),
                true,
            ), // Single input
            (
                BlockInputTest::WithoutMock((
                    &block_inputs_multiple,
                    vec![BlockChunk::Numbers(vec![1]), BlockChunk::Numbers(vec![2])],
                )),
                true,
            ), // Multi input
            (
                BlockInputTest::WithMock((
                    &block_inputs_latest,
                    vec![BlockChunk::Range(1, 12)],
                    12,
                )),
                true,
            ), // Single input latest
            (
                BlockInputTest::WithoutMock((
                    &block_inputs_multiple_complex,
                    vec![
                        BlockChunk::Numbers(vec![15000000]),
                        BlockChunk::Numbers(vec![1000, 1001]),
                        BlockChunk::Numbers(vec![999999998, 999999999, 1000000000]),
                        BlockChunk::Numbers(vec![2000]),
                    ],
                )),
                true,
            ), // Multi input complex
        ];
        let mut mock_server = MockIpcServer::new();
        let mock_ipc_path = mock_server.path().clone();
        for (test, _) in tests.clone() {
            match test {
                BlockInputTest::WithMock((_, _, expected)) => mock_server.add_reply(expected),
                BlockInputTest::WithoutMock(_) => {}
            }
        }
        mock_server.spawn().await;
        block_input_test_helper(tests, mock_ipc_path).await;
    }

    #[tokio::test]
    async fn block_number_parsing() {
        // Ranges
        let tests: Vec<(BlockNumberTest<'_>, bool)> = vec![
            (BlockNumberTest::WithoutMock((r"1", RangePosition::None, 1)), true), // Integer
            (BlockNumberTest::WithMock((r"latest", RangePosition::None, 12, 12)), true), /* Latest block */
            (BlockNumberTest::WithoutMock((r"", RangePosition::First, 0)), true), // First block
            (BlockNumberTest::WithMock((r"", RangePosition::Last, 12, 12)), true), // Last block
            (BlockNumberTest::WithoutMock((r"1B", RangePosition::None, 1000000000)), true), // B
            (BlockNumberTest::WithoutMock((r"1M", RangePosition::None, 1000000)), true), // M
            (BlockNumberTest::WithoutMock((r"1K", RangePosition::None, 1000)), true), // K
            (BlockNumberTest::WithoutMock((r"1b", RangePosition::None, 1000000000)), true), // b
            (BlockNumberTest::WithoutMock((r"1m", RangePosition::None, 1000000)), true), // m
            (BlockNumberTest::WithoutMock((r"1k", RangePosition::None, 1000)), true), // k
        ];
        let mut mock_server = MockIpcServer::new();
        let mock_ipc_path = mock_server.path().clone();
        for (test, _) in tests.clone() {
            match test {
                BlockNumberTest::WithMock((_, _, _, expected)) => mock_server.add_reply(expected),
                BlockNumberTest::WithoutMock(_) => {}
            }
        }
        mock_server.spawn().await;
        block_number_test_helper(tests, mock_ipc_path).await;
    }
}
