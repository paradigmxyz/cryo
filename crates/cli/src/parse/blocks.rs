use std::sync::Arc;

use ethers::prelude::*;

use cryo_freeze::{BlockChunk, Chunk, ChunkData, ParseError, Subchunk};

use crate::args::Args;

pub(crate) async fn parse_blocks(
    args: &Args,
    provider: Arc<Provider<Http>>,
) -> Result<Vec<Chunk>, ParseError> {
    let block_chunks = parse_block_inputs(&args.blocks, &provider).await?;
    let block_chunks = if args.align {
        block_chunks.into_iter().filter_map(|x| x.align(args.chunk_size)).collect()
    } else {
        block_chunks
    };
    let block_chunks = match args.n_chunks {
        Some(n_chunks) => block_chunks.subchunk_by_count(&n_chunks),
        None => block_chunks.subchunk_by_size(&args.chunk_size),
    };
    let block_chunks = apply_reorg_buffer(block_chunks, args.reorg_buffer, &provider).await?;
    let chunks: Vec<Chunk> = block_chunks.iter().map(|x| Chunk::Block(x.clone())).collect();
    Ok(chunks)
}

/// parse block numbers to freeze
async fn parse_block_inputs<P>(
    inputs: &Vec<String>,
    provider: &Provider<P>,
) -> Result<Vec<BlockChunk>, ParseError>
where
    P: JsonRpcClient,
{
    match inputs.len() {
        1 => {
            let first_input = inputs.get(0).ok_or_else(|| {
                ParseError::ParseError("Failed to get the first input".to_string())
            })?;
            parse_block_token(first_input, true, provider).await.map(|x| vec![x])
        }
        _ => {
            let mut chunks = Vec::new();
            for input in inputs {
                chunks.push(parse_block_token(input, false, provider).await?);
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

async fn parse_block_token<P>(
    s: &str,
    as_range: bool,
    provider: &Provider<P>,
) -> Result<BlockChunk, ParseError>
where
    P: JsonRpcClient,
{
    let s = s.replace('_', "");

    let parts: Vec<&str> = s.split(':').collect();
    match parts.as_slice() {
        [block_ref] => {
            let block = parse_block_number(block_ref, RangePosition::None, provider).await?;
            Ok(BlockChunk::Numbers(vec![block]))
        }
        [first_ref, second_ref] => {
            let (start_block, end_block) = match (first_ref, second_ref) {
                _ if first_ref.starts_with('-') => {
                    let end_block =
                        parse_block_number(second_ref, RangePosition::Last, provider).await?;
                    let start_block = end_block
                        .checked_sub(first_ref[1..].parse::<u64>().map_err(|_e| {
                            ParseError::ParseError("start_block parse error".to_string())
                        })?)
                        .ok_or_else(|| {
                            ParseError::ParseError("start_block underflow".to_string())
                        })?;
                    (start_block, end_block)
                }
                _ if second_ref.starts_with('+') => {
                    let start_block =
                        parse_block_number(first_ref, RangePosition::First, provider).await?;
                    let end_block = start_block
                        .checked_add(second_ref[1..].parse::<u64>().map_err(|_e| {
                            ParseError::ParseError("start_block parse error".to_string())
                        })?)
                        .ok_or_else(|| ParseError::ParseError("end_block underflow".to_string()))?;
                    (start_block, end_block)
                }
                _ => {
                    let start_block =
                        parse_block_number(first_ref, RangePosition::First, provider).await?;
                    let end_block =
                        parse_block_number(second_ref, RangePosition::Last, provider).await?;
                    (start_block, end_block)
                }
            };

            if end_block <= start_block {
                Err(ParseError::ParseError(
                    "end_block should not be less than start_block".to_string(),
                ))
            } else if as_range {
                Ok(BlockChunk::Range(start_block, end_block))
            } else {
                Ok(BlockChunk::Numbers((start_block..=end_block).collect()))
            }
        }
        _ => Err(ParseError::ParseError(
            "blocks must be in format block_number or start_block:end_block".to_string(),
        )),
    }
}

async fn parse_block_number<P>(
    block_ref: &str,
    range_position: RangePosition,
    provider: &Provider<P>,
) -> Result<u64, ParseError>
where
    P: JsonRpcClient,
{
    match (block_ref, range_position) {
        ("latest", _) => provider.get_block_number().await.map(|n| n.as_u64()).map_err(|_e| {
            ParseError::ParseError("Error retrieving latest block number".to_string())
        }),
        ("", RangePosition::First) => Ok(0),
        ("", RangePosition::Last) => {
            provider.get_block_number().await.map(|n| n.as_u64()).map_err(|_e| {
                ParseError::ParseError("Error retrieving last block number".to_string())
            })
        }
        ("", RangePosition::None) => Err(ParseError::ParseError("invalid input".to_string())),
        _ if block_ref.ends_with('B') | block_ref.ends_with('b') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e9 * n) as u64)
                .map_err(|_e| ParseError::ParseError("Error parsing block ref".to_string()))
        }
        _ if block_ref.ends_with('M') | block_ref.ends_with('m') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e6 * n) as u64)
                .map_err(|_e| ParseError::ParseError("Error parsing block ref".to_string()))
        }
        _ if block_ref.ends_with('K') | block_ref.ends_with('k') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e3 * n) as u64)
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
    provider: &Provider<Http>,
) -> Result<Vec<BlockChunk>, ParseError> {
    match reorg_filter {
        0 => Ok(block_chunks),
        reorg_filter => {
            let latest_block = match provider.get_block_number().await {
                Ok(result) => result.as_u64(),
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

#[cfg(test)]
mod tests {
    use super::*;

    enum BlockTokenTest<'a> {
        WithoutMock((&'a str, BlockChunk)),   // Token | Expected
        WithMock((&'a str, BlockChunk, u64)), // Token | Expected | Mock Block Response
    }

    async fn block_token_test_helper(tests: Vec<(BlockTokenTest<'_>, bool)>) {
        let (provider, mock) = Provider::mocked();
        for (test, res) in tests {
            match test {
                BlockTokenTest::WithMock((token, expected, latest)) => {
                    mock.push(U64::from(latest)).unwrap();
                    assert_eq!(block_token_test_executor(&token, expected, &provider).await, res);
                }
                BlockTokenTest::WithoutMock((token, expected)) => {
                    assert_eq!(block_token_test_executor(&token, expected, &provider).await, res);
                }
            }
        }
    }

    async fn block_token_test_executor<P>(
        token: &str,
        expected: BlockChunk,
        provider: &Provider<P>,
    ) -> bool
    where
        P: JsonRpcClient,
    {
        match expected {
            BlockChunk::Numbers(expected_block_numbers) => {
                let block_chunks = parse_block_token(token, false, &provider).await.unwrap();
                assert!(matches!(block_chunks, BlockChunk::Numbers { .. }));
                let BlockChunk::Numbers(block_numbers) = block_chunks else {
                    panic!("Unexpected shape")
                };
                return block_numbers == expected_block_numbers
            }
            BlockChunk::Range(expected_range_start, expected_range_end) => {
                let block_chunks = parse_block_token(token, true, &provider).await.unwrap();
                assert!(matches!(block_chunks, BlockChunk::Range { .. }));
                let BlockChunk::Range(range_start, range_end) = block_chunks else {
                    panic!("Unexpected shape")
                };
                return expected_range_start == range_start && expected_range_end == range_end
            }
        }
    }

    enum BlockInputTest<'a> {
        WithoutMock((&'a Vec<String>, Vec<BlockChunk>)), // Token | Expected
        WithMock((&'a Vec<String>, Vec<BlockChunk>, u64)), // Token | Expected | Mock Block Response
    }

    async fn block_input_test_helper(tests: Vec<(BlockInputTest<'_>, bool)>) {
        let (provider, mock) = Provider::mocked();
        for (test, res) in tests {
            match test {
                BlockInputTest::WithMock((inputs, expected, latest)) => {
                    mock.push(U64::from(latest)).unwrap();
                    assert_eq!(block_input_test_executor(&inputs, expected, &provider).await, res);
                }
                BlockInputTest::WithoutMock((inputs, expected)) => {
                    assert_eq!(block_input_test_executor(&inputs, expected, &provider).await, res);
                }
            }
        }
    }

    async fn block_input_test_executor<P>(
        inputs: &Vec<String>,
        expected: Vec<BlockChunk>,
        provider: &Provider<P>,
    ) -> bool
    where
        P: JsonRpcClient,
    {
        let block_chunks = parse_block_inputs(inputs, &provider).await.unwrap();
        assert_eq!(block_chunks.len(), expected.len());
        for (i, block_chunk) in block_chunks.iter().enumerate() {
            let expected_chunk = &expected[i];
            match expected_chunk {
                BlockChunk::Numbers(expected_block_numbers) => {
                    assert!(matches!(block_chunk, BlockChunk::Numbers { .. }));
                    let BlockChunk::Numbers(block_numbers) = block_chunk else {
                        panic!("Unexpected shape")
                    };
                    if expected_block_numbers != block_numbers {
                        return false
                    }
                }
                BlockChunk::Range(expected_range_start, expected_range_end) => {
                    assert!(matches!(block_chunk, BlockChunk::Range { .. }));
                    let BlockChunk::Range(range_start, range_end) = block_chunk else {
                        panic!("Unexpected shape")
                    };
                    if expected_range_start != range_start || expected_range_end != range_end {
                        return false
                    }
                }
            }
        }
        return true
    }

    enum BlockNumberTest<'a> {
        WithoutMock((&'a str, RangePosition, u64)),
        WithMock((&'a str, RangePosition, u64, u64)),
    }

    async fn block_number_test_helper(tests: Vec<(BlockNumberTest<'_>, bool)>) {
        let (provider, mock) = Provider::mocked();
        for (test, res) in tests {
            match test {
                BlockNumberTest::WithMock((block_ref, range_position, expected, latest)) => {
                    mock.push(U64::from(latest)).unwrap();
                    assert_eq!(
                        block_number_test_executor(&block_ref, range_position, expected, &provider)
                            .await,
                        res
                    );
                }
                BlockNumberTest::WithoutMock((block_ref, range_position, expected)) => {
                    assert_eq!(
                        block_number_test_executor(&block_ref, range_position, expected, &provider)
                            .await,
                        res
                    );
                }
            }
        }
    }

    async fn block_number_test_executor<P>(
        block_ref: &str,
        range_position: RangePosition,
        expected: u64,
        provider: &Provider<P>,
    ) -> bool
    where
        P: JsonRpcClient,
    {
        let block_number = parse_block_number(block_ref, range_position, &provider).await.unwrap();
        return block_number == expected
    }

    #[tokio::test]
    async fn block_token_parsing() {
        // Ranges
        let tests: Vec<(BlockTokenTest<'_>, bool)> = vec![
            // Range Type
            (BlockTokenTest::WithoutMock((r"1:2", BlockChunk::Range(1, 2))), true), /* Single block range */
            (BlockTokenTest::WithoutMock((r"0:2", BlockChunk::Range(0, 2))), true), /* Implicit start */
            (BlockTokenTest::WithoutMock((r"-10:100", BlockChunk::Range(90, 100))), true), /* Relative negative */
            (BlockTokenTest::WithoutMock((r"10:+100", BlockChunk::Range(10, 110))), true), /* Relative positive */
            (BlockTokenTest::WithMock((r"1:latest", BlockChunk::Range(1, 12), 12)), true), /* Explicit latest */
            (BlockTokenTest::WithMock((r"1:", BlockChunk::Range(1, 12), 12)), true), /* Implicit latest */
            // Number type
            (BlockTokenTest::WithoutMock((r"1", BlockChunk::Numbers(vec![1]))), true), /* Single block */
        ];
        block_token_test_helper(tests).await;
    }

    #[tokio::test]
    async fn block_inputs_parsing() {
        // Ranges
        let block_inputs_single = vec![String::from(r"1:2")];
        let block_inputs_multiple = vec![String::from(r"1"), String::from(r"2")];
        let block_inputs_latest = vec![String::from(r"1:latest")];
        let tests: Vec<(BlockInputTest<'_>, bool)> = vec![
            // Range Type
            (
                BlockInputTest::WithoutMock((&block_inputs_single, vec![BlockChunk::Range(1, 2)])),
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
        ];
        block_input_test_helper(tests).await;
    }

    #[tokio::test]
    async fn block_number_parsing() {
        // Ranges
        let tests: Vec<(BlockNumberTest<'_>, bool)> = vec![
            (BlockNumberTest::WithoutMock((r"1", RangePosition::None, 1)), true), // Integer
            (BlockNumberTest::WithMock((r"latest", RangePosition::None, 12, 12)), true), /* Lastest block */
            (BlockNumberTest::WithoutMock((r"", RangePosition::First, 0)), true), // First block
            (BlockNumberTest::WithMock((r"", RangePosition::Last, 12, 12)), true), // Last block
            (BlockNumberTest::WithoutMock((r"1B", RangePosition::None, 1000000000)), true), // B
            (BlockNumberTest::WithoutMock((r"1M", RangePosition::None, 1000000)), true), // M
            (BlockNumberTest::WithoutMock((r"1K", RangePosition::None, 1000)), true), // K
            (BlockNumberTest::WithoutMock((r"1b", RangePosition::None, 1000000000)), true), // b
            (BlockNumberTest::WithoutMock((r"1m", RangePosition::None, 1000000)), true), // m
            (BlockNumberTest::WithoutMock((r"1k", RangePosition::None, 1000)), true), // k
        ];
        block_number_test_helper(tests).await;
    }
}
