use std::sync::Arc;

use ethers::prelude::*;
use futures::future::join_all;
use tokio::sync::Semaphore;

use crate::chunks::ChunkAgg;
use crate::types::BlockChunk;
use crate::types::CollectError;

pub async fn fetch_state_diffs(
    block_chunk: &BlockChunk,
    provider: &Provider<Http>,
    max_concurrent_blocks: &u64,
) -> Result<Vec<BlockTrace>, CollectError> {
    fetch_block_traces(
        block_chunk,
        provider,
        max_concurrent_blocks,
        &[TraceType::StateDiff],
    )
    .await
}

pub async fn fetch_vm_traces(
    block_chunk: &BlockChunk,
    provider: &Provider<Http>,
    max_concurrent_blocks: &u64,
) -> Result<Vec<BlockTrace>, CollectError> {
    fetch_block_traces(
        block_chunk,
        provider,
        max_concurrent_blocks,
        &[TraceType::VmTrace],
    )
    .await
}

async fn fetch_block_traces(
    block_chunk: &BlockChunk,
    provider: &Provider<Http>,
    max_concurrent_blocks: &u64,
    trace_types: &[TraceType],
) -> Result<Vec<BlockTrace>, CollectError> {
    let semaphore = Arc::new(Semaphore::new(*max_concurrent_blocks as usize));

    let block_numbers = block_chunk.numbers();
    let futures = block_numbers.into_iter().map(|block_number| {
        let provider = provider.clone();
        let semaphore = Arc::clone(&semaphore);
        let trace_types = trace_types.to_vec();
        tokio::spawn(async move {
            let _permit = Arc::clone(&semaphore).acquire_owned().await;
            provider
                .trace_replay_block_transactions(
                    BlockNumber::Number(block_number.into()),
                    trace_types,
                )
                .await
        })
    });

    let results: Vec<_> = join_all(futures)
        .await
        .into_iter()
        .map(|res| res.map_err(CollectError::TaskFailed))
        .collect();

    let mut traces: Vec<BlockTrace> = Vec::new();
    for result in results {
        let block_traces = result?.map_err(CollectError::ProviderError)?;
        traces.extend(block_traces);
    }

    Ok(traces)
}
