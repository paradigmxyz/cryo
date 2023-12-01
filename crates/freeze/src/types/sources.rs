use std::sync::Arc;

use ethers::prelude::*;
use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{direct::NotKeyed, InMemoryState},
};
use tokio::{
    sync::{AcquireError, Semaphore, SemaphorePermit},
    task,
};

use crate::CollectError;

/// RateLimiter based on governor crate
pub type RateLimiter = governor::RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

/// Options for fetching data from node
#[derive(Clone, Debug)]
pub struct Source {
    /// Shared provider for rpc data
    pub fetcher: Arc<Fetcher<RetryClient<Http>>>,
    /// chain_id of network
    pub chain_id: u64,
    /// number of blocks per log request
    pub inner_request_size: u64,
    /// Maximum chunks collected concurrently
    pub max_concurrent_chunks: Option<u64>,
    /// Rpc Url
    pub rpc_url: String,
    /// Labels (these are non-functional)
    pub labels: SourceLabels,
}

impl Source {
    /// Returns all receipts for a block.
    /// Tries to use `eth_getBlockReceipts` first, and falls back to `eth_getTransactionReceipt`
    pub async fn get_tx_receipts_in_block(
        &self,
        block: &Block<Transaction>,
    ) -> Result<Vec<TransactionReceipt>> {
        let block_number =
            block.number.ok_or(CollectError::CollectError("no block number".to_string()))?.as_u64();
        if let Ok(receipts) = self.fetcher.get_block_receipts(block_number).await {
            return Ok(receipts);
        }

        self.get_tx_receipts(&block.transactions).await
    }

    /// Returns all receipts for vector of transactions using `eth_getTransactionReceipt`
    pub async fn get_tx_receipts(
        &self,
        transactions: &Vec<Transaction>,
    ) -> Result<Vec<TransactionReceipt>> {
        let mut tasks = Vec::new();
        for tx in transactions {
            let tx_hash = tx.hash;
            let fetcher = self.fetcher.clone();
            let task = task::spawn(async move {
                match fetcher.get_transaction_receipt(tx_hash).await? {
                    Some(receipt) => Ok(receipt),
                    None => {
                        Err(CollectError::CollectError("could not find tx receipt".to_string()))
                    }
                }
            });
            tasks.push(task);
        }
        let mut receipts = Vec::new();
        for task in tasks {
            match task.await {
                Ok(receipt) => receipts.push(receipt?),
                Err(e) => return Err(CollectError::TaskFailed(e)),
            }
        }

        Ok(receipts)
    }
}

const DEFAULT_INNER_REQUEST_SIZE: u64 = 100;
const DEFAULT_MAX_RETRIES: u32 = 5;
const DEFAULT_INTIAL_BACKOFF: u64 = 5;
const DEFAULT_MAX_CONCURRENT_CHUNKS: u64 = 4;
const DEFAULT_MAX_CONCURRENT_REQUESTS: u64 = 100;

/// builder
impl Source {
    /// initialize source
    pub async fn init(rpc_url: Option<String>) -> Result<Source> {
        let rpc_url = parse_rpc_url(rpc_url);
        let provider = Provider::<RetryClient<Http>>::new_client(
            &rpc_url,
            DEFAULT_MAX_RETRIES,
            DEFAULT_INTIAL_BACKOFF,
        )
        .map_err(|_| CollectError::RPCError("could not connect to provider".to_string()))?;
        let chain_id = provider
            .get_chainid()
            .await
            .map_err(|_| CollectError::RPCError("could not get chain_id".to_string()))?
            .as_u64();

        let rate_limiter = None;
        let semaphore = None;
        let fetcher = Fetcher { provider, semaphore, rate_limiter };

        let source = Source {
            fetcher: Arc::new(fetcher),
            chain_id,
            inner_request_size: DEFAULT_INNER_REQUEST_SIZE,
            max_concurrent_chunks: Some(DEFAULT_MAX_CONCURRENT_CHUNKS),
            rpc_url,
            labels: SourceLabels {
                max_concurrent_requests: Some(DEFAULT_MAX_CONCURRENT_REQUESTS),
                max_requests_per_second: Some(0),
                max_retries: Some(DEFAULT_MAX_RETRIES),
                initial_backoff: Some(DEFAULT_INTIAL_BACKOFF),
            },
        };

        Ok(source)
    }

    // /// set rate limit
    // pub fn rate_limit(mut self, _requests_per_second: u64) -> Source {
    //     todo!();
    // }
}

fn parse_rpc_url(rpc_url: Option<String>) -> String {
    let mut url = match rpc_url {
        Some(url) => url.clone(),
        _ => match std::env::var("ETH_RPC_URL") {
            Ok(url) => url,
            Err(_e) => {
                println!("must provide --rpc or set ETH_RPC_URL");
                std::process::exit(0);
            }
        },
    };
    if !url.starts_with("http") {
        url = "http://".to_string() + url.as_str();
    };
    url
}

// builder

// struct SourceBuilder {
//     /// Shared provider for rpc data
//     pub fetcher: Option<Arc<Fetcher<RetryClient<Http>>>>,
//     /// chain_id of network
//     pub chain_id: Option<u64>,
//     /// number of blocks per log request
//     pub inner_request_size: Option<u64>,
//     /// Maximum chunks collected concurrently
//     pub max_concurrent_chunks: Option<u64>,
//     /// Rpc Url
//     pub rpc_url: Option<String>,
//     /// Labels (these are non-functional)
//     pub labels: Option<SourceLabels>,
// }

// impl SourceBuilder {
//     fn new(mut self) -> SourceBuilder {
//     }

//     fn build(self) -> Source {
//     }
// }

/// source labels (non-functional)
#[derive(Clone, Debug)]
pub struct SourceLabels {
    /// Maximum requests collected concurrently
    pub max_concurrent_requests: Option<u64>,
    /// Maximum requests per second
    pub max_requests_per_second: Option<u64>,
    /// Max retries
    pub max_retries: Option<u32>,
    /// Initial backoff
    pub initial_backoff: Option<u64>,
}

/// Wrapper over `Provider<P>` that adds concurrency and rate limiting controls
#[derive(Debug)]
pub struct Fetcher<P> {
    /// provider data source
    pub provider: Provider<P>,
    /// semaphore for controlling concurrency
    pub semaphore: Option<Semaphore>,
    /// rate limiter for controlling request rate
    pub rate_limiter: Option<RateLimiter>,
}

type Result<T> = ::core::result::Result<T, CollectError>;

impl<P: JsonRpcClient> Fetcher<P> {
    /// Returns an array (possibly empty) of logs that match the filter
    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_logs(filter).await)
    }

    /// Replays all transactions in a block returning the requested traces for each transaction
    pub async fn trace_replay_block_transactions(
        &self,
        block: BlockNumber,
        trace_types: Vec<TraceType>,
    ) -> Result<Vec<BlockTrace>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.trace_replay_block_transactions(block, trace_types).await)
    }

    /// Get state diff traces of block
    pub async fn trace_block_state_diffs(
        &self,
        block: u32,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<BlockTrace>)> {
        // get traces
        let result = self
            .trace_replay_block_transactions(
                block.into(),
                vec![ethers::types::TraceType::StateDiff],
            )
            .await?;

        // get transactions
        let txs = if include_transaction_hashes {
            self.get_block(block as u64)
                .await?
                .ok_or(CollectError::CollectError("could not find block".to_string()))?
                .transactions
                .iter()
                .map(|tx| Some(tx.0.to_vec()))
                .collect()
        } else {
            vec![None; result.len()]
        };

        Ok((Some(block), txs, result))
    }

    /// Get VM traces of block
    pub async fn trace_block_vm_traces(
        &self,
        block: u32,
    ) -> Result<(Option<u32>, Option<Vec<u8>>, Vec<BlockTrace>)> {
        let result = self
            .trace_replay_block_transactions(block.into(), vec![ethers::types::TraceType::VmTrace])
            .await;
        Ok((Some(block), None, result?))
    }

    /// Replays a transaction, returning the traces
    pub async fn trace_replay_transaction(
        &self,
        tx_hash: TxHash,
        trace_types: Vec<TraceType>,
    ) -> Result<BlockTrace> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.trace_replay_transaction(tx_hash, trace_types).await)
    }

    /// Get state diff traces of transaction
    pub async fn trace_transaction_state_diffs(
        &self,
        transaction_hash: Vec<u8>,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<BlockTrace>)> {
        let result = self
            .trace_replay_transaction(
                H256::from_slice(&transaction_hash),
                vec![ethers::types::TraceType::StateDiff],
            )
            .await;
        Ok((None, vec![Some(transaction_hash)], vec![result?]))
    }

    /// Get VM traces of transaction
    pub async fn trace_transaction_vm_traces(
        &self,
        transaction_hash: Vec<u8>,
    ) -> Result<(Option<u32>, Option<Vec<u8>>, Vec<BlockTrace>)> {
        let result = self
            .trace_replay_transaction(
                H256::from_slice(&transaction_hash),
                vec![ethers::types::TraceType::VmTrace],
            )
            .await;
        Ok((None, Some(transaction_hash), vec![result?]))
    }

    /// Gets the transaction with transaction_hash
    pub async fn get_transaction(&self, tx_hash: TxHash) -> Result<Option<Transaction>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_transaction(tx_hash).await)
    }

    /// Gets the transaction receipt with transaction_hash
    pub async fn get_transaction_receipt(
        &self,
        tx_hash: TxHash,
    ) -> Result<Option<TransactionReceipt>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_transaction_receipt(tx_hash).await)
    }

    /// Gets the block at `block_num` (transaction hashes only)
    pub async fn get_block(&self, block_num: u64) -> Result<Option<Block<TxHash>>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_block(block_num).await)
    }

    /// Gets the block at `block_num` (transaction hashes only)
    pub async fn get_block_by_hash(&self, block_hash: H256) -> Result<Option<Block<TxHash>>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_block(BlockId::Hash(block_hash)).await)
    }

    /// Gets the block at `block_num` (full transactions included)
    pub async fn get_block_with_txs(&self, block_num: u64) -> Result<Option<Block<Transaction>>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_block_with_txs(block_num).await)
    }

    /// Returns all receipts for a block.
    /// Note that this uses the `eth_getBlockReceipts` method which is not supported by all nodes.
    /// Consider using `FetcherExt::get_tx_receipts_in_block` which takes a block, and falls back to
    /// `eth_getTransactionReceipt` if `eth_getBlockReceipts` is not supported.
    pub async fn get_block_receipts(&self, block_num: u64) -> Result<Vec<TransactionReceipt>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_block_receipts(block_num).await)
    }

    /// Returns traces created at given block
    pub async fn trace_block(&self, block_num: BlockNumber) -> Result<Vec<Trace>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.trace_block(block_num).await)
    }

    /// Returns all traces of a given transaction
    pub async fn trace_transaction(&self, tx_hash: TxHash) -> Result<Vec<Trace>> {
        let _permit = self.permit_request().await;
        self.provider.trace_transaction(tx_hash).await.map_err(CollectError::ProviderError)
    }

    /// Deprecated
    pub async fn call(
        &self,
        transaction: TransactionRequest,
        block_number: BlockNumber,
    ) -> Result<Bytes> {
        let _permit = self.permit_request().await;
        self.provider
            .call(&transaction.into(), Some(block_number.into()))
            .await
            .map_err(CollectError::ProviderError)
    }

    /// Returns traces for given call data
    pub async fn trace_call(
        &self,
        transaction: TransactionRequest,
        trace_type: Vec<TraceType>,
        block_number: Option<BlockNumber>,
    ) -> Result<BlockTrace> {
        let _permit = self.permit_request().await;
        self.provider
            .trace_call(transaction, trace_type, block_number)
            .await
            .map_err(CollectError::ProviderError)
    }

    /// Get nonce of address
    pub async fn get_transaction_count(
        &self,
        address: H160,
        block_number: BlockNumber,
    ) -> Result<U256> {
        let _permit = self.permit_request().await;
        self.provider
            .get_transaction_count(address, Some(block_number.into()))
            .await
            .map_err(CollectError::ProviderError)
    }

    /// Get code at address
    pub async fn get_balance(&self, address: H160, block_number: BlockNumber) -> Result<U256> {
        let _permit = self.permit_request().await;
        self.provider
            .get_balance(address, Some(block_number.into()))
            .await
            .map_err(CollectError::ProviderError)
    }

    /// Get code at address
    pub async fn get_code(&self, address: H160, block_number: BlockNumber) -> Result<Bytes> {
        let _permit = self.permit_request().await;
        self.provider
            .get_code(address, Some(block_number.into()))
            .await
            .map_err(CollectError::ProviderError)
    }

    /// Get stored data at given location
    pub async fn get_storage_at(
        &self,
        address: H160,
        slot: H256,
        block_number: BlockNumber,
    ) -> Result<H256> {
        let _permit = self.permit_request().await;
        self.provider
            .get_storage_at(address, slot, Some(block_number.into()))
            .await
            .map_err(CollectError::ProviderError)
    }

    /// Get the block number
    pub async fn get_block_number(&self) -> Result<U64> {
        Self::map_err(self.provider.get_block_number().await)
    }

    // extra helpers below

    /// block number of transaction
    pub async fn get_transaction_block_number(&self, transaction_hash: Vec<u8>) -> Result<u32> {
        let block = self.get_transaction(H256::from_slice(&transaction_hash)).await?;
        let block = block.ok_or(CollectError::CollectError("could not get block".to_string()))?;
        Ok(block
            .block_number
            .ok_or(CollectError::CollectError("could not get block number".to_string()))?
            .as_u32())
    }

    /// block number of transaction
    pub async fn get_transaction_logs(&self, transaction_hash: Vec<u8>) -> Result<Vec<Log>> {
        Ok(self
            .get_transaction_receipt(H256::from_slice(&transaction_hash))
            .await?
            .ok_or(CollectError::CollectError("transaction receipt not found".to_string()))?
            .logs)
    }

    /// Return output data of a contract call
    pub async fn call2(
        &self,
        address: H160,
        call_data: Vec<u8>,
        block_number: BlockNumber,
    ) -> Result<Bytes> {
        let transaction = TransactionRequest {
            to: Some(address.into()),
            data: Some(call_data.into()),
            ..Default::default()
        };
        let _permit = self.permit_request().await;
        self.provider
            .call(&transaction.into(), Some(block_number.into()))
            .await
            .map_err(CollectError::ProviderError)
    }

    /// Return output data of a contract call
    pub async fn trace_call2(
        &self,
        address: H160,
        call_data: Vec<u8>,
        trace_type: Vec<TraceType>,
        block_number: Option<BlockNumber>,
    ) -> Result<BlockTrace> {
        let transaction = TransactionRequest {
            to: Some(address.into()),
            data: Some(call_data.into()),
            ..Default::default()
        };
        let _permit = self.permit_request().await;
        self.provider
            .trace_call(transaction, trace_type, block_number)
            .await
            .map_err(CollectError::ProviderError)
    }

    /// get geth debug block traces
    pub async fn geth_debug_trace_block(
        &self,
        block_number: u32,
        options: GethDebugTracingOptions,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<GethTrace>)> {
        let traces = {
            let _permit = self.permit_request().await;
            self.provider
                .debug_trace_block_by_number(Some(block_number.into()), options)
                .await
                .map_err(CollectError::ProviderError)?
        };

        let txs = if include_transaction_hashes {
            match self.get_block(block_number as u64).await? {
                Some(block) => {
                    block.transactions.iter().map(|x| Some(x.as_bytes().to_vec())).collect()
                }
                None => {
                    return Err(CollectError::CollectError(
                        "could not get block for txs".to_string(),
                    ))
                }
            }
        } else {
            vec![None; traces.len()]
        };

        Ok((Some(block_number), txs, traces))
    }

    /// get geth debug block call traces
    pub async fn geth_debug_trace_block_javascript_traces(
        &self,
        js_tracer: String,
        block_number: u32,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<serde_json::Value>)> {
        let tracer = GethDebugTracerType::JsTracer(js_tracer);
        let options = GethDebugTracingOptions { tracer: Some(tracer), ..Default::default() };
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::Unknown(value) => calls.push(value),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block opcode traces
    pub async fn geth_debug_trace_block_opcodes(
        &self,
        block_number: u32,
        include_transaction_hashes: bool,
        options: GethDebugTracingOptions,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<DefaultFrame>)> {
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::Known(GethTraceFrame::Default(frame)) => calls.push(frame),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block 4byte traces
    pub async fn geth_debug_trace_block_4byte_traces(
        &self,
        block_number: u32,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<BTreeMap<String, u64>>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::FourByteTracer);
        let options = GethDebugTracingOptions { tracer: Some(tracer), ..Default::default() };
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::Known(GethTraceFrame::FourByteTracer(FourByteFrame(frame))) => {
                    calls.push(frame)
                }
                GethTrace::Known(GethTraceFrame::NoopTracer(_)) => {}
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block call traces
    pub async fn geth_debug_trace_block_prestate(
        &self,
        block_number: u32,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<BTreeMap<H160, AccountState>>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::PreStateTracer);
        let options = GethDebugTracingOptions { tracer: Some(tracer), ..Default::default() };
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::Known(GethTraceFrame::PreStateTracer(PreStateFrame::Default(
                    PreStateMode(frame),
                ))) => calls.push(frame),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block call traces
    pub async fn geth_debug_trace_block_calls(
        &self,
        block_number: u32,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<CallFrame>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::CallTracer);
        let config = GethDebugTracerConfig::BuiltInTracer(
            GethDebugBuiltInTracerConfig::CallTracer(CallConfig { ..Default::default() }),
        );
        let options = GethDebugTracingOptions {
            tracer: Some(tracer),
            tracer_config: Some(config),
            ..Default::default()
        };
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::Known(GethTraceFrame::CallTracer(call_frame)) => calls.push(call_frame),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block diff traces
    pub async fn geth_debug_trace_block_diffs(
        &self,
        block_number: u32,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<DiffMode>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::PreStateTracer);
        let config = GethDebugTracerConfig::BuiltInTracer(
            GethDebugBuiltInTracerConfig::PreStateTracer(PreStateConfig { diff_mode: Some(true) }),
        );
        let options = GethDebugTracingOptions {
            tracer: Some(tracer),
            tracer_config: Some(config),
            ..Default::default()
        };
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut diffs = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::Known(GethTraceFrame::PreStateTracer(PreStateFrame::Diff(diff))) => {
                    diffs.push(diff)
                }
                GethTrace::Unknown(ethers::utils::__serde_json::Value::Object(map)) => {
                    let diff = parse_geth_diff_object(map)?;
                    diffs.push(diff)
                }
                _ => {
                    println!("{:?}", trace);
                    return Err(CollectError::CollectError("invalid trace result".to_string()));
                }
            }
        }
        Ok((block, txs, diffs))
    }

    /// get geth debug transaction traces
    pub async fn geth_debug_trace_transaction(
        &self,
        transaction_hash: Vec<u8>,
        options: GethDebugTracingOptions,
        include_block_number: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<GethTrace>)> {
        let ethers_tx = H256::from_slice(&transaction_hash);

        let trace = {
            let _permit = self.permit_request().await;
            self.provider
                .debug_trace_transaction(ethers_tx, options)
                .await
                .map_err(CollectError::ProviderError)?
        };
        let traces = vec![trace];

        let block_number = if include_block_number {
            match self.get_transaction(ethers_tx).await? {
                Some(tx) => tx.block_number.map(|x| x.as_u32()),
                None => {
                    return Err(CollectError::CollectError(
                        "could not get block for txs".to_string(),
                    ))
                }
            }
        } else {
            None
        };

        Ok((block_number, vec![Some(transaction_hash)], traces))
    }

    /// get geth debug block javascript traces
    pub async fn geth_debug_trace_transaction_javascript_traces(
        &self,
        js_tracer: String,
        transaction_hash: Vec<u8>,
        include_block_number: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<serde_json::Value>)> {
        let tracer = GethDebugTracerType::JsTracer(js_tracer);
        let options = GethDebugTracingOptions { tracer: Some(tracer), ..Default::default() };
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_block_number)
            .await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::Unknown(value) => calls.push(value),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block opcode traces
    pub async fn geth_debug_trace_transaction_opcodes(
        &self,
        transaction_hash: Vec<u8>,
        include_block_number: bool,
        options: GethDebugTracingOptions,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<DefaultFrame>)> {
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_block_number)
            .await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::Known(GethTraceFrame::Default(frame)) => calls.push(frame),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block 4byte traces
    pub async fn geth_debug_trace_transaction_4byte_traces(
        &self,
        transaction_hash: Vec<u8>,
        include_block_number: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<BTreeMap<String, u64>>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::FourByteTracer);
        let options = GethDebugTracingOptions { tracer: Some(tracer), ..Default::default() };
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_block_number)
            .await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::Known(GethTraceFrame::FourByteTracer(FourByteFrame(frame))) => {
                    calls.push(frame)
                }
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block call traces
    pub async fn geth_debug_trace_transaction_prestate(
        &self,
        transaction_hash: Vec<u8>,
        include_block_number: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<BTreeMap<H160, AccountState>>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::PreStateTracer);
        let options = GethDebugTracingOptions { tracer: Some(tracer), ..Default::default() };
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_block_number)
            .await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::Known(GethTraceFrame::PreStateTracer(PreStateFrame::Default(
                    PreStateMode(frame),
                ))) => calls.push(frame),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block call traces
    pub async fn geth_debug_trace_transaction_calls(
        &self,
        transaction_hash: Vec<u8>,
        include_block_number: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<CallFrame>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::CallTracer);
        let config = GethDebugTracerConfig::BuiltInTracer(
            GethDebugBuiltInTracerConfig::CallTracer(CallConfig { ..Default::default() }),
        );
        let options = GethDebugTracingOptions {
            tracer: Some(tracer),
            tracer_config: Some(config),
            ..Default::default()
        };
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_block_number)
            .await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::Known(GethTraceFrame::CallTracer(call_frame)) => calls.push(call_frame),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block diff traces
    pub async fn geth_debug_trace_transaction_diffs(
        &self,
        transaction_hash: Vec<u8>,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<DiffMode>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::PreStateTracer);
        let config = GethDebugTracerConfig::BuiltInTracer(
            GethDebugBuiltInTracerConfig::PreStateTracer(PreStateConfig { diff_mode: Some(true) }),
        );
        let options = GethDebugTracingOptions {
            tracer: Some(tracer),
            tracer_config: Some(config),
            ..Default::default()
        };
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_transaction_hashes)
            .await?;

        let mut diffs = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::Known(GethTraceFrame::PreStateTracer(PreStateFrame::Diff(diff))) => {
                    diffs.push(diff)
                }
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, diffs))
    }

    async fn permit_request(
        &self,
    ) -> Option<::core::result::Result<SemaphorePermit<'_>, AcquireError>> {
        let permit = match &self.semaphore {
            Some(semaphore) => Some(semaphore.acquire().await),
            _ => None,
        };
        if let Some(limiter) = &self.rate_limiter {
            limiter.until_ready().await;
        }
        permit
    }

    fn map_err<T>(res: ::core::result::Result<T, ProviderError>) -> Result<T> {
        res.map_err(CollectError::ProviderError)
    }
}

use crate::err;
use std::collections::BTreeMap;

fn parse_geth_diff_object(
    map: ethers::utils::__serde_json::Map<String, ethers::utils::__serde_json::Value>,
) -> Result<DiffMode> {
    println!("HERE {:?}", map);
    let pre: BTreeMap<H160, AccountState> = serde_json::from_value(map["pre"].clone())
        .map_err(|_| err("cannot deserialize pre diff"))?;
    let post: BTreeMap<H160, AccountState> = serde_json::from_value(map["post"].clone())
        .map_err(|_| err("cannot deserialize pre diff"))?;

    Ok(DiffMode { pre, post })
}
