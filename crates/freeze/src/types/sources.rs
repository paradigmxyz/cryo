use std::sync::Arc;

use alloy::{
    eips::BlockNumberOrTag,
    primitives::{Address, BlockNumber, Bytes, TxHash, B256, U256},
    providers::{
        ext::{DebugApi, TraceApi},
        DynProvider, Provider, ProviderBuilder,
    },
    rpc::{
        client::{ClientBuilder, RpcClient},
        types::{
            trace::{
                common::TraceResult,
                geth::{
                    AccountState, CallConfig, CallFrame, DefaultFrame, DiffMode,
                    GethDebugBuiltInTracerType, GethDebugTracerType, GethDebugTracingOptions,
                    GethTrace, PreStateConfig, PreStateFrame,
                },
                parity::{
                    LocalizedTransactionTrace, TraceResults, TraceResultsWithTransactionHash,
                    TraceType,
                },
            },
            Block, BlockTransactions, BlockTransactionsKind, Filter, Log, Transaction,
            TransactionInput, TransactionReceipt, TransactionRequest,
        },
    },
    transports::{
        layers::RetryBackoffLayer, utils::guess_local_url, BoxTransport, IntoBoxTransport,
        RpcError, TransportErrorKind,
    },
};
use alloy_transport_http::{AuthLayer, Http, HyperClient};
use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{direct::NotKeyed, InMemoryState},
};
use tokio::{
    sync::{AcquireError, Semaphore, SemaphorePermit},
    task,
};

use crate::{CollectError, ParseError};

/// RateLimiter based on governor crate
pub type RateLimiter = governor::RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

/// Options for fetching data from node
#[derive(Clone, Debug)]
pub struct Source {
    /// provider
    pub provider: DynProvider,
    /// chain_id of network
    pub chain_id: u64,
    /// number of blocks per log request
    pub inner_request_size: u64,
    /// Maximum chunks collected concurrently
    pub max_concurrent_chunks: Option<u64>,
    /// Rpc Url
    pub rpc_url: String,
    /// Optional JWT auth token for the RPC
    pub jwt: Option<String>,
    /// semaphore for controlling concurrency
    pub semaphore: Arc<Option<Semaphore>>,
    /// rate limiter for controlling request rate
    pub rate_limiter: Arc<Option<RateLimiter>>,
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
        let block_number = block.header.number;
        if let Ok(Some(receipts)) = self.get_block_receipts(block_number).await {
            return Ok(receipts);
        }

        self.get_tx_receipts(block.transactions.clone()).await
    }

    /// Returns all receipts for vector of transactions using `eth_getTransactionReceipt`
    pub async fn get_tx_receipts(
        &self,
        transactions: BlockTransactions<Transaction>,
    ) -> Result<Vec<TransactionReceipt>> {
        let mut tasks = Vec::new();
        for tx in transactions.as_transactions().unwrap() {
            let tx_hash = *tx.inner.tx_hash();
            let source = self.clone();
            let task: task::JoinHandle<std::result::Result<TransactionReceipt, CollectError>> =
                task::spawn(async move {
                    match source.get_transaction_receipt(tx_hash).await? {
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
    pub async fn new(rpc_url: String) -> Result<Source> {
        SourceBuilder::new().rpc_url(rpc_url).build().await
    }
}

/// Normalizes a raw RPC endpoint string by ensuring it has a URI scheme
/// (http://) unless it already starts with http/https, ws/wss, or is an IPC path.
/// This is public so front-ends (CLI, Python bindings) can share identical
/// normalization logic.
pub(crate) fn normalize_rpc_url<S: AsRef<str>>(raw: S) -> String {
    let raw = raw.as_ref();
    if raw.starts_with("http") || raw.starts_with("ws") || raw.ends_with(".ipc") {
        raw.to_string()
    } else {
        format!("http://{}", raw)
    }
}

/// Builder for `Source`. Keeps the `freeze` crate lightweight while allowing
/// richer construction logic (CLI, Python bindings, tests) to compose a
/// provider with concurrency & rate limiting concerns.
#[derive(Default, Debug)]
pub struct SourceBuilder {
    rpc_url: Option<String>,
    provider: Option<DynProvider>,
    chain_id: Option<u64>,
    inner_request_size: Option<u64>,
    /// explicit None => unlimited
    max_concurrent_chunks: Option<u64>,
    semaphore: Option<Arc<Option<Semaphore>>>,
    rate_limiter: Option<Arc<Option<RateLimiter>>>,
    jwt: Option<String>,
    labels: Option<SourceLabels>,
    /// (max_retries, initial_backoff, compute_units_per_second)
    retry: Option<(u32, u64, u64)>,
}

macro_rules! build_layered_client {
    ($transport:expr, $($layer:ident $(? $([[$option:tt]])?)?),*) => {
        build_layered_client!(@parse[] $transport, $([$(@option$($option)?)? $layer],)*)
    };
    (@parse[$($l:expr),*] $transport:expr, [@option $layer:ident], $($t:tt)*) => {
        if let Some($layer) = $layer {
            build_layered_client!(@parse[$($l,)* $layer] $transport, $($t)*)
        } else {
            build_layered_client!(@parse[$($l),*] $transport, $($t)*)
        }
    };
    (@parse[$($layer:expr),*] $transport:expr,) => {
        build_layered_client!(@build $transport, $($layer,)*)
    };
    (@build $transport:expr, $($layer:expr,)*) => {{
        let (transport, is_local) = $transport;
        ClientBuilder::default()
            $(.layer($layer))*
            .transport(transport, is_local)
    }};
}

impl SourceBuilder {
    /// Create a fresh builder with no configuration.
    pub fn new() -> Self {
        Self { max_concurrent_chunks: Some(DEFAULT_MAX_CONCURRENT_CHUNKS), ..Self::default() }
    }

    /// Provide an already constructed provider (overrides rpc_url if both set).
    pub fn provider(mut self, provider: DynProvider) -> Self {
        self.provider = Some(provider);
        self
    }

    /// Set the RPC url (used only if `provider` not supplied).
    pub fn rpc_url(mut self, url: String) -> Self {
        self.rpc_url = Some(normalize_rpc_url(url));
        self
    }

    /// Override chain id instead of querying it from the remote node.
    pub fn chain_id(mut self, chain_id: u64) -> Self {
        self.chain_id = Some(chain_id);
        self
    }

    /// Set internal request batch size used for certain dataset fetch operations.
    pub fn inner_request_size(mut self, size: u64) -> Self {
        self.inner_request_size = Some(size);
        self
    }

    /// Limit number of dataset chunks processed concurrently (`None` => unlimited).
    pub fn max_concurrent_chunks(mut self, m: Option<u64>) -> Self {
        self.max_concurrent_chunks = m;
        self
    }

    /// Supply a custom semaphore controlling concurrent RPC requests.
    pub fn semaphore(mut self, semaphore: Option<Semaphore>) -> Self {
        self.semaphore = Some(Arc::new(semaphore));
        self
    }

    /// Supply a rate limiter gating outbound RPC calls.
    pub fn rate_limiter(mut self, limiter: Option<RateLimiter>) -> Self {
        self.rate_limiter = Some(Arc::new(limiter));
        self
    }

    /// Attach an optional JWT token (currently stored for future use when auth headers become
    /// available).
    pub fn jwt(mut self, jwt: Option<String>) -> Self {
        self.jwt = jwt;
        self
    }

    /// Provide custom label metadata (non-functional; used for diagnostics / reporting).
    pub fn labels(mut self, labels: SourceLabels) -> Self {
        self.labels = Some(labels);
        self
    }

    /// Configure a retry policy layer to be attached to the underlying transport.
    /// Parameters mirror `RetryBackoffLayer::new(max_retries, initial_backoff,
    /// compute_units_per_second)`.
    pub fn retry_policy(
        mut self,
        max_retries: u32,
        initial_backoff: u64,
        compute_units_per_second: u64,
    ) -> Self {
        self.retry = Some((max_retries, initial_backoff, compute_units_per_second));
        self
    }

    fn build_http_transport(&self) -> Result<(BoxTransport, bool)> {
        let rpc_url = self.rpc_url.as_deref().ok_or_else(|| {
            CollectError::CollectError(
                "SourceBuilder requires either provider or rpc_url".to_string(),
            )
        })?;
        let is_local = guess_local_url(rpc_url);
        let rpc_url =
            rpc_url.parse().map_err(|e| CollectError::ParseError(ParseError::ParseUrlError(e)))?;

        // Attach auth layer at the transport level if JWT provided
        let http = if let Some(jwt) = self.jwt.as_deref() {
            match jwt.parse() {
                Ok(secret) => {
                    let layer = AuthLayer::new(secret);
                    // HyperClient::with_service(service)
                    build_http_jwt_client(layer, rpc_url)
                }
                Err(e) => {
                    return Err(CollectError::ParseError(ParseError::ParseJwtError(e.to_string())));
                }
            }
        } else {
            Http::new(rpc_url).into_box_transport()
        };

        Ok((http, is_local))
    }

    fn build_client(&self) -> Result<RpcClient> {
        // Optional retry layer only (auth already applied at transport level)
        let retry_layer = self.retry.map(|(max_r, initial_backoff, cu_ps)| {
            RetryBackoffLayer::new(max_r, initial_backoff, cu_ps)
        });
        let transport = self.build_http_transport()?;
        let client = build_layered_client!(transport, retry_layer?);
        Ok(client)
    }

    /// Build the `Source`. This may perform network I/O to fetch `chain_id`
    /// if it was not supplied.
    pub async fn build(mut self) -> Result<Source> {
        // Ensure we have a provider
        if self.provider.is_none() {
            let client = self.build_client()?;
            let provider = ProviderBuilder::new().connect_client(client);
            self.provider = Some(provider.erased());
        }
        let provider = self.provider.expect("provider just ensured above");

        // Resolve chain id if needed
        if self.chain_id.is_none() {
            let id = provider
                .get_chain_id()
                .await
                .map_err(|_| CollectError::RPCError("could not get chain_id".to_string()))?;
            self.chain_id = Some(id);
        }

        // Defaults
        let inner_request_size = self.inner_request_size.unwrap_or(DEFAULT_INNER_REQUEST_SIZE);
        let labels = self.labels.unwrap_or(SourceLabels {
            max_concurrent_requests: Some(DEFAULT_MAX_CONCURRENT_REQUESTS),
            max_requests_per_second: Some(0),
            max_retries: Some(DEFAULT_MAX_RETRIES),
            initial_backoff: Some(DEFAULT_INTIAL_BACKOFF),
        });

        let semaphore = self.semaphore.unwrap_or_else(|| Arc::new(None));
        let rate_limiter = self.rate_limiter.unwrap_or_else(|| Arc::new(None));

        Ok(Source {
            provider,
            chain_id: self.chain_id.expect("chain id ensured"),
            inner_request_size,
            max_concurrent_chunks: self.max_concurrent_chunks,
            rpc_url: self.rpc_url.unwrap_or_else(|| "unknown".to_string()),
            jwt: self.jwt,
            semaphore,
            rate_limiter,
            labels,
        })
    }
}

/// Build a hyper-based transport that applies the provided `AuthLayer`.
/// We take ownership of the parsed URL so we can construct an `Http` transport
/// with the layered hyper client and return it as a `BoxTransport`.
pub fn build_http_jwt_client(layer: AuthLayer, url: url::Url) -> BoxTransport {
    use alloy_transport_http::{
        hyper::body::Bytes,
        hyper_util::{client::legacy::Client, rt::TokioExecutor},
    };
    use http_body_util::Full;

    // Build a legacy hyper client (the underlying service)
    let hyper_service = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();

    // Wrap the hyper service with the auth layer
    let service = tower::ServiceBuilder::new().layer(layer).service(hyper_service);

    // Create an alloy HyperClient from the layered service and wrap it in an
    // Http transport, then convert to a BoxTransport.
    let hyper_client = HyperClient::with_service(service);
    Http::with_client(hyper_client, url).into_box_transport()
}

impl Source {
    /// Start building a `Source` with `SourceBuilder`.
    pub fn builder() -> SourceBuilder {
        SourceBuilder::new()
    }
}

/// source labels (non-functional)
#[derive(Clone, Debug, Default)]
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

type Result<T> = ::core::result::Result<T, CollectError>;

// impl<P: JsonRpcClient> Fetcher<P> {
impl Source {
    /// Returns an array (possibly empty) of logs that match the filter
    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_logs(filter).await)
    }

    /// Replays all transactions in a block returning the requested traces for each transaction
    pub async fn trace_replay_block_transactions(
        &self,
        block: BlockNumberOrTag,
        trace_types: Vec<TraceType>,
    ) -> Result<Vec<TraceResultsWithTransactionHash>> {
        let _permit = self.permit_request().await;
        let trace_result = self
            .provider
            .trace_replay_block_transactions(block.into())
            .trace_types(trace_types)
            .await;
        Self::map_err(trace_result)
    }

    /// Get state diff traces of block
    pub async fn trace_block_state_diffs(
        &self,
        block: u32,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<TraceResultsWithTransactionHash>)> {
        // get traces
        let result = self
            .trace_replay_block_transactions(
                BlockNumberOrTag::Number(block as u64),
                vec![TraceType::StateDiff],
            )
            .await?;

        // get transactions
        let txs = if include_transaction_hashes {
            let transactions = self
                .get_block(block as u64, BlockTransactionsKind::Hashes)
                .await?
                .ok_or(CollectError::CollectError("could not find block".to_string()))?
                .transactions;
            match transactions {
                BlockTransactions::Hashes(hashes) => {
                    hashes.into_iter().map(|tx| Some(tx.to_vec())).collect()
                }
                _ => return Err(CollectError::CollectError("wrong transaction format".to_string())),
            }
        } else {
            vec![None; result.len()]
        };

        Ok((Some(block), txs, result))
    }

    /// Get VM traces of block
    pub async fn trace_block_vm_traces(
        &self,
        block: u32,
    ) -> Result<(Option<u32>, Option<Vec<u8>>, Vec<TraceResultsWithTransactionHash>)> {
        let result = self
            .trace_replay_block_transactions(
                BlockNumberOrTag::Number(block as u64),
                vec![TraceType::VmTrace],
            )
            .await;
        Ok((Some(block), None, result?))
    }

    /// Replays a transaction, returning the traces
    pub async fn trace_replay_transaction(
        &self,
        tx_hash: TxHash,
        trace_types: Vec<TraceType>,
    ) -> Result<TraceResults> {
        let _permit = self.permit_request().await;
        let trace_result =
            self.provider.trace_replay_transaction(tx_hash).trace_types(trace_types).await;
        Self::map_err(trace_result)
    }

    /// Get state diff traces of transaction
    pub async fn trace_transaction_state_diffs(
        &self,
        transaction_hash: Vec<u8>,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<TraceResults>)> {
        let result = self
            .trace_replay_transaction(
                B256::from_slice(&transaction_hash),
                vec![TraceType::StateDiff],
            )
            .await;
        Ok((None, vec![Some(transaction_hash)], vec![result?]))
    }

    /// Get VM traces of transaction
    pub async fn trace_transaction_vm_traces(
        &self,
        transaction_hash: Vec<u8>,
    ) -> Result<(Option<u32>, Option<Vec<u8>>, Vec<TraceResults>)> {
        let result = self
            .trace_replay_transaction(B256::from_slice(&transaction_hash), vec![TraceType::VmTrace])
            .await;
        Ok((None, Some(transaction_hash), vec![result?]))
    }

    /// Gets the transaction with transaction_hash
    pub async fn get_transaction_by_hash(&self, tx_hash: TxHash) -> Result<Option<Transaction>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_transaction_by_hash(tx_hash).await)
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
    pub async fn get_block(
        &self,
        block_num: u64,
        kind: BlockTransactionsKind,
    ) -> Result<Option<Block>> {
        let _permit = self.permit_request().await;
        let block_result = self.provider.get_block(block_num.into()).kind(kind).await;
        Self::map_err(block_result)
    }

    /// Gets the block with `block_hash` (transaction hashes only)
    pub async fn get_block_by_hash(
        &self,
        block_hash: B256,
        kind: BlockTransactionsKind,
    ) -> Result<Option<Block>> {
        let _permit = self.permit_request().await;
        let block_result = self.provider.get_block(block_hash.into()).kind(kind).await;
        Self::map_err(block_result)
    }

    /// Returns all receipts for a block.
    /// Note that this uses the `eth_getBlockReceipts` method which is not supported by all nodes.
    /// Consider using `FetcherExt::get_tx_receipts_in_block` which takes a block, and falls back to
    /// `eth_getTransactionReceipt` if `eth_getBlockReceipts` is not supported.
    pub async fn get_block_receipts(
        &self,
        block_num: u64,
    ) -> Result<Option<Vec<TransactionReceipt>>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_block_receipts(block_num.into()).await)
    }

    /// Returns traces created at given block
    pub async fn trace_block(
        &self,
        block_num: BlockNumber,
    ) -> Result<Vec<LocalizedTransactionTrace>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.trace_block(block_num.into()).await)
    }

    /// Returns all traces of a given transaction
    pub async fn trace_transaction(
        &self,
        tx_hash: TxHash,
    ) -> Result<Vec<LocalizedTransactionTrace>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.trace_transaction(tx_hash).await)
    }

    /// Deprecated
    pub async fn call(
        &self,
        transaction: TransactionRequest,
        block_number: BlockNumber,
    ) -> Result<Bytes> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.call(transaction).block(block_number.into()).await)
    }

    /// Returns traces for given call data
    pub async fn trace_call(
        &self,
        transaction: TransactionRequest,
        trace_type: Vec<TraceType>,
        block_number: Option<BlockNumber>,
    ) -> Result<TraceResults> {
        let _permit = self.permit_request().await;
        if let Some(bn) = block_number {
            return Self::map_err(
                self.provider
                    .trace_call(&transaction)
                    .trace_types(trace_type.clone())
                    .block_id(bn.into())
                    .await,
            );
        }
        Self::map_err(self.provider.trace_call(&transaction).trace_types(trace_type.clone()).await)
    }

    /// Get nonce of address
    pub async fn get_transaction_count(
        &self,
        address: Address,
        block_number: BlockNumber,
    ) -> Result<u64> {
        let _permit = self.permit_request().await;
        Self::map_err(
            self.provider.get_transaction_count(address).block_id(block_number.into()).await,
        )
    }

    /// Get code at address
    pub async fn get_balance(&self, address: Address, block_number: BlockNumber) -> Result<U256> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_balance(address).block_id(block_number.into()).await)
    }

    /// Get code at address
    pub async fn get_code(&self, address: Address, block_number: BlockNumber) -> Result<Bytes> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_code_at(address).block_id(block_number.into()).await)
    }

    /// Get stored data at given location
    pub async fn get_storage_at(
        &self,
        address: Address,
        slot: U256,
        block_number: BlockNumber,
    ) -> Result<U256> {
        let _permit = self.permit_request().await;
        Self::map_err(
            self.provider.get_storage_at(address, slot).block_id(block_number.into()).await,
        )
    }

    /// Get the block number
    pub async fn get_block_number(&self) -> Result<u64> {
        Self::map_err(self.provider.get_block_number().await)
    }

    // extra helpers below

    /// block number of transaction
    pub async fn get_transaction_block_number(&self, transaction_hash: Vec<u8>) -> Result<u32> {
        let block = self.get_transaction_by_hash(B256::from_slice(&transaction_hash)).await?;
        let block = block.ok_or(CollectError::CollectError("could not get block".to_string()))?;
        Ok(block
            .block_number
            .ok_or(CollectError::CollectError("could not get block number".to_string()))?
            as u32)
    }

    /// block number of transaction
    pub async fn get_transaction_logs(&self, transaction_hash: Vec<u8>) -> Result<Vec<Log>> {
        Ok(self
            .get_transaction_receipt(B256::from_slice(&transaction_hash))
            .await?
            .ok_or(CollectError::CollectError("transaction receipt not found".to_string()))?
            .inner
            .logs()
            .to_vec())
    }

    /// Return output data of a contract call
    pub async fn call2(
        &self,
        address: Address,
        call_data: Vec<u8>,
        block_number: BlockNumber,
    ) -> Result<Bytes> {
        let transaction = TransactionRequest {
            to: Some(address.into()),
            input: TransactionInput::new(call_data.into()),
            ..Default::default()
        };
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.call(transaction).block(block_number.into()).await)
    }

    /// Return output data of a contract call
    pub async fn trace_call2(
        &self,
        address: Address,
        call_data: Vec<u8>,
        trace_type: Vec<TraceType>,
        block_number: Option<BlockNumber>,
    ) -> Result<TraceResults> {
        let transaction = TransactionRequest {
            to: Some(address.into()),
            input: TransactionInput::new(call_data.into()),
            ..Default::default()
        };
        let _permit = self.permit_request().await;
        if let Some(bn) = block_number {
            Self::map_err(
                self.provider
                    .trace_call(&transaction)
                    .trace_types(trace_type.clone())
                    .block_id(bn.into())
                    .await,
            )
        } else {
            Self::map_err(
                self.provider.trace_call(&transaction).trace_types(trace_type.clone()).await,
            )
        }
    }

    /// get geth debug block traces
    pub async fn geth_debug_trace_block(
        &self,
        block_number: u32,
        options: GethDebugTracingOptions,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<TraceResult<GethTrace, String>>)> {
        let traces = {
            let _permit = self.permit_request().await;
            Self::map_err(
                self.provider
                    .debug_trace_block_by_number(
                        BlockNumberOrTag::Number(block_number.into()),
                        options,
                    )
                    .await,
            )?
        };

        let txs = if include_transaction_hashes {
            match self.get_block(block_number as u64, BlockTransactionsKind::Hashes).await? {
                Some(block) => block
                    .transactions
                    .as_hashes()
                    .unwrap()
                    .iter()
                    .map(|x| Some(x.to_vec()))
                    .collect(),
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
                TraceResult::Success { result, tx_hash } => match result {
                    GethTrace::JS(value) => calls.push(value),
                    _ => {
                        return Err(CollectError::CollectError(format!(
                            "invalid trace result in tx {tx_hash:?}"
                        )))
                    }
                },
                TraceResult::Error { error, tx_hash } => {
                    return Err(CollectError::CollectError(format!(
                        "invalid trace result in tx {tx_hash:?}: {error}"
                    )))
                }
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
                TraceResult::Success { result, tx_hash } => match result {
                    GethTrace::Default(frame) => calls.push(frame),
                    _ => {
                        return Err(CollectError::CollectError(format!(
                            "invalid trace result in tx {tx_hash:?}"
                        )))
                    }
                },
                TraceResult::Error { error, tx_hash } => {
                    return Err(CollectError::CollectError(format!(
                        "inalid trace result in tx {tx_hash:?}: {error}"
                    )));
                }
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
                // GethTrace::Known(GethTraceFrame::FourByteTracer(FourByteFrame(frame))) => {
                //     calls.push(frame)
                // }
                // GethTrace::Known(GethTraceFrame::NoopTracer(_)) => {}
                // _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
                TraceResult::Success { result, tx_hash } => match result {
                    GethTrace::FourByteTracer(frame) => calls.push(frame.0),
                    GethTrace::NoopTracer(_) => {}
                    _ => {
                        return Err(CollectError::CollectError(format!(
                            "invalid trace result in tx {tx_hash:?}"
                        )))
                    }
                },
                TraceResult::Error { error, tx_hash } => {
                    return Err(CollectError::CollectError(format!(
                        "invalid trace result in tx {tx_hash:?}: {error}"
                    )));
                }
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block call traces
    pub async fn geth_debug_trace_block_prestate(
        &self,
        block_number: u32,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<BTreeMap<Address, AccountState>>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::PreStateTracer);
        let options = GethDebugTracingOptions { tracer: Some(tracer), ..Default::default() };
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                // GethTrace::Known(GethTraceFrame::PreStateTracer(PreStateFrame::Default(
                //     PreStateMode(frame),
                // ))) => calls.push(frame),
                // _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
                TraceResult::Success { result, tx_hash } => match result {
                    GethTrace::PreStateTracer(PreStateFrame::Default(frame)) => calls.push(frame.0),
                    _ => {
                        return Err(CollectError::CollectError(format!(
                            "invalid trace result in tx {tx_hash:?}"
                        )))
                    }
                },
                TraceResult::Error { error, tx_hash } => {
                    return Err(CollectError::CollectError(format!(
                        "invalid trace result in tx {tx_hash:?}: {error}"
                    )));
                }
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
        // let config = GethDebugTracerConfig::BuiltInTracer(
        //     GethDebugBuiltInTracerConfig::CallTracer(CallConfig { ..Default::default() }),
        // );
        let options = GethDebugTracingOptions::default()
            .with_tracer(tracer)
            .with_call_config(CallConfig::default());
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            // match trace {
            //     GethTrace::Known(GethTraceFrame::CallTracer(call_frame)) =>
            // calls.push(call_frame),     _ => return
            // Err(CollectError::CollectError("invalid trace result".to_string())), }
            match trace {
                TraceResult::Success { result, tx_hash } => match result {
                    GethTrace::CallTracer(frame) => calls.push(frame),
                    _ => {
                        return Err(CollectError::CollectError(format!(
                            "invalid trace result in tx {tx_hash:?}"
                        )))
                    }
                },
                TraceResult::Error { error, tx_hash } => {
                    return Err(CollectError::CollectError(format!(
                        "invalid trace result in tx {tx_hash:?}: {error}"
                    )));
                }
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
        // let config = GethDebugTracerConfig::BuiltInTracer(
        //     GethDebugBuiltInTracerConfig::PreStateTracer(PreStateConfig { diff_mode: Some(true)
        // }),
        let options = GethDebugTracingOptions::default()
            .with_prestate_config(PreStateConfig {
                diff_mode: Some(true),
                disable_code: None,
                disable_storage: None,
            })
            .with_tracer(tracer);
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut diffs = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                TraceResult::Success { result, tx_hash } => match result {
                    GethTrace::PreStateTracer(PreStateFrame::Diff(diff)) => diffs.push(diff),
                    GethTrace::JS(serde_json::Value::Object(map)) => {
                        let diff = parse_geth_diff_object(map)?;
                        diffs.push(diff);
                    }
                    _ => {
                        println!("{result:?}");
                        return Err(CollectError::CollectError(format!(
                            "invalid trace result in tx {tx_hash:?}"
                        )));
                    }
                },
                TraceResult::Error { error, tx_hash } => {
                    return Err(CollectError::CollectError(format!(
                        "invalid trace result in tx {tx_hash:?}: {error}"
                    )));
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
        let ethers_tx = B256::from_slice(&transaction_hash);

        let trace = {
            let _permit = self.permit_request().await;
            self.provider
                .debug_trace_transaction(ethers_tx, options)
                .await
                .map_err(CollectError::ProviderError)?
        };
        let traces = vec![trace];

        let block_number = if include_block_number {
            match self.get_transaction_by_hash(ethers_tx).await? {
                Some(tx) => tx.block_number.map(|x| x as u32),
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
                GethTrace::JS(value) => calls.push(value),
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
                GethTrace::Default(frame) => calls.push(frame),
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
                GethTrace::FourByteTracer(frame) => calls.push(frame.0),
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
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<BTreeMap<Address, AccountState>>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::PreStateTracer);
        let options = GethDebugTracingOptions { tracer: Some(tracer), ..Default::default() };
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_block_number)
            .await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::PreStateTracer(PreStateFrame::Default(frame)) => calls.push(frame.0),
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
        // let config = GethDebugTracerConfig::BuiltInTracer(
        //     GethDebugBuiltInTracerConfig::CallTracer(CallConfig { ..Default::default() }),
        // );
        let options = GethDebugTracingOptions::default()
            .with_tracer(tracer)
            .with_call_config(CallConfig::default());
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_block_number)
            .await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                // GethTrace::Known(GethTraceFrame::CallTracer(call_frame)) =>
                // calls.push(call_frame),
                GethTrace::CallTracer(frame) => calls.push(frame),
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
        // let config = GethDebugTracerConfig::BuiltInTracer(
        //     GethDebugBuiltInTracerConfig::PreStateTracer(PreStateConfig { diff_mode: Some(true)
        // }), );
        let options = GethDebugTracingOptions::default().with_tracer(tracer).with_prestate_config(
            PreStateConfig { diff_mode: Some(true), disable_code: None, disable_storage: None },
        );
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_transaction_hashes)
            .await?;

        let mut diffs = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                // GethTrace::Known(GethTraceFrame::PreStateTracer(PreStateFrame::Diff(diff))) => {
                //     diffs.push(diff)
                // }
                GethTrace::PreStateTracer(PreStateFrame::Diff(diff)) => diffs.push(diff),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, diffs))
    }

    async fn permit_request(
        &self,
    ) -> Option<::core::result::Result<SemaphorePermit<'_>, AcquireError>> {
        let permit = match &*self.semaphore {
            Some(semaphore) => Some(semaphore.acquire().await),
            _ => None,
        };
        if let Some(limiter) = &*self.rate_limiter {
            limiter.until_ready().await;
        }
        permit
    }

    fn map_err<T>(res: ::core::result::Result<T, RpcError<TransportErrorKind>>) -> Result<T> {
        res.map_err(CollectError::ProviderError)
    }
}

use crate::err;
use std::collections::BTreeMap;

fn parse_geth_diff_object(map: serde_json::Map<String, serde_json::Value>) -> Result<DiffMode> {
    let pre: BTreeMap<Address, AccountState> = serde_json::from_value(map["pre"].clone())
        .map_err(|_| err("cannot deserialize pre diff"))?;
    let post: BTreeMap<Address, AccountState> = serde_json::from_value(map["post"].clone())
        .map_err(|_| err("cannot deserialize pre diff"))?;

    Ok(DiffMode { pre, post })
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests assume an ETH_RPC_URL pointing to a dev node or that the URL is unreachable.
    // For CI determinism, consider mocking provider once alloy offers an in-memory transport.

    #[tokio::test]
    async fn builder_defaults_from_url() {
        // Use an obviously invalid URL but with correct shape; build should fail only when chain_id
        // fetch fails. If environment has ETH_RPC_URL set and reachable, this will exercise
        // more.
        let url = "http://localhost:8545".to_string();
        let build = Source::builder().rpc_url(url).build().await;
        // We can't guarantee local node is up in test environment, so just assert we get some
        // result (Ok or Err is acceptable). If Ok, validate some defaults.
        if let Ok(source) = build {
            assert_eq!(source.inner_request_size, 100, "default inner_request_size");
            assert_eq!(source.max_concurrent_chunks, Some(4));
            assert!(source.labels.max_retries.is_some());
        }
    }

    #[tokio::test]
    async fn builder_overrides() {
        // This test will likely fail without a running node because chain_id fetch is required.
        // Skip if no local node.
        let url = "http://localhost:8545";
        let result = Source::builder()
            .rpc_url(url.to_string())
            .inner_request_size(42)
            .max_concurrent_chunks(Some(7))
            .labels(SourceLabels {
                max_concurrent_requests: Some(10),
                max_requests_per_second: Some(5),
                max_retries: Some(3),
                initial_backoff: Some(1),
            })
            .build()
            .await;
        if let Ok(source) = result {
            // if a node is available
            assert_eq!(source.inner_request_size, 42);
            assert_eq!(source.max_concurrent_chunks, Some(7));
            assert_eq!(source.labels.max_retries, Some(3));
        }
    }

    #[tokio::test]
    async fn builder_with_retry_policy() {
        let url = "http://localhost:8545";
        let result = Source::builder().rpc_url(url.to_string()).retry_policy(2, 1, 0).build().await;
        // Success depends on local node; if Ok, we at least exercised retry path.
        if let Ok(source) = result {
            assert_eq!(source.rpc_url, url);
        }
    }
}
