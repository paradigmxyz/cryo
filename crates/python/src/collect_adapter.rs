use polars::prelude::*;
use pyo3::{exceptions::PyTypeError, prelude::*};
use pyo3_polars::PyDataFrame;

use cryo_cli::{parse_args, Args};
use cryo_freeze::collect;

#[pyfunction(
    signature = (
        datatype = None,
        blocks = None,
        *,
        remember = false,
        command = None,
        txs = None,
        align = false,
        reorg_buffer = 0,
        include_columns = None,
        exclude_columns = None,
        columns = None,
        u256_types = None,
        hex = false,
        sort = None,
        rpc = None,
        network_name = None,
        requests_per_second = None,
        max_concurrent_requests = None,
        max_concurrent_chunks = None,
        max_retries = 10,
        initial_backoff = 500,
        dry = false,
        chunk_size = 1000,
        n_chunks = None,
        partition_by = None,
        output_dir = ".".to_string(),
        subdirs = vec![],
        file_suffix = None,
        overwrite = false,
        csv = false,
        json = false,
        row_group_size = None,
        n_row_groups = None,
        no_stats = false,
        compression = vec!["lz4".to_string()],
        report_dir = None,
        no_report = false,
        address = None,
        to_address = None,
        from_address = None,
        call_data = None,
        function = None,
        inputs = None,
        slot = None,
        contract = None,
        topic0 = None,
        topic1 = None,
        topic2 = None,
        topic3 = None,
        inner_request_size = 1,
        verbose = false,
        no_verbose = false,
        event_signature = None,
    )
)]
#[allow(clippy::too_many_arguments)]
pub fn _collect(
    py: Python<'_>,
    datatype: Option<String>,
    blocks: Option<Vec<String>>,
    remember: bool,
    command: Option<String>,
    txs: Option<Vec<String>>,
    align: bool,
    reorg_buffer: u64,
    include_columns: Option<Vec<String>>,
    exclude_columns: Option<Vec<String>>,
    columns: Option<Vec<String>>,
    u256_types: Option<Vec<String>>,
    hex: bool,
    sort: Option<Vec<String>>,
    rpc: Option<String>,
    network_name: Option<String>,
    requests_per_second: Option<u32>,
    max_concurrent_requests: Option<u64>,
    max_concurrent_chunks: Option<u64>,
    max_retries: u32,
    initial_backoff: u64,
    dry: bool,
    chunk_size: u64,
    n_chunks: Option<u64>,
    partition_by: Option<Vec<String>>,
    output_dir: String,
    subdirs: Vec<String>,
    file_suffix: Option<String>,
    overwrite: bool,
    csv: bool,
    json: bool,
    row_group_size: Option<usize>,
    n_row_groups: Option<usize>,
    no_stats: bool,
    compression: Vec<String>,
    report_dir: Option<String>,
    no_report: bool,
    address: Option<Vec<String>>,
    to_address: Option<Vec<String>>,
    from_address: Option<Vec<String>>,
    call_data: Option<Vec<String>>,
    function: Option<Vec<String>>,
    inputs: Option<Vec<String>>,
    slot: Option<Vec<String>>,
    contract: Option<Vec<String>>,
    topic0: Option<Vec<String>>,
    topic1: Option<Vec<String>>,
    topic2: Option<Vec<String>>,
    topic3: Option<Vec<String>>,
    inner_request_size: u64,
    verbose: bool,
    no_verbose: bool,
    event_signature: Option<String>,
) -> PyResult<&PyAny> {
    if let Some(command) = command {
        pyo3_asyncio::tokio::future_into_py(py, async move {
            match run_execute(command).await {
                Ok(df) => Ok(PyDataFrame(df)),
                Err(_e) => Err(PyErr::new::<PyTypeError, _>("failed")),
            }
        })
    } else if let Some(datatype) = datatype {
        let args = Args {
            datatype: vec![datatype],
            blocks,
            remember,
            txs,
            align,
            reorg_buffer,
            include_columns,
            exclude_columns,
            columns,
            u256_types,
            hex,
            sort,
            rpc,
            network_name,
            requests_per_second,
            max_concurrent_requests,
            max_concurrent_chunks,
            max_retries,
            initial_backoff,
            dry,
            chunk_size,
            n_chunks,
            partition_by,
            output_dir,
            subdirs,
            file_suffix,
            overwrite,
            csv,
            json,
            row_group_size,
            n_row_groups,
            no_stats,
            compression,
            report_dir: report_dir.map(std::path::PathBuf::from),
            no_report,
            address,
            to_address,
            from_address,
            call_data,
            function,
            inputs,
            slot,
            contract,
            topic0,
            topic1,
            topic2,
            topic3,
            inner_request_size,
            verbose,
            no_verbose,
            event_signature,
        };
        pyo3_asyncio::tokio::future_into_py(py, async move {
            match run_collect(args).await {
                // Ok(df) => Ok(Python::with_gil(|py| py.None())),
                Ok(df) => Ok(PyDataFrame(df)),
                Err(_e) => Err(PyErr::new::<PyTypeError, _>("failed")),
            }
        })
    } else {
        return Err(PyErr::new::<PyTypeError, _>("must specify datatype or command"))
    }
}

async fn run_collect(args: Args) -> PolarsResult<DataFrame> {
    let (query, source, _sink, _env) = match parse_args(&args).await {
        Ok(opts) => opts,
        Err(e) => panic!("error parsing opts {:?}", e),
    };
    match collect(query, source.into()).await {
        Ok(df) => Ok(df),
        Err(e) => panic!("error collecting {:?}", e),
    }
}

async fn run_execute(command: String) -> PolarsResult<DataFrame> {
    let args = match cryo_cli::parse_str(command.as_str()).await {
        Ok(opts) => opts,
        Err(e) => panic!("error parsing opts {:?}", e),
    };
    run_collect(args).await
}
