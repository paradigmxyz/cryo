use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    types::{IntoPyDict, PyDict},
};

use cryo_cli::{run, Args};

#[pyfunction(
    signature = (
        datatype,
        blocks,
        *,
        align = false,
        reorg_buffer = 0,
        include_columns = None,
        exclude_columns = None,
        columns = None,
        hex = false,
        sort = None,
        rpc = None,
        network_name = None,
        requests_per_second = None,
        max_concurrent_requests = None,
        max_concurrent_chunks = None,
        dry = false,
        chunk_size = 1000,
        n_chunks = None,
        output_dir = ".".to_string(),
        file_suffix = None,
        overwrite = false,
        csv = false,
        json = false,
        row_group_size = None,
        n_row_groups = None,
        no_stats = false,
        compression = vec!["lz4".to_string()],
        contract = None,
        topic0 = None,
        topic1 = None,
        topic2 = None,
        topic3 = None,
        inner_request_size = 1,
    )
)]
#[allow(clippy::too_many_arguments)]
pub fn _freeze(
    py: Python<'_>,
    datatype: Vec<String>,
    blocks: Vec<String>,
    align: bool,
    reorg_buffer: u64,
    include_columns: Option<Vec<String>>,
    exclude_columns: Option<Vec<String>>,
    columns: Option<Vec<String>>,
    hex: bool,
    sort: Option<Vec<String>>,
    rpc: Option<String>,
    network_name: Option<String>,
    requests_per_second: Option<u32>,
    max_concurrent_requests: Option<u64>,
    max_concurrent_chunks: Option<u64>,
    dry: bool,
    chunk_size: u64,
    n_chunks: Option<u64>,
    output_dir: String,
    file_suffix: Option<String>,
    overwrite: bool,
    csv: bool,
    json: bool,
    row_group_size: Option<usize>,
    n_row_groups: Option<usize>,
    no_stats: bool,
    compression: Vec<String>,
    contract: Option<String>,
    topic0: Option<String>,
    topic1: Option<String>,
    topic2: Option<String>,
    topic3: Option<String>,
    inner_request_size: u64,
) -> PyResult<&PyAny> {
    let args = Args {
        datatype,
        blocks,
        align,
        reorg_buffer,
        include_columns,
        exclude_columns,
        columns,
        hex,
        sort,
        rpc,
        network_name,
        requests_per_second,
        max_concurrent_requests,
        max_concurrent_chunks,
        dry,
        chunk_size,
        n_chunks,
        output_dir,
        file_suffix,
        overwrite,
        csv,
        json,
        row_group_size,
        n_row_groups,
        no_stats,
        compression,
        contract,
        topic0,
        topic1,
        topic2,
        topic3,
        inner_request_size,
    };

    pyo3_asyncio::tokio::future_into_py(py, async move {
        match run(args).await {
            Ok(Some(result)) => Python::with_gil(|py| {
                let paths_by_type = PyDict::new(py);
                for (key, values) in &result.paths_by_type {
                    let key = key.dataset().name();
                    let values: Vec<&str> = values.iter().map(AsRef::as_ref).collect();
                    paths_by_type.set_item(key, values).unwrap();
                }
                let paths_by_type = paths_by_type.to_object(py);

                let dict = [
                    ("n_completed".to_string(), result.n_completed.into_py(py)),
                    ("n_skipped".to_string(), result.n_skipped.into_py(py)),
                    ("n_errored".to_string(), result.n_errored.into_py(py)),
                    ("paths_by_type".to_string(), paths_by_type),
                ]
                .into_py_dict(py);
                Ok(dict.to_object(py))
            }),
            Ok(None) => Ok(Python::with_gil(|py| py.None())),
            _ => Err(PyErr::new::<PyTypeError, _>("failed")),
        }
    })
}
