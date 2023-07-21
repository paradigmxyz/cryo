use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use cryo_freeze::collect;


pub fn _collect(
    py: Python<'_>,
) -> PyResult<&PyAny> {

    pyo3_asyncio::tokio::future_into_py(py, async move {
        match run(args).await {
            Ok(()) => Ok(Python::with_gil(|py| py.None())),
            Err(_e) => Err(PyErr::new::<PyTypeError, _>("failed")),
        }
    })
}

