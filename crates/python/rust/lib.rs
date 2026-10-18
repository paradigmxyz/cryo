mod collect_adapter;
mod freeze_adapter;

use pyo3::prelude::*;
// use crate::freeze_adapter;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "_cryo_rust")]
fn cryo_rust(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(freeze_adapter::_freeze, m)?)?;
    m.add_function(wrap_pyfunction!(collect_adapter::_collect, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
