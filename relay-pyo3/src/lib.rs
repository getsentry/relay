use pyo3::prelude::*;
use pyo3::wrap_pymodule;

mod auth;
mod codeowners;
mod consts;
mod exceptions;
mod processing;
mod utils;

#[pymodule]
fn _relay_pyo3(m: &Bound<PyModule>) -> PyResult<()> {
    processing::processing(m)?;

    Ok(())
}
