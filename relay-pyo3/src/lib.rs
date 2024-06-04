use pyo3::prelude::*;

mod auth;
mod codeowners;
mod consts;
pub(crate) mod exceptions;
mod processing;
mod utils;

#[pymodule]
fn _relay_pyo3(m: &Bound<PyModule>) -> PyResult<()> {
    auth::auth(m)?;
    exceptions::exceptions(m)?;
    processing::processing(m)?;

    Ok(())
}
