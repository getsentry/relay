use pyo3::prelude::*;
use pyo3::wrap_pymodule;

mod auth;
mod codeowners;
mod consts;
mod exceptions;
mod processing;
mod utils;

#[pymodule]
fn _relay_pyo3(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    let modules = py.import_bound("sys")?.getattr("modules")?;

    let auth_module = wrap_pymodule!(auth::auth);
    let consts_module = wrap_pymodule!(consts::consts);
    let exceptions_module = wrap_pymodule!(exceptions::exceptions);
    let processing_module = wrap_pymodule!(processing::processing);

    // Expose them as "from sentry_relay as processing"
    m.add_wrapped(auth_module)?;
    m.add_wrapped(processing_module)?;
    m.add_wrapped(consts_module)?;
    m.add_wrapped(exceptions_module)?;

    // Expose them as "import sentry_relay.processing"
    modules.set_item("sentry_relay.auth", auth_module(py))?;
    modules.set_item("sentry_relay.consts", consts_module(py))?;
    modules.set_item("sentry_relay.exceptions", exceptions_module(py))?;
    modules.set_item("sentry_relay.processing", processing_module(py))?;

    Ok(())
}
