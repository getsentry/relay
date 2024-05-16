use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

/// Downcast a PyAny which is PyString or PyBytes to &str.
pub fn extract_bytes_or_str<'a>(input: &'a Bound<'a, PyAny>) -> PyResult<&'a str> {
    if let Ok(s) = input.downcast::<PyString>() {
        s.to_str()
    } else if let Ok(b) = input.downcast::<PyBytes>() {
        std::str::from_utf8(b.as_bytes()).map_err(PyValueError::new_err)
    } else {
        Err(PyValueError::new_err(
            "Invalid type. Input has to be string or bytes.",
        ))
    }
}
