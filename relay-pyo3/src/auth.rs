use std::collections::HashMap;

use chrono::Duration;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use relay_auth::{
    KeyParseError, PublicKey, RegisterRequest, RegisterResponse, RelayVersion, SecretKey,
};

use crate::exceptions::pyerr_from_unpack_error;
use crate::utils::extract_bytes_or_str;

#[pyclass(name = "RustPublicKey")]
#[derive(Clone, Eq, PartialEq)]
pub struct PyPublicKey(PublicKey);

#[pymethods]
impl PyPublicKey {
    #[staticmethod]
    fn parse(string: &Bound<PyAny>) -> PyResult<Self> {
        let string = extract_bytes_or_str(string)?;
        let inner = string
            .parse::<PublicKey>()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self(inner))
    }

    fn verify(
        &self,
        buf: &Bound<PyBytes>,
        sig: &Bound<PyAny>,
        max_age: Option<Duration>,
    ) -> PyResult<bool> {
        let sig = extract_bytes_or_str(sig)?;
        let buf = buf.as_bytes();
        match max_age {
            Some(max_age) => Ok(self.0.verify_timestamp(buf, sig, Some(max_age))),
            None => Ok(self.0.verify(buf, sig)),
        }
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(slf: &Bound<Self>) -> PyResult<String> {
        let class_name = slf.get_type().qualname()?;
        Ok(format!("<{class_name} '{}'>", slf.borrow().0))
    }
}

#[pyclass(name = "RustSecretKey")]
#[derive(Clone, Eq, PartialEq)]
pub struct PySecretKey(SecretKey);

#[pymethods]
impl PySecretKey {
    #[staticmethod]
    fn parse(string: &Bound<PyAny>) -> PyResult<Self> {
        let string = extract_bytes_or_str(string)?;
        let inner = string
            .parse()
            .map_err(|e: KeyParseError| PyValueError::new_err(e.to_string()))?;
        Ok(Self(inner))
    }

    fn sign(&self, value: &Bound<PyBytes>) -> String {
        self.0.sign(value.as_bytes())
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(slf: &Bound<Self>) -> PyResult<String> {
        let class_name = slf.get_type().qualname()?;
        Ok(format!("<{class_name} '{}'>", slf.borrow().0))
    }
}

#[pyfunction(name = "_generate_key_pair")]
fn generate_key_pair() -> (PySecretKey, PyPublicKey) {
    let (secret, public) = relay_auth::generate_key_pair();
    (PySecretKey(secret), PyPublicKey(public))
}

// TODO: Decode on the Python side
#[pyfunction]
fn generate_relay_id() -> [u8; 16] {
    let id = relay_auth::generate_relay_id();
    id.into_bytes()
}

#[pyfunction(name = "_create_register_challenge")]
#[pyo3(signature = (data, signature, secret, max_age = 60))]
fn create_register_challenge(
    data: &Bound<PyBytes>,
    signature: &Bound<PyAny>,
    secret: &Bound<PyAny>,
    max_age: u32,
) -> PyResult<HashMap<&'static str, String>> {
    let max_age = (max_age > 0).then_some(Duration::seconds(max_age.into()));
    let signature = extract_bytes_or_str(signature)?;
    let secret = extract_bytes_or_str(secret)?;

    let req = RegisterRequest::bootstrap_unpack(data.as_bytes(), signature, max_age)
        .map_err(pyerr_from_unpack_error)?;

    let challenge = req.into_challenge(secret.as_bytes());

    let mut output = HashMap::new();

    output.insert("relay_id", challenge.relay_id().to_string());
    output.insert("token", challenge.token().to_owned());

    Ok(output)
}

#[pyfunction(name = "_validate_register_response")]
#[pyo3(signature = (data, signature, secret, max_age = 60))]
fn validate_register_response(
    data: &Bound<PyBytes>,
    signature: &Bound<PyAny>,
    secret: &Bound<PyAny>,
    max_age: u32,
) -> PyResult<HashMap<&'static str, String>> {
    let max_age = (max_age > 0).then_some(Duration::seconds(max_age.into()));
    let signature = extract_bytes_or_str(signature)?;
    let secret = extract_bytes_or_str(secret)?;

    let (response, state) =
        RegisterResponse::unpack(data.as_bytes(), signature, secret.as_bytes(), max_age)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

    let mut output = HashMap::new();

    // TODO: Parse the UUID on the Python side
    output.insert("relay_id", response.relay_id().to_string());
    output.insert("token", response.token().to_owned());
    output.insert("public_key", state.public_key().to_string());
    output.insert("version", response.version().to_string());

    Ok(output)
}

#[pyfunction]
#[pyo3(signature = (version = None))]
fn is_version_supported(version: Option<&Bound<PyAny>>) -> PyResult<bool> {
    let version = if let Some(version) = version {
        extract_bytes_or_str(version)?
    } else {
        ""
    };

    // These can be updated when deprecating legacy versions:
    if version.is_empty() {
        return Ok(true);
    }

    let version = version
        .parse::<RelayVersion>()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    Ok(version.supported())
}

#[pymodule]
pub fn auth(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(generate_key_pair, m)?)?;
    m.add_function(wrap_pyfunction!(generate_relay_id, m)?)?;
    m.add_function(wrap_pyfunction!(create_register_challenge, m)?)?;
    m.add_function(wrap_pyfunction!(validate_register_response, m)?)?;
    m.add_function(wrap_pyfunction!(is_version_supported, m)?)?;

    m.add_class::<PyPublicKey>()?;
    m.add_class::<PySecretKey>()?;
    Ok(())
}
