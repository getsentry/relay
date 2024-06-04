use pyo3::prelude::*;
use relay_base_schema::data_category::DataCategory;
use std::collections::HashMap;
use strum::IntoEnumIterator;

/**
 * Trace status.
 *
 * Values from <https://github.com/open-telemetry/opentelemetry-specification/blob/8fb6c14e4709e75a9aaa64b0dbbdf02a6067682a/specification/api-tracing.md#status>
 * Mapping to HTTP from <https://github.com/open-telemetry/opentelemetry-specification/blob/8fb6c14e4709e75a9aaa64b0dbbdf02a6067682a/specification/data-http.md#status>
 */
#[derive(Debug, Copy, Clone, strum::Display, strum::EnumIter)]
#[strum(serialize_all = "snake_case")]
#[repr(u8)]
enum SpanStatus {
    /**
     * The operation completed successfully.
     *
     * HTTP status 100..299 + successful redirects from the 3xx range.
     */
    Ok = 0,
    /**
     * The operation was cancelled (typically by the user).
     */
    Cancelled = 1,
    /**
     * Unknown. Any non-standard HTTP status code.
     *
     * "We do not know whether the transaction failed or succeeded"
     */
    Unknown = 2,
    /**
     * Client specified an invalid argument. 4xx.
     *
     * Note that this differs from FailedPrecondition. InvalidArgument indicates arguments that
     * are problematic regardless of the state of the system.
     */
    InvalidArgument = 3,
    /**
     * Deadline expired before operation could complete.
     *
     * For operations that change the state of the system, this error may be returned even if the
     * operation has been completed successfully.
     *
     * HTTP redirect loops and 504 Gateway Timeout
     */
    DeadlineExceeded = 4,
    /**
     * 404 Not Found. Some requested entity (file or directory) was not found.
     */
    NotFound = 5,
    /**
     * Already exists (409)
     *
     * Some entity that we attempted to create already exists.
     */
    AlreadyExists = 6,
    /**
     * 403 Forbidden
     *
     * The caller does not have permission to execute the specified operation.
     */
    PermissionDenied = 7,
    /**
     * 429 Too Many Requests
     *
     * Some resource has been exhausted, perhaps a per-user quota or perhaps the entire file
     * system is out of space.
     */
    ResourceExhausted = 8,
    /**
     * Operation was rejected because the system is not in a state required for the operation's
     * execution
     */
    FailedPrecondition = 9,
    /**
     * The operation was aborted, typically due to a concurrency issue.
     */
    Aborted = 10,
    /**
     * Operation was attempted past the valid range.
     */
    OutOfRange = 11,
    /**
     * 501 Not Implemented
     *
     * Operation is not implemented or not enabled.
     */
    Unimplemented = 12,
    /**
     * Other/generic 5xx.
     */
    InternalError = 13,
    /**
     * 503 Service Unavailable
     */
    Unavailable = 14,
    /**
     * Unrecoverable data loss or corruption
     */
    DataLoss = 15,
    /**
     * 401 Unauthorized (actually does mean unauthenticated according to RFC 7235)
     *
     * Prefer PermissionDenied if a user is logged in.
     */
    Unauthenticated = 16,
}

impl SpanStatus {
    fn status_code_to_name() -> HashMap<u8, String> {
        Self::iter().map(|v| (v as u8, v.to_string())).collect()
    }

    fn status_name_to_code() -> HashMap<String, u8> {
        Self::iter().map(|v| (v.to_string(), v as u8)).collect()
    }
}

#[pymodule]
pub fn consts(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<DataCategory>()?;
    m.add(
        "SPAN_STATUS_CODE_TO_NAME",
        SpanStatus::status_code_to_name(),
    )?;
    m.add(
        "SPAN_STATUS_NAME_TO_CODE",
        SpanStatus::status_name_to_code(),
    )?;
    Ok(())
}
