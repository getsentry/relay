//! Integrity Violation reports.
//!
//! See <https://w3c.github.io/webappsec-subresource-integrity/#report-violations>.

use serde::{Deserialize, Serialize};

/// The body of an Integrity Violation report.
///
/// See <https://w3c.github.io/webappsec-subresource-integrity/#report-violations>.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Body {
    /// The URL of the request client's
    /// document or `WorkerGlobalScope`.
    ///
    /// See <https://w3c.github.io/webappsec-subresource-integrity/#report-violations>,
    /// points 6 and 7.
    pub document_url: String,
    /// The request's URL.
    pub blocked_url: String,
    /// The request's destination.
    ///
    /// See <https://fetch.spec.whatwg.org/#destination-type>.
    pub destination: String,
    /// Whether the integrity policy is configured to
    /// only report violations instead of blocking the request.
    pub report_only: bool,
}
