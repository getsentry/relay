//! Integrity Violation reports.
//!
//! See <https://w3c.github.io/webappsec-subresource-integrity/#report-violations>.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Body {
    pub document_url: String,
    pub blocked_url: String,
    pub destination: String,
    pub report_only: bool,
}
