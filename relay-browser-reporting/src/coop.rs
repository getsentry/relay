//! Cross-Origin-Opener-Policy (COOP) violation reports.
//!
//! See <https://html.spec.whatwg.org/multipage/browsers.html#reporting>.

use serde::{Deserialize, Serialize};

use crate::SourceFile;

/// Whether the policy was enforced or only reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Disposition {
    /// The policy was enforced.
    Enforce,
    /// The policy was only reported.
    Reporting,
}

/// The body of a COOP violation report.
///
/// Browsers name some fields differently than the spec, both names are accepted.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Body {
    /// The kind of violation, for example `navigation-to-response` or
    /// `access-from-coop-page-to-openee`.
    #[serde(rename = "type")]
    pub ty: String,
    /// Whether the policy was enforced or only reported.
    pub disposition: Disposition,
    /// The policy that caused the report, for example `same-origin`.
    pub effective_policy: String,
    /// The window property that was accessed, for access reports.
    pub property: Option<String>,
    /// The URL of the previous document, for navigations to a COOP page.
    #[serde(rename = "previousResponseURL")]
    pub previous_response_url: Option<String>,
    /// The URL of the next document, for navigations away from a COOP page.
    #[serde(rename = "nextResponseURL")]
    pub next_response_url: Option<String>,
    /// The URL of the opener window, for access reports.
    #[serde(rename = "openerURL")]
    pub opener_url: Option<String>,
    /// The URL of the opened window, for access reports.
    #[serde(rename = "openeeURL", alias = "openedWindowURL")]
    pub openee_url: Option<String>,
    /// The initial URL of the opened window, for access reports.
    #[serde(rename = "initialPopupURL", alias = "openedWindowInitialURL")]
    pub initial_popup_url: Option<String>,
    /// The URL of another window, for access reports.
    #[serde(rename = "otherDocumentURL", alias = "otherURL")]
    pub other_document_url: Option<String>,
    /// The referrer of the document.
    pub referrer: Option<String>,
    /// The script location that caused an access report.
    #[serde(flatten)]
    pub source_file: Option<SourceFile>,
}
