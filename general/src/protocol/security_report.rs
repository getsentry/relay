//! Contains definitions for the security report interfaces
//!
//! The security interfaces are CSP, HPKP, ExpectCT and ExpectStaple.

use crate::types::{Annotated, Object, Value};

/// Models the content of a CSP report
/// Note this models the older CSP reports (report-uri policy directive)
/// The new CSP reports (using report-to policy directive) are different
/// See  https://www.w3.org/TR/CSP3/
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct Csp {
    /// The directive whose enforcement caused the violation.
    pub effective_directive: Annotated<String>,
    /// The URI of the resource that was blocked from loading by the Content Security Policy.
    pub blocked_uri: Annotated<String>,
    /// The URI of the document in which the violation occurred.
    pub document_uri: Annotated<String>,
    /// The original policy as specified by the Content-Security-Policy HTTP header.
    pub original_policy: Annotated<String>,
    /// The referrer of the document in which the violation occurred.
    pub referrer: Annotated<String>,
    /// The HTTP status code of the resource on which the global object was instantiated.
    pub status_code: Annotated<u64>,
    /// The name of the policy section that was violated.
    pub violated_directive: Annotated<String>,
    /// The URL of the resource where the violation occurred,
    pub source_file: Annotated<String>,
    /// The line number in source-file on which the violation occurred.
    pub line_number: Annotated<u64>,
    /// The column number in source-file on which the violation occurred.
    pub column_number: Annotated<u64>,
    /// The first 40 characters of the inline script, event handler, or style that caused the violation.
    pub script_sample: Annotated<String>,
    /// policy disposition (enforce or report)
    pub disposition: Annotated<String>,
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

//TODO: fill in the types
pub type Hpkp = Value;
pub type ExpectCt = Value;
pub type ExpectStaple = Value;
