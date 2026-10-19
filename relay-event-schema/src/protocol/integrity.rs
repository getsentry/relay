//! Contains definitions for the browser Integrity policy interface.

use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

/// Generated Integrity policy.
/// Can't name as "BodyRaw", like in nel.rs, due to name conflicts
#[derive(Debug, Default, Clone, PartialEq, FromValue, IntoValue, Empty)]
pub struct IntegrityBodyRaw {
    #[metastructure(field = "documentURL")]
    pub document_url: Annotated<String>,
    #[metastructure(field = "blockedURL")]
    pub blocked_url: Annotated<String>,
    pub destination: Annotated<String>,
    #[metastructure(field = "reportOnly")]
    pub report_only: Annotated<bool>,
    #[metastructure(additional_properties, pii = "maybe")]
    pub other: Object<Value>,
}

/// Models the content of a Integrity report.
#[derive(Debug, Default, Clone, PartialEq, FromValue, IntoValue, Empty)]
pub struct IntegrityReportRaw {
    /// The age of the report since it got collected and before it got sent.
    pub age: Annotated<i64>,
    /// The type of the report.
    #[metastructure(field = "type")]
    pub ty: Annotated<String>,
    /// The URL of the document in which the error occurred.
    #[metastructure(pii = "true")]
    pub url: Annotated<String>,
    /// The User-Agent HTTP header.
    pub user_agent: Annotated<String>,
    /// The body of the Integrity report.
    pub body: Annotated<IntegrityBodyRaw>,
    /// For forward compatibility.
    #[metastructure(additional_properties, pii = "maybe")]
    pub other: Object<Value>,
}
