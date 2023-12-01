use crate::span::OtelSpan;
use serde::Deserialize;

/// TracesData represents the traces data that can be stored in a persistent storage,
/// OR can be embedded by other protocols that transfer OTLP traces data but do
/// not implement the OTLP protocol.
///
/// The main difference between this message and collector protocol is that
/// in this message there will not be any "control" or "metadata" specific to
/// OTLP protocol.
///
/// When new fields are added into this message, the OTLP request MUST be updated
/// as well.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TracesData {
    /// An array of ResourceSpans.
    /// For data coming from a single resource this array will typically contain
    /// one element. Intermediary nodes that receive data from multiple origins
    /// typically batch the data before forwarding further and in that case this
    /// array will contain multiple elements.
    pub resource_spans: Vec<ResourceSpans>,
}
/// A collection of ScopeSpans from a Resource.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSpans {
    /// A list of ScopeSpans that originate from a resource.
    pub scope_spans: Vec<ScopeSpans>,
}
/// A collection of Spans produced by an InstrumentationScope.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeSpans {
    /// A list of Spans that originate from an instrumentation scope.
    pub spans: Vec<OtelSpan>,
}
