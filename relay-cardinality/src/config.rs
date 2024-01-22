use relay_base_schema::metrics::MetricNamespace;
use serde::{Deserialize, Serialize};

use crate::SlidingWindow;

/// A cardinality limit.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct CardinalityLimit {
    /// Unique identifier of the cardinality limit.
    pub id: String,
    /// The sliding window to enforce the cardinality limits in.
    pub window: SlidingWindow,
    /// The cardinality limit.
    pub limit: u64,

    /// Scope which the limit applies to.
    pub scope: CardinalityScope,
    /// Metric namespace the limit applies to.
    ///
    /// No namespace means this specific limit is enforced across all namespaces.
    pub namespace: Option<MetricNamespace>, // TODO: should this be a list of namespaces?
}

/// A scope to restrict the [`CardinalityLimit`] to.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub enum CardinalityScope {
    /// The organization that this project belongs to.
    ///
    /// This is the top-level scope.
    Organization,

    /// Any other scope that is not known by this Relay.
    #[serde(other)]
    Unknown,
}
