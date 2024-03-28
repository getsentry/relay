use relay_base_schema::metrics::MetricNamespace;
use serde::{Deserialize, Serialize};

use crate::SlidingWindow;

/// A cardinality limit.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CardinalityLimit {
    /// Unique identifier of the cardinality limit.
    pub id: String,
    /// Whether this is a passive limit.
    ///
    /// Passive limits are tracked separately to normal limits
    /// and are not enforced, but still evaluated.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub passive: bool,

    /// The sliding window to enforce the cardinality limits in.
    pub window: SlidingWindow,
    /// The cardinality limit.
    pub limit: u64,

    /// Scope which the limit applies to.
    pub scope: CardinalityScope,
    /// Metric namespace the limit applies to.
    ///
    /// No namespace means this specific limit is enforced across all namespaces.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<MetricNamespace>,
}

/// A scope to restrict the [`CardinalityLimit`] to.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CardinalityScope {
    /// An organization level cardinality limit.
    ///
    /// The limit will be enforced across the entire org.
    Organization,

    /// A project level cardinality limit.
    ///
    /// The limit will be enforced for a specific project.
    Project,

    /// A per metric name cardinality limit.
    Name,

    /// Any other scope that is not known by this Relay.
    #[serde(other)]
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cardinality_limit_json() {
        let limit = CardinalityLimit {
            id: "some_id".to_string(),
            passive: false,
            window: SlidingWindow {
                window_seconds: 3600,
                granularity_seconds: 200,
            },
            limit: 1337,
            scope: CardinalityScope::Organization,
            namespace: Some(MetricNamespace::Custom),
        };

        let j = serde_json::to_string(&limit).unwrap();
        assert_eq!(serde_json::from_str::<CardinalityLimit>(&j).unwrap(), limit);

        let j = r#"{
            "id":"some_id",
            "window":{"windowSeconds":3600,"granularitySeconds":200},
            "limit":1337,
            "scope":"organization",
            "namespace":"custom"
        }"#;
        assert_eq!(serde_json::from_str::<CardinalityLimit>(j).unwrap(), limit);
    }
}
