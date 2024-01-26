use relay_base_schema::metrics::MetricNamespace;
use serde::{Deserialize, Serialize};

use crate::SlidingWindow;

/// A cardinality limit.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
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
    pub namespace: Option<MetricNamespace>,
}

/// A scope to restrict the [`CardinalityLimit`] to.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CardinalityScope {
    /// The organization that this project belongs to.
    ///
    /// This is the top-level scope.
    Organization,

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
