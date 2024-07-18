//! Metrics aggregator configuration.

use relay_metrics::aggregator::AggregatorConfig;
use relay_metrics::MetricNamespace;
use serde::{Deserialize, Serialize};

/// Parameters used for metric aggregation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct AggregatorServiceConfig {
    /// The config used by the internal aggregator.
    #[serde(flatten)]
    pub aggregator: AggregatorConfig,

    /// Maximum amount of bytes used for metrics aggregation.
    ///
    /// When aggregating metrics, Relay keeps track of how many bytes a metric takes in memory.
    /// This is only an approximation and does not take into account things such as pre-allocation
    /// in hashmaps.
    ///
    /// Defaults to `None`, i.e. no limit.
    pub max_total_bucket_bytes: Option<usize>,

    /// The approximate maximum number of bytes submitted within one flush cycle.
    ///
    /// This controls how big flushed batches of buckets get, depending on the number of buckets,
    /// the cumulative length of their keys, and the number of raw values. Since final serialization
    /// adds some additional overhead, this number is approxmate and some safety margin should be
    /// left to hard limits.
    pub max_flush_bytes: usize,

    /// The flushing interval in milliseconds that determines how often the aggregator is polled for
    /// flushing new buckets.
    ///
    /// Defaults to `100` milliseconds.
    pub flush_interval_ms: u64,
}

impl Default for AggregatorServiceConfig {
    fn default() -> Self {
        Self {
            aggregator: AggregatorConfig::default(),
            max_total_bucket_bytes: None,
            max_flush_bytes: 5_000_000, // 5 MB
            flush_interval_ms: 100,     // 100 milliseconds
        }
    }
}

/// Contains an [`AggregatorServiceConfig`] for a specific scope.
///
/// For now, the only way to scope an aggregator is by [`MetricNamespace`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScopedAggregatorConfig {
    /// Name of the aggregator, used to tag statsd metrics.
    pub name: String,
    /// Condition that needs to be met for a metric or bucket to be routed to a
    /// secondary aggregator.
    pub condition: Condition,
    /// The configuration of the secondary aggregator.
    pub config: AggregatorServiceConfig,
}

/// Condition that needs to be met for a metric or bucket to be routed to a
/// secondary aggregator.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Condition {
    /// Checks for equality on a specific field.
    Eq(FieldCondition),
    /// Matches if all conditions are true.
    And {
        /// Inner rules to combine.
        inner: Vec<Condition>,
    },
    /// Matches if any condition is true.
    Or {
        /// Inner rules to combine.
        inner: Vec<Condition>,
    },
    /// Inverts the condition.
    Not {
        /// Inner rule to negate.
        inner: Box<Condition>,
    },
}

impl Condition {
    /// Checks if the condition matches the given namespace.
    pub fn matches(&self, namespace: Option<MetricNamespace>) -> bool {
        match self {
            Condition::Eq(field) => field.matches(namespace),
            Condition::And { inner } => inner.iter().all(|cond| cond.matches(namespace)),
            Condition::Or { inner } => inner.iter().any(|cond| cond.matches(namespace)),
            Condition::Not { inner } => !inner.matches(namespace),
        }
    }
}

/// Defines a field and a field value to compare to when a [`Condition`] is evaluated.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "field", content = "value", rename_all = "lowercase")]
pub enum FieldCondition {
    /// Field that allows comparison to a metric or bucket's namespace.
    Namespace(MetricNamespace),
}

impl FieldCondition {
    fn matches(&self, namespace: Option<MetricNamespace>) -> bool {
        match (self, namespace) {
            (FieldCondition::Namespace(expected), Some(actual)) => expected == &actual,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;
    use relay_metrics::MetricNamespace;
    use serde_json::json;

    use super::*;

    #[test]
    fn condition_roundtrip() {
        let json = json!({"op": "eq", "field": "namespace", "value": "spans"});
        assert_debug_snapshot!(
            serde_json::from_value::<Condition>(json).unwrap(),
            @r###"
        Eq(
            Namespace(
                Spans,
            ),
        )
        "###
        );
    }

    #[test]
    fn condition_multiple_namespaces() {
        let json = json!({
            "op": "or",
            "inner": [
                {"op": "eq", "field": "namespace", "value": "spans"},
                {"op": "eq", "field": "namespace", "value": "custom"}
            ]
        });

        let condition = serde_json::from_value::<Condition>(json).unwrap();
        assert!(condition.matches(Some(MetricNamespace::Spans)));
        assert!(condition.matches(Some(MetricNamespace::Custom)));
        assert!(!condition.matches(Some(MetricNamespace::Transactions)));
    }
}
