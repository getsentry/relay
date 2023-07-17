//! Routing logic for metrics. Metrics from different namespaces may be routed to different aggregators,
//! with their own limits, bucket intervals, etc.

use std::collections::HashMap;

use crate::{Aggregator, AggregatorConfig, AggregatorService, MetricNamespace};

/// Contains an [`AggregatorConfig`] for a specific scope.
///
/// For now, the only way to scope an aggregator is by [`MetricNamespace`].
pub struct ScopedAggregatorConfig {
    /// TODO: docs
    pub namespace: MetricNamespace,
    /// TODO: docs
    pub config: AggregatorConfig,
}

/// TODO: docs
pub struct RouterService {
    default_aggregator: AggregatorService,
    secondary_aggregators: HashMap<MetricNamespace, AggregatorService>,
}

impl RouterService {
    pub fn new(
        aggregator_config: AggregatorConfig,
        secondary_aggregators: Vec<ScopedAggregatorConfig>,
    ) -> Self {
        Self {
            default_aggregator: AggregatorService::new(aggregator_config, receiver.clone()),
            secondary_aggregators: secondary_aggregators
                .into_iter()
                .map(|c| (c.namespace, c.config))
                .into(),
        }
    }
}
