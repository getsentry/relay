//! Routing logic for metrics. Metrics from different namespaces may be routed to different aggregators,
//! with their own limits, bucket intervals, etc.

use std::collections::BTreeMap;

use itertools::Itertools;
use relay_base_schema::project::ProjectKey;
use relay_system::{Addr, FromMessage, NoResponse, Recipient, Service};
use serde::{Deserialize, Serialize};

use crate::{
    AcceptsMetrics, Aggregator, AggregatorConfig, AggregatorService, Bucket, FlushBuckets,
    InsertMetrics, MergeBuckets, Metric, MetricNamespace, MetricResourceIdentifier,
    MetricsContainer,
};

/// Contains an [`AggregatorConfig`] for a specific scope.
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
    pub config: AggregatorConfig,
}

/// Condition that needs to be met for a metric or bucket to be routed to a
/// secondary aggregator.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Condition {
    /// Checks for equality on a specific field.
    Eq(Field),
}

/// Defines a field and a field value to compare to when a [`Condition`] is evaluated.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "field", content = "value", rename_all = "lowercase")]
pub enum Field {
    /// Field that allows comparison to a metric or bucket's namespace.
    Namespace(MetricNamespace),
}

/// Service that routes metrics & metric buckets to the appropriate aggregator.
///
/// Each aggregator gets its own configuration.
/// Metrics are routed to the first aggregator which matches the configuration's [`Condition`].
/// If no condition matches, the metric/bucket is routed to the `default_aggregator`.
pub struct RouterService {
    default_aggregator: AggregatorService,
    secondary_aggregators: BTreeMap<MetricNamespace, AggregatorService>,
}

impl RouterService {
    /// Create a new router service.
    pub fn new(
        aggregator_config: AggregatorConfig,
        secondary_aggregators: Vec<ScopedAggregatorConfig>,
        receiver: Option<Recipient<FlushBuckets, NoResponse>>,
    ) -> Self {
        Self {
            default_aggregator: AggregatorService::new(aggregator_config, receiver.clone()),
            secondary_aggregators: secondary_aggregators
                .into_iter()
                .map(|c| {
                    let namespace = match c.condition {
                        Condition::Eq(Field::Namespace(namespace)) => namespace,
                    };
                    (
                        namespace,
                        AggregatorService::named(c.name, c.config, receiver.clone()),
                    )
                })
                .collect(),
        }
    }
}

impl Service for RouterService {
    type Interface = Aggregator;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut router = StartedRouter::start(self);
            relay_log::info!("metrics router started");

            // Note that currently this loop never exists and will run till the tokio runtime shuts
            // down. This is about to change with the refactoring for the shutdown process.
            loop {
                tokio::select! {
                    biased;

                    Some(message) = rx.recv() => router.handle_message(message),

                    else => break,
                }
            }
            relay_log::info!("metrics router stopped");
        });
    }
}

/// Generalization of [`InsertMetrics`] and [`MergeBuckets`].
///
/// Used to handle these messages generically.
trait Insert {
    type Item: MetricsContainer;
    fn into_parts(self) -> (ProjectKey, Vec<Self::Item>);
    fn from_parts(project_key: ProjectKey, items: Vec<Self::Item>) -> Self;
}

impl Insert for InsertMetrics {
    type Item = Metric;

    fn into_parts(self) -> (ProjectKey, Vec<Self::Item>) {
        let InsertMetrics {
            project_key,
            metrics,
        } = self;
        (project_key, metrics)
    }

    fn from_parts(project_key: ProjectKey, items: Vec<Self::Item>) -> Self {
        Self {
            project_key,
            metrics: items,
        }
    }
}

impl Insert for MergeBuckets {
    type Item = Bucket;

    fn into_parts(self) -> (ProjectKey, Vec<Self::Item>) {
        let MergeBuckets {
            project_key,
            buckets,
        } = self;
        (project_key, buckets)
    }

    fn from_parts(project_key: ProjectKey, items: Vec<Self::Item>) -> Self {
        Self {
            project_key,
            buckets: items,
        }
    }
}

/// Helper struct that holds the [`Addr`]s of started aggregators.
struct StartedRouter {
    default_aggregator: Addr<Aggregator>,
    secondary_aggregators: BTreeMap<MetricNamespace, Addr<Aggregator>>,
}

impl StartedRouter {
    fn start(router: RouterService) -> Self {
        Self {
            default_aggregator: router.default_aggregator.start(),
            secondary_aggregators: router
                .secondary_aggregators
                .into_iter()
                .map(|(key, service)| (key, service.start()))
                .collect(),
        }
    }

    fn handle_message(&mut self, msg: Aggregator) {
        match msg {
            Aggregator::AcceptsMetrics(_, sender) => {
                let requests: Vec<_> = Some(self.default_aggregator.send(AcceptsMetrics))
                    .into_iter()
                    .chain(
                        self.secondary_aggregators
                            .values_mut()
                            .map(|agg| agg.send(AcceptsMetrics)),
                    )
                    .collect();
                tokio::spawn(async {
                    let mut accepts = true;
                    for req in requests {
                        accepts &= req.await.unwrap_or_default();
                    }
                    sender.send(accepts);
                });
            }
            Aggregator::InsertMetrics(msg) => self.handle_metrics(msg),
            Aggregator::MergeBuckets(msg) => self.handle_metrics(msg),
            #[cfg(test)]
            Aggregator::BucketCountInquiry(_, _sender) => (), // not supported
        }
    }

    fn handle_metrics<M: Insert>(&mut self, message: M)
    where
        Aggregator: FromMessage<M>,
    {
        let (project_key, metrics) = message.into_parts();
        let metrics_by_namespace = metrics.into_iter().group_by(|m| {
            MetricResourceIdentifier::parse(m.name())
                .map(|mri| mri.namespace)
                .ok()
        });
        // TODO: Parse MRI only once, move validation from Aggregator here.
        for (namespace, group) in metrics_by_namespace.into_iter() {
            let agg = namespace
                .and_then(|ns| self.secondary_aggregators.get_mut(&ns))
                .unwrap_or(&mut self.default_aggregator);
            let message = M::from_parts(project_key, group.collect());
            agg.send(message);
        }
    }
}

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;
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
}
