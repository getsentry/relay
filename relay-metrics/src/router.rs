//! Routing logic for metrics. Metrics from different namespaces may be routed to different aggregators,
//! with their own limits, bucket intervals, etc.

use std::collections::BTreeMap;

use itertools::Itertools;
use relay_common::ProjectKey;
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
#[derive(Debug, Deserialize, Serialize)]
pub struct ScopedAggregatorConfig {
    /// TODO: docs
    /// TODO: more generic condition
    pub namespace: MetricNamespace,
    /// TODO: docs
    pub config: AggregatorConfig,
}

/// TODO: docs
pub struct RouterService {
    default_aggregator: AggregatorService,
    secondary_aggregators: BTreeMap<MetricNamespace, AggregatorService>,
}

impl RouterService {
    /// TODO: docs
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
                    (
                        c.namespace,
                        AggregatorService::new(c.config, receiver.clone()),
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

/// Helper struct that keeps the [`Addr`]s of started aggregators.
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
                // TODO: Should ask secondary aggregators as well.
                let req = self.default_aggregator.send(AcceptsMetrics);
                tokio::spawn(async {
                    let res = req.await;
                    sender.send(res.unwrap_or_default());
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
