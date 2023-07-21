//! Routing logic for metrics. Metrics from different namespaces may be routed to different aggregators,
//! with their own limits, bucket intervals, etc.

use std::collections::BTreeMap;

use itertools::Itertools;
use relay_system::{Addr, Controller, NoResponse, Recipient, Service};

use crate::{
    Aggregator, AggregatorConfig, AggregatorService, FlushBuckets, InsertMetrics, MetricNamespace,
    MetricResourceIdentifier,
};

/// Contains an [`AggregatorConfig`] for a specific scope.
///
/// For now, the only way to scope an aggregator is by [`MetricNamespace`].
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

    fn spawn_handler(self, rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut shutdown = Controller::shutdown_handle();
            let router = StartedRouter::start(self);
            relay_log::info!("metrics router started");

            // Note that currently this loop never exists and will run till the tokio runtime shuts
            // down. This is about to change with the refactoring for the shutdown process.
            loop {
                tokio::select! {
                    biased;

                    Some(message) = rx.recv() => router.handle_message(message),
                    shutdown = shutdown.notified() => router.handle_shutdown(shutdown),

                    else => break,
                }
            }
            relay_log::info!("metrics router stopped");
        });
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
                self.default_aggregator.handle_accepts_metrics(sender)
            }
            Aggregator::InsertMetrics(InsertMetrics {
                project_key,
                metrics,
            }) => {
                let metrics_by_namespace = metrics.into_iter().group_by(|m| {
                    MetricResourceIdentifier::parse(&m.name)
                        .map(|mri| mri.namespace)
                        .ok()
                });
                // TODO: Parse MRI only once, move validation from Aggregator here.
                for (namespace, group) in metrics_by_namespace.into_iter() {
                    let agg = namespace
                        .and_then(|ns| self.secondary_aggregators.get_mut(&ns))
                        .unwrap_or(&mut self.default_aggregator);
                    agg.
                }
            }
            Aggregator::MergeBuckets(msg) => self.handle_merge_buckets(msg),
            #[cfg(test)]
            Aggregator::BucketCountInquiry(_, sender) => (), // not supported
        }
    }

    fn handle_shutdown(&mut self, message: Shutdown) {
        if message.timeout.is_some() {
            self.state = AggregatorState::ShuttingDown;
        }
    }
}
