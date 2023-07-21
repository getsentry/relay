//! Routing logic for metrics. Metrics from different namespaces may be routed to different aggregators,
//! with their own limits, bucket intervals, etc.

use std::collections::{BTreeMap, HashMap};

use relay_system::{Controller, NoResponse, Recipient, Service};

use crate::{
    Aggregator, AggregatorConfig, AggregatorService, FlushBuckets, InsertMetrics, MetricNamespace,
};

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

impl RouterService {
    fn handle_message(&mut self, msg: Aggregator) {
        match msg {
            Aggregator::AcceptsMetrics(_, sender) => self.handle_accepts_metrics(sender),
            Aggregator::InsertMetrics(InsertMetrics {
                project_key,
                metrics,
            }) => {
                for (ns, group) in metrics.into_iter().group_by(|m| m.namespace) {
                    let agg = self
                        .secondary_aggregators
                        .get_mut(ns)
                        .unwrap_or(&mut self.default_aggregator);
                    agg.agg.send(InsertMetrics(project_key, group));
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

impl Service for RouterService {
    type Interface = Aggregator;

    fn spawn_handler(self, rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut shutdown = Controller::shutdown_handle();
            relay_log::info!("metrics router started");

            // Note that currently this loop never exists and will run till the tokio runtime shuts
            // down. This is about to change with the refactoring for the shutdown process.
            loop {
                tokio::select! {
                    biased;

                    Some(message) = rx.recv() => self.handle_message(message),
                    shutdown = shutdown.notified() => self.handle_shutdown(shutdown),

                    else => break,
                }
            }
            relay_log::info!("metrics router stopped");
        });
    }
}
