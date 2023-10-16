//! Routing logic for metrics. Metrics from different namespaces may be routed to different aggregators,
//! with their own limits, bucket intervals, etc.

use std::collections::BTreeMap;

use relay_system::{NoResponse, Recipient, Sender, Service};
use serde::{Deserialize, Serialize};

use crate::aggregator::{self, FLUSH_INTERVAL};
use crate::aggregatorservice::{AggregatorService, FlushBuckets};
use crate::{Aggregator, AggregatorServiceConfig, MergeBuckets, MetricNamespace};

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
    Eq(Field),
}

/// Defines a field and a field value to compare to when a [`Condition`] is evaluated.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "field", content = "value", rename_all = "lowercase")]
pub enum Field {
    /// Field that allows comparison to a metric or bucket's namespace.
    Namespace(MetricNamespace),
}

enum AggregatorState {
    Running,
    ShuttingDown,
}

/// Service that routes metrics & metric buckets to the appropriate aggregator.
///
/// Each aggregator gets its own configuration.
/// Metrics are routed to the first aggregator which matches the configuration's [`Condition`].
/// If no condition matches, the metric/bucket is routed to the `default_aggregator`.
pub struct RouterService {
    default_aggregator: AggregatorService,
    secondary_aggregators: BTreeMap<MetricNamespace, AggregatorService>,
    max_flush_bytes: usize,
    max_total_bucket_bytes: Option<usize>,
    flush_partitions: Option<u64>,
    state: AggregatorState,
    receiver: Option<Recipient<FlushBuckets, NoResponse>>,
}

impl RouterService {
    /// Create a new router service.
    pub fn new(
        aggregator_config: AggregatorServiceConfig,
        secondary_aggregators: Vec<ScopedAggregatorConfig>,
        receiver: Option<Recipient<FlushBuckets, NoResponse>>,
    ) -> Self {
        Self {
            max_total_bucket_bytes: aggregator_config.max_total_bucket_bytes,
            max_flush_bytes: aggregator_config.max_flush_bytes,
            flush_partitions: aggregator_config.flush_partitions,
            default_aggregator: AggregatorService::new(aggregator_config, receiver.clone()),
            secondary_aggregators: secondary_aggregators
                .into_iter()
                .map(|c| {
                    let Condition::Eq(Field::Namespace(namespace)) = c.condition;
                    (
                        namespace,
                        AggregatorService::named(c.name, c.config, receiver.clone()),
                    )
                })
                .collect(),
            state: AggregatorState::Running,
            receiver,
        }
    }

    /// Sends the [`FlushBuckets`] message to the receiver in the fire and forget fashion. It is up
    /// to the receiver to send the [`MergeBuckets`] message back if buckets could not be flushed
    /// and we require another re-try.
    ///
    /// If `force` is true, flush all buckets unconditionally and do not attempt to merge back.
    fn try_flush(&mut self) {
        let force_flush = matches!(&self.state, &AggregatorState::ShuttingDown);
        let flush_partitions = None;
        let max_flush_bytes = 5;

        self.default_aggregator.aggregator().try_flush(
            force_flush,
            flush_partitions,
            max_flush_bytes,
            self.receiver.clone(),
        );

        for agg in &mut self.secondary_aggregators {
            agg.1.aggregator().try_flush(
                force_flush,
                flush_partitions,
                max_flush_bytes,
                self.receiver.clone(),
            );
        }
    }

    /*
    fn _handle_message(&mut self, msg: Aggregator) {
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
            Aggregator::MergeBuckets(msg) => self.handle_merge_buckets(msg),
            #[cfg(test)]
            Aggregator::BucketCountInquiry(_, _sender) => (), // not supported
        }
    }
    */

    fn handle_message(&mut self, msg: Aggregator) {
        match msg {
            Aggregator::AcceptsMetrics(_, sender) => self.handle_accepts_metrics(sender),
            Aggregator::MergeBuckets(msg) => self.handle_merge_buckets(msg),
            #[cfg(test)]
            Aggregator::BucketCountInquiry(_, sender) => {
                sender.send(self.default_aggregator.aggregator().bucket_count())
            }
        }
    }

    fn aggregators(&mut self) -> impl Iterator<Item = &mut aggregator::Aggregator> {
        std::iter::once(self.default_aggregator.aggregator()).chain(
            self.secondary_aggregators
                .iter_mut()
                .map(|(_, agg)| agg.aggregator()),
        )
    }

    fn handle_accepts_metrics(&mut self, sender: Sender<bool>) {
        let max_bytes = self.max_total_bucket_bytes;

        let accepts = self
            .aggregators()
            .all(|agg| agg.totals_cost_exceeded(max_bytes));

        sender.send(accepts);
    }

    fn handle_merge_buckets(&mut self, msg: MergeBuckets) {
        let MergeBuckets {
            project_key,
            buckets,
        } = msg;

        self.default_aggregator.aggregator().merge_all(
            project_key,
            buckets,
            self.max_total_bucket_bytes,
        );
    }

    /*
    fn _handle_merge_buckets(&mut self, message: MergeBuckets) {
        let metrics_by_namespace = message.buckets.into_iter().group_by(|bucket| {
            MetricResourceIdentifier::parse(&bucket.name)
                .map(|mri| mri.namespace)
                .ok()
        });

        // TODO: Parse MRI only once, move validation from Aggregator here.
        for (namespace, group) in metrics_by_namespace.into_iter() {
            let aggregator = self.get_aggregator(namespace);

            aggregator.send(MergeBuckets {
                project_key: message.project_key,
                buckets: group.collect(),
            });
        }
    }
    */
}

impl Service for RouterService {
    type Interface = Aggregator;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            relay_log::info!("metrics router started");

            let mut ticker = tokio::time::interval(FLUSH_INTERVAL);

            // Note that currently this loop never exists and will run till the tokio runtime shuts
            // down. This is about to change with the refactoring for the shutdown process.
            loop {
                tokio::select! {
                    biased;


                    _ = ticker.tick() => self.try_flush(),
                    Some(message) = rx.recv() => self.handle_message(message),

                    else => break,
                }
            }
            relay_log::info!("metrics router stopped");
        });
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
