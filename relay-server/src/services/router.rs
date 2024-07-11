//! Routing logic for metrics. Metrics from different namespaces may be routed to different aggregators,
//! with their own limits, bucket intervals, etc.

use itertools::Itertools;
use relay_config::{aggregator::Condition, AggregatorServiceConfig, ScopedAggregatorConfig};
use relay_system::{Addr, NoResponse, Recipient, Service};

use crate::services::aggregatorservice::{
    AcceptsMetrics, Aggregator, AggregatorService, FlushBuckets, MergeBuckets,
};

/// Service that routes metrics & metric buckets to the appropriate aggregator.
///
/// Each aggregator gets its own configuration.
/// Metrics are routed to the first aggregator which matches the configuration's [`Condition`].
/// If no condition matches, the metric/bucket is routed to the `default_aggregator`.
pub struct RouterService {
    default_config: AggregatorServiceConfig,
    secondary_configs: Vec<ScopedAggregatorConfig>,
    receiver: Option<Recipient<FlushBuckets, NoResponse>>,
}

impl RouterService {
    /// Create a new router service.
    pub fn new(
        default_config: AggregatorServiceConfig,
        secondary_configs: Vec<ScopedAggregatorConfig>,
        receiver: Option<Recipient<FlushBuckets, NoResponse>>,
    ) -> Self {
        Self {
            default_config,
            secondary_configs,
            receiver,
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

/// Helper struct that holds the [`Addr`]s of started aggregators.
struct StartedRouter {
    default: Addr<Aggregator>,
    secondary: Vec<(Condition, Addr<Aggregator>)>,
}

impl StartedRouter {
    fn start(router: RouterService) -> Self {
        let RouterService {
            default_config,
            secondary_configs,
            receiver,
        } = router;

        let secondary = secondary_configs
            .into_iter()
            .map(|c| {
                let addr = AggregatorService::named(c.name, c.config, receiver.clone()).start();
                (c.condition, addr)
            })
            .collect();

        Self {
            default: AggregatorService::new(default_config, receiver).start(),
            secondary,
        }
    }

    fn handle_message(&mut self, msg: Aggregator) {
        match msg {
            Aggregator::AcceptsMetrics(_, sender) => {
                let requests = self
                    .secondary
                    .iter()
                    .map(|(_, agg)| agg.send(AcceptsMetrics))
                    .chain(Some(self.default.send(AcceptsMetrics)))
                    .collect::<Vec<_>>();

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

    fn handle_merge_buckets(&mut self, message: MergeBuckets) {
        let metrics_by_namespace = message
            .buckets
            .into_iter()
            .group_by(|bucket| bucket.name.try_namespace());

        for (namespace, group) in metrics_by_namespace.into_iter() {
            let aggregator = self
                .secondary
                .iter()
                .find_map(|(cond, addr)| cond.matches(namespace).then_some(addr))
                .unwrap_or(&self.default);

            aggregator.send(MergeBuckets::new(message.project_key, group.collect()));
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
