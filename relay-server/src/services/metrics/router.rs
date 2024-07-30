//! Routing logic for metrics. Metrics from different namespaces may be routed to different aggregators,
//! with their own limits, bucket intervals, etc.

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use relay_config::{AggregatorServiceConfig, ScopedAggregatorConfig};
use relay_metrics::MetricNamespace;
use relay_system::{Addr, NoResponse, Recipient, Service};

use crate::services::metrics::{
    AcceptsMetrics, Aggregator, AggregatorService, FlushBuckets, MergeBuckets,
};
use crate::statsd::RelayTimers;
use crate::utils;

/// Service that routes metrics & metric buckets to the appropriate aggregator.
///
/// Each aggregator gets its own configuration.
/// Metrics are routed to the first aggregator which matches the configuration's
/// [`Condition`](relay_config::aggregator::Condition).
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

                    Some(message) = rx.recv() => {
                        router.handle_message(message)
                    },

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
    secondary: Vec<(Addr<Aggregator>, Vec<MetricNamespace>)>,
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
            .map(|(cond, agg)| {
                let namespaces: Vec<_> = MetricNamespace::all()
                    .into_iter()
                    .filter(|&namespace| cond.matches(Some(namespace)))
                    .collect();

                (agg, namespaces)
            })
            .collect();

        Self {
            default: AggregatorService::new(default_config, receiver).start(),
            secondary,
        }
    }

    fn handle_message(&mut self, message: Aggregator) {
        let ty = message.variant();
        relay_statsd::metric!(
            timer(RelayTimers::MetricRouterServiceDuration),
            message = ty,
            {
                match message {
                    Aggregator::AcceptsMetrics(_, sender) => {
                        let mut requests = self
                            .secondary
                            .iter()
                            .map(|(agg, _)| agg.send(AcceptsMetrics))
                            .chain(Some(self.default.send(AcceptsMetrics)))
                            .collect::<FuturesUnordered<_>>();

                        tokio::spawn(async move {
                            let mut accepts = true;
                            while let Some(req) = requests.next().await {
                                accepts &= req.unwrap_or_default();
                            }
                            sender.send(accepts);
                        });
                    }
                    Aggregator::MergeBuckets(msg) => self.handle_merge_buckets(msg),
                    #[cfg(test)]
                    Aggregator::BucketCountInquiry(_, _sender) => (), // not supported
                }
            }
        )
    }

    fn handle_merge_buckets(&mut self, message: MergeBuckets) {
        let MergeBuckets {
            project_key,
            mut buckets,
        } = message;

        for (aggregator, namespaces) in &self.secondary {
            let matching;
            (buckets, matching) = utils::split_off(buckets, |bucket| {
                bucket
                    .name
                    .try_namespace()
                    .map(|namespace| namespaces.contains(&namespace))
                    .unwrap_or(false)
            });

            if !matching.is_empty() {
                aggregator.send(MergeBuckets::new(project_key, matching));
            }
        }

        if !buckets.is_empty() {
            self.default.send(MergeBuckets::new(project_key, buckets));
        }
    }
}
