//! Routing logic for metrics. Metrics from different namespaces may be routed to different aggregators,
//! with their own limits, bucket intervals, etc.

use relay_config::aggregator::Condition;
use relay_config::{AggregatorServiceConfig, ScopedAggregatorConfig};
use relay_metrics::MetricNamespace;
use relay_system::{Addr, NoResponse, Recipient, Service, ServiceSpawnExt as _};

use crate::services::metrics::{
    Aggregator, AggregatorHandle, AggregatorService, FlushBuckets, MergeBuckets,
};
use crate::services::projects::cache::ProjectCacheHandle;
use crate::statsd::RelayTimers;
use crate::utils;

/// Service that routes metrics & metric buckets to the appropriate aggregator.
///
/// Each aggregator gets its own configuration.
/// Metrics are routed to the first aggregator which matches the configuration's [`Condition`].
/// If no condition matches, the metric/bucket is routed to the `default_aggregator`.
pub struct RouterService {
    handle: relay_system::Handle,
    default: AggregatorService,
    secondary: Vec<(AggregatorService, Condition)>,
}

impl RouterService {
    /// Create a new router service.
    pub fn new(
        handle: relay_system::Handle,
        default_config: AggregatorServiceConfig,
        secondary_configs: Vec<ScopedAggregatorConfig>,
        receiver: Option<Recipient<FlushBuckets, NoResponse>>,
        project_cache: ProjectCacheHandle,
    ) -> Self {
        let mut secondary = Vec::new();

        for c in secondary_configs {
            let service =
                AggregatorService::named(c.name, c.config, receiver.clone(), project_cache.clone());
            secondary.push((service, c.condition));
        }

        let default = AggregatorService::new(default_config, receiver, project_cache);
        Self {
            handle,
            default,
            secondary,
        }
    }

    pub fn handle(&self) -> RouterHandle {
        let mut handles = vec![self.default.handle()];
        for (aggregator, _) in &self.secondary {
            handles.push(aggregator.handle());
        }

        RouterHandle(handles)
    }
}

impl Service for RouterService {
    type Interface = Aggregator;

    async fn run(self, mut rx: relay_system::Receiver<Self::Interface>) {
        let mut router = StartedRouter::start(self);
        relay_log::info!("metrics router started");

        // Note that currently this loop never exits and will run till the tokio runtime shuts
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
            default,
            secondary,
            handle,
        } = router;

        let secondary = secondary
            .into_iter()
            .map(|(aggregator, condition)| {
                let namespaces: Vec<_> = MetricNamespace::all()
                    .into_iter()
                    .filter(|&namespace| condition.matches(Some(namespace)))
                    .collect();

                (handle.start(aggregator), namespaces)
            })
            .collect();

        Self {
            default: handle.start(default),
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
                    Aggregator::MergeBuckets(msg) => self.handle_merge_buckets(msg),
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

/// Provides sync access to the state of the [`RouterService`].
#[derive(Clone, Debug)]
pub struct RouterHandle(Vec<AggregatorHandle>);

impl RouterHandle {
    /// Returns `true` if all the aggregators can still accept metrics.
    pub fn can_accept_metrics(&self) -> bool {
        self.0.iter().all(|ah| ah.can_accept_metrics())
    }
}
