use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hashbrown::HashMap;
use relay_base_schema::project::ProjectKey;
use relay_config::AggregatorServiceConfig;
use relay_metrics::aggregator::{self, AggregateMetricsError, FlushDecision};
use relay_metrics::{Bucket, UnixTimestamp};
use relay_quotas::{RateLimits, Scoping};
use relay_system::{Controller, FromMessage, Interface, NoResponse, Recipient, Service, Shutdown};

use crate::services::projects::cache::ProjectCacheHandle;
use crate::services::projects::project::{ProjectInfo, ProjectState};
use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};

/// Aggregator for metric buckets.
///
/// Buckets are flushed to a receiver after their time window and a grace period have passed.
/// Metrics with a recent timestamp are given a longer grace period than backdated metrics, which
/// are flushed after a shorter debounce delay. See [`AggregatorServiceConfig`] for configuration options.
///
/// Internally, the aggregator maintains a continuous flush cycle every 100ms. It guarantees that
/// all elapsed buckets belonging to the same [`ProjectKey`] are flushed together.
///
/// Receivers must implement a handler for the [`FlushBuckets`] message.
#[derive(Debug)]
pub enum Aggregator {
    /// Merge the buckets.
    MergeBuckets(MergeBuckets),

    /// Message is used only for tests to get the current number of buckets in `AggregatorService`.
    #[cfg(test)]
    BucketCountInquiry(BucketCountInquiry, relay_system::Sender<usize>),
}

impl Aggregator {
    /// Returns the name of the message variant.
    pub fn variant(&self) -> &'static str {
        match self {
            Aggregator::MergeBuckets(_) => "MergeBuckets",
            #[cfg(test)]
            Aggregator::BucketCountInquiry(_, _) => "BucketCountInquiry",
        }
    }
}

impl Interface for Aggregator {}

impl FromMessage<MergeBuckets> for Aggregator {
    type Response = NoResponse;
    fn from_message(message: MergeBuckets, _: ()) -> Self {
        Self::MergeBuckets(message)
    }
}

#[cfg(test)]
impl FromMessage<BucketCountInquiry> for Aggregator {
    type Response = relay_system::AsyncResponse<usize>;
    fn from_message(message: BucketCountInquiry, sender: relay_system::Sender<usize>) -> Self {
        Self::BucketCountInquiry(message, sender)
    }
}

/// Used only for testing the `AggregatorService`.
#[cfg(test)]
#[derive(Debug)]
pub struct BucketCountInquiry;

/// A message containing a vector of buckets to be flushed.
///
/// Handlers must respond to this message with a `Result`:
/// - If flushing has succeeded or the buckets should be dropped for any reason, respond with `Ok`.
/// - If flushing fails and should be retried at a later time, respond with `Err` containing the
///   failed buckets. They will be merged back into the aggregator and flushed at a later time.
#[derive(Clone, Debug)]
pub struct FlushBuckets {
    /// The partition to which the buckets belong.
    ///
    /// When set to `Some` it means that partitioning was enabled in the [`Aggregator`].
    pub partition_key: Option<u64>,
    /// The buckets to be flushed.
    pub buckets: HashMap<ProjectKey, ProjectBuckets>,
}

/// Metric buckets with additional project.
#[derive(Debug, Clone)]
pub struct ProjectBuckets {
    /// The metric buckets to encode.
    pub buckets: Vec<Bucket>,
    /// Scoping of the project.
    pub scoping: Scoping,
    /// Project info for extracting quotas.
    pub project_info: Arc<ProjectInfo>,
    /// Currently cached rate limits.
    pub rate_limits: Arc<RateLimits>,
}

impl Extend<Bucket> for ProjectBuckets {
    fn extend<T: IntoIterator<Item = Bucket>>(&mut self, iter: T) {
        self.buckets.extend(iter)
    }
}

enum AggregatorState {
    Running,
    ShuttingDown,
}

/// Service implementing the [`Aggregator`] interface.
pub struct AggregatorService {
    aggregator: aggregator::Aggregator,
    state: AggregatorState,
    receiver: Option<Recipient<FlushBuckets, NoResponse>>,
    project_cache: ProjectCacheHandle,
    flush_interval_ms: u64,
    can_accept_metrics: Arc<AtomicBool>,
}

impl AggregatorService {
    /// Create a new aggregator service and connect it to `receiver`.
    ///
    /// The aggregator will flush a list of buckets to the receiver in regular intervals based on
    /// the given `config`.
    pub fn new(
        config: AggregatorServiceConfig,
        receiver: Option<Recipient<FlushBuckets, NoResponse>>,
        project_cache: ProjectCacheHandle,
    ) -> Self {
        Self::named("default".to_owned(), config, receiver, project_cache)
    }

    /// Like [`Self::new`], but with a provided name.
    pub(crate) fn named(
        name: String,
        config: AggregatorServiceConfig,
        receiver: Option<Recipient<FlushBuckets, NoResponse>>,
        project_cache: ProjectCacheHandle,
    ) -> Self {
        let aggregator = aggregator::Aggregator::named(name, config.aggregator);
        Self {
            receiver,
            state: AggregatorState::Running,
            flush_interval_ms: config.flush_interval_ms,
            can_accept_metrics: Arc::new(AtomicBool::new(!aggregator.totals_cost_exceeded())),
            aggregator,
            project_cache,
        }
    }

    pub fn handle(&self) -> AggregatorHandle {
        AggregatorHandle {
            can_accept_metrics: Arc::clone(&self.can_accept_metrics),
        }
    }

    /// Sends the [`FlushBuckets`] message to the receiver in the fire and forget fashion. It is up
    /// to the receiver to send the [`MergeBuckets`] message back if buckets could not be flushed
    /// and we require another re-try.
    ///
    /// If `force` is true, flush all buckets unconditionally and do not attempt to merge back.
    fn try_flush(&mut self) {
        let force_flush = matches!(&self.state, AggregatorState::ShuttingDown);

        let flush_decision = |project_key| {
            let project = self.project_cache.get(project_key);

            let project_info = match project.state() {
                ProjectState::Enabled(project_info) => project_info,
                ProjectState::Disabled => return FlushDecision::Drop,
                // Delay the flush, project is not yet ready.
                //
                // Querying the project will make sure it is eventually fetched.
                ProjectState::Pending => {
                    relay_statsd::metric!(
                        counter(RelayCounters::ProjectStateFlushMetricsNoProject) += 1
                    );
                    return FlushDecision::Delay;
                }
            };

            let Some(scoping) = project_info.scoping(project_key) else {
                // This should never happen, at this point we should always have a valid
                // project with the necessary information to construct a scoping.
                //
                // Ideally we enforce this through the type system in the future.
                relay_log::error!(
                    tags.project_key = project_key.as_str(),
                    "dropping buckets because of missing scope",
                );
                return FlushDecision::Drop;
            };

            FlushDecision::Flush(ProjectBuckets {
                scoping,
                project_info: Arc::clone(project_info),
                rate_limits: project.rate_limits().current_limits(),
                buckets: Vec::new(),
            })
        };

        let partitions = self
            .aggregator
            .pop_flush_buckets(force_flush, flush_decision);

        self.can_accept_metrics
            .store(!self.aggregator.totals_cost_exceeded(), Ordering::Relaxed);

        if partitions.is_empty() {
            return;
        }

        let partitions_count = partitions.len() as u64;
        relay_log::trace!("flushing {} partitions to receiver", partitions_count);
        relay_statsd::metric!(
            histogram(RelayHistograms::PartitionsFlushed) = partitions_count,
            aggregator = self.aggregator.name(),
        );

        let mut total_bucket_count = 0u64;
        for buckets_by_project in partitions.values() {
            for buckets in buckets_by_project.values() {
                let bucket_count = buckets.buckets.len() as u64;
                total_bucket_count += bucket_count;

                relay_statsd::metric!(
                    histogram(RelayHistograms::BucketsFlushedPerProject) = bucket_count,
                    aggregator = self.aggregator.name(),
                );
            }
        }

        relay_statsd::metric!(
            histogram(RelayHistograms::BucketsFlushed) = total_bucket_count,
            aggregator = self.aggregator.name(),
        );

        if let Some(ref receiver) = self.receiver {
            for (partition_key, buckets_by_project) in partitions {
                receiver.send(FlushBuckets {
                    partition_key,
                    buckets: buckets_by_project,
                })
            }
        }
    }

    fn handle_merge_buckets(&mut self, msg: MergeBuckets) {
        let MergeBuckets {
            project_key,
            buckets,
        } = msg;

        let now = UnixTimestamp::now();
        for bucket in buckets.into_iter() {
            match self.aggregator.merge_with_options(project_key, bucket, now) {
                // Ignore invalid timestamp errors.
                Err(AggregateMetricsError::InvalidTimestamp(_)) => {}
                Err(AggregateMetricsError::TotalLimitExceeded) => {
                    relay_log::error!(
                        tags.aggregator = self.aggregator.name(),
                        "aggregator limit exceeded"
                    );
                    self.can_accept_metrics.store(false, Ordering::Relaxed);
                    break;
                }
                Err(AggregateMetricsError::ProjectLimitExceeded) => {
                    relay_log::error!(
                        tags.aggregator = self.aggregator.name(),
                        tags.project_key = project_key.as_str(),
                        "project metrics limit exceeded for project {project_key}"
                    );
                    break;
                }
                Err(error) => {
                    relay_log::error!(
                        tags.aggregator = self.aggregator.name(),
                        tags.project_key = project_key.as_str(),
                        bucket.error = &error as &dyn std::error::Error,
                        "failed to aggregate metric bucket"
                    );
                }
                Ok(()) => {}
            };
        }
    }

    fn handle_message(&mut self, message: Aggregator) {
        match message {
            Aggregator::MergeBuckets(msg) => self.handle_merge_buckets(msg),
            #[cfg(test)]
            Aggregator::BucketCountInquiry(_, sender) => {
                sender.send(self.aggregator.bucket_count())
            }
        }
    }

    fn handle_shutdown(&mut self, message: Shutdown) {
        if message.timeout.is_some() {
            self.state = AggregatorState::ShuttingDown;
        }
    }
}

impl Service for AggregatorService {
    type Interface = Aggregator;

    async fn run(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        let mut ticker = tokio::time::interval(Duration::from_millis(self.flush_interval_ms));
        let mut shutdown = Controller::shutdown_handle();

        macro_rules! timed {
            ($task:expr, $body:expr) => {{
                let task_name = $task;
                relay_statsd::metric!(
                    timer(RelayTimers::AggregatorServiceDuration),
                    task = task_name,
                    aggregator = self.aggregator.name(),
                    { $body }
                )
            }};
        }

        // Note that currently this loop never exits and will run till the tokio runtime shuts
        // down. This is about to change with the refactoring for the shutdown process.
        loop {
            tokio::select! {
                biased;

                _ = ticker.tick() => timed!(
                    "try_flush",
                    if cfg!(test) {
                        // Tests are running in a single thread / current thread runtime,
                        // which is required for 'fast-forwarding' and `block_in_place`
                        // requires a multi threaded runtime. Relay always requires a multi
                        // threaded runtime.
                        self.try_flush()
                    } else {
                        tokio::task::block_in_place(|| self.try_flush())
                    }
                ),
                Some(message) = rx.recv() => timed!(message.variant(), self.handle_message(message)),
                shutdown = shutdown.notified() => timed!("shutdown", self.handle_shutdown(shutdown)),

                else => break,
            }
        }
    }
}

impl Drop for AggregatorService {
    fn drop(&mut self) {
        let remaining_buckets = self.aggregator.bucket_count();
        if remaining_buckets > 0 {
            relay_log::error!(
                tags.aggregator = self.aggregator.name(),
                "metrics aggregator dropping {remaining_buckets} buckets"
            );
            relay_statsd::metric!(
                counter(RelayCounters::BucketsDropped) += remaining_buckets as i64,
                aggregator = self.aggregator.name(),
            );
        }
    }
}

/// A message containing a list of [`Bucket`]s to be inserted into the aggregator.
#[derive(Debug)]
pub struct MergeBuckets {
    pub project_key: ProjectKey,
    pub buckets: Vec<Bucket>,
}

impl MergeBuckets {
    /// Creates a new message containing a list of [`Bucket`]s.
    pub fn new(project_key: ProjectKey, buckets: Vec<Bucket>) -> Self {
        Self {
            project_key,
            buckets,
        }
    }
}

/// Provides sync access to the state of the [`AggregatorService`].
#[derive(Debug, Clone)]
pub struct AggregatorHandle {
    can_accept_metrics: Arc<AtomicBool>,
}

impl AggregatorHandle {
    /// Returns `true` if the aggregator can still accept metrics.
    pub fn can_accept_metrics(&self) -> bool {
        self.can_accept_metrics.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, RwLock};

    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::ProjectId;
    use relay_common::time::UnixTimestamp;
    use relay_metrics::{aggregator::AggregatorConfig, BucketMetadata, BucketValue};

    use super::*;

    #[derive(Default)]
    struct ReceivedData {
        buckets: Vec<Bucket>,
    }

    struct TestInterface(FlushBuckets);

    impl Interface for TestInterface {}

    impl FromMessage<FlushBuckets> for TestInterface {
        type Response = NoResponse;

        fn from_message(message: FlushBuckets, _: ()) -> Self {
            Self(message)
        }
    }

    #[derive(Clone, Default)]
    struct TestReceiver {
        data: Arc<RwLock<ReceivedData>>,
        reject_all: bool,
    }

    impl TestReceiver {
        fn add_buckets(&self, buckets: HashMap<ProjectKey, ProjectBuckets>) {
            let buckets = buckets.into_values().flat_map(|s| s.buckets);
            self.data.write().unwrap().buckets.extend(buckets);
        }

        fn bucket_count(&self) -> usize {
            self.data.read().unwrap().buckets.len()
        }
    }

    impl Service for TestReceiver {
        type Interface = TestInterface;

        async fn run(self, mut rx: relay_system::Receiver<Self::Interface>) {
            while let Some(message) = rx.recv().await {
                let buckets = message.0.buckets;
                relay_log::debug!(?buckets, "received buckets");
                if !self.reject_all {
                    self.add_buckets(buckets);
                }
            }
        }
    }

    fn some_bucket() -> Bucket {
        let timestamp = UnixTimestamp::from_secs(999994711);
        Bucket {
            timestamp,
            width: 0,
            name: "c:transactions/foo".into(),
            value: BucketValue::counter(42.into()),
            tags: BTreeMap::new(),
            metadata: BucketMetadata::new(timestamp),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_flush_bucket() {
        relay_test::setup();

        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let receiver = TestReceiver::default();
        let recipient = receiver.clone().start_detached().recipient();
        let project_cache = ProjectCacheHandle::for_test();
        project_cache.test_set_project_state(
            project_key,
            ProjectState::Enabled({
                Arc::new(ProjectInfo {
                    // Minimum necessary to get a valid scoping.
                    project_id: Some(ProjectId::new(3)),
                    organization_id: Some(OrganizationId::new(1)),
                    ..Default::default()
                })
            }),
        );

        let config = AggregatorServiceConfig {
            aggregator: AggregatorConfig {
                bucket_interval: 1,
                initial_delay: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let aggregator =
            AggregatorService::new(config, Some(recipient), project_cache).start_detached();

        let mut bucket = some_bucket();
        bucket.timestamp = UnixTimestamp::now();

        aggregator.send(MergeBuckets::new(project_key, vec![bucket]));

        let buckets_count = aggregator.send(BucketCountInquiry).await.unwrap();
        // Let's check the number of buckets in the aggregator just after sending a
        // message.
        assert_eq!(buckets_count, 1);

        // Wait until flush delay has passed. It is up to 2s: 1s for the current bucket
        // and 1s for the flush shift. Adding 100ms buffer.
        tokio::time::sleep(Duration::from_millis(2100)).await;
        // receiver must have 1 bucket flushed
        assert_eq!(receiver.bucket_count(), 1);
    }
}
