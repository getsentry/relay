use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use hashbrown::HashMap;
use hashbrown::hash_map::Entry;
use relay_base_schema::project::ProjectKey;
use relay_config::AggregatorServiceConfig;
use relay_metrics::Bucket;
use relay_metrics::aggregator::{self, AggregateMetricsError, AggregatorConfig, Partition};
use relay_quotas::{RateLimits, Scoping};
use relay_system::{Controller, FromMessage, Interface, NoResponse, Recipient, Service};
use tokio::time::{Instant, Sleep};

use crate::services::projects::cache::ProjectCacheHandle;
use crate::services::projects::project::{ProjectInfo, ProjectState};
use crate::statsd::{RelayCounters, RelayTimers};

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
}

impl Aggregator {
    /// Returns the name of the message variant.
    pub fn variant(&self) -> &'static str {
        match self {
            Aggregator::MergeBuckets(_) => "MergeBuckets",
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

/// A message containing a vector of buckets to be flushed.
///
/// Handlers must respond to this message with a `Result`:
/// - If flushing has succeeded or the buckets should be dropped for any reason, respond with `Ok`.
/// - If flushing fails and should be retried at a later time, respond with `Err` containing the
///   failed buckets. They will be merged back into the aggregator and flushed at a later time.
#[derive(Clone, Debug)]
pub struct FlushBuckets {
    /// The partition to which the buckets belong.
    pub partition_key: u32,
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

/// Service implementing the [`Aggregator`] interface.
pub struct AggregatorService {
    aggregator: aggregator::Aggregator,
    receiver: Option<Recipient<FlushBuckets, NoResponse>>,
    project_cache: ProjectCacheHandle,
    config: AggregatorServiceConfig,
    can_accept_metrics: Arc<AtomicBool>,
    next_flush: Pin<Box<Sleep>>,
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
        let aggregator = aggregator::Aggregator::named(name, &config.aggregator);
        Self {
            receiver,
            config,
            can_accept_metrics: Arc::new(AtomicBool::new(true)),
            aggregator,
            project_cache,
            next_flush: Box::pin(tokio::time::sleep(Duration::from_secs(0))),
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
    /// Returns when the next flush should be attempted.
    fn try_flush(&mut self) -> Duration {
        let partition = match self.aggregator.try_flush_next(SystemTime::now()) {
            Ok(partition) => partition,
            Err(duration) => return duration,
        };
        self.can_accept_metrics.store(true, Ordering::Relaxed);

        self.flush_partition(partition);

        self.aggregator.next_flush_at(SystemTime::now())
    }

    fn flush_partition(&mut self, partition: Partition) {
        let Some(receiver) = &self.receiver else {
            return;
        };

        let mut buckets_by_project = hashbrown::HashMap::new();

        let partition_key = partition.partition_key;
        for (project_key, bucket) in partition {
            let s = match buckets_by_project.entry(project_key) {
                Entry::Occupied(occupied_entry) => occupied_entry.into_mut(),
                Entry::Vacant(vacant_entry) => {
                    let project = self.project_cache.get(project_key);

                    let project_info = match project.state() {
                        ProjectState::Enabled(info) => Arc::clone(info),
                        ProjectState::Disabled => continue, // Drop the bucket.
                        ProjectState::Pending => {
                            // Return to the aggregator, which will assign a new flush time.
                            if let Err(error) = self.aggregator.merge(project_key, bucket) {
                                relay_log::error!(
                                    tags.aggregator = self.aggregator.name(),
                                    tags.project_key = project_key.as_str(),
                                    bucket.error = &error as &dyn std::error::Error,
                                    "failed to return metric bucket back to the aggregator"
                                );
                            }
                            relay_statsd::metric!(
                                counter(RelayCounters::ProjectStateFlushMetricsNoProject) += 1
                            );
                            continue;
                        }
                    };

                    let rate_limits = project.rate_limits().current_limits();
                    let Some(scoping) = project_info.scoping(project_key) else {
                        // This should never happen, at this point we should always have a valid
                        // project with the necessary information to construct a scoping.
                        //
                        // Ideally we enforce this through the type system in the future.
                        relay_log::error!(
                            tags.project_key = project_key.as_str(),
                            "dropping buckets because of missing scope",
                        );
                        continue;
                    };

                    vacant_entry.insert(ProjectBuckets {
                        buckets: Vec::new(),
                        scoping,
                        project_info,
                        rate_limits,
                    })
                }
            };

            s.buckets.push(bucket);
        }

        if !buckets_by_project.is_empty() {
            relay_log::debug!(
                "flushing buckets for {} projects in partition {partition_key}",
                buckets_by_project.len()
            );

            receiver.send(FlushBuckets {
                partition_key,
                buckets: buckets_by_project,
            });
        }
    }

    fn handle_merge_buckets(&mut self, msg: MergeBuckets) {
        let MergeBuckets {
            project_key,
            buckets,
        } = msg;

        for mut bucket in buckets.into_iter() {
            if !validate_bucket(&mut bucket, &self.config) {
                continue;
            };

            match self.aggregator.merge(project_key, bucket) {
                // Ignore invalid timestamp errors and drop the bucket.
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
        }
    }

    fn handle_shutdown(&mut self) {
        relay_log::info!(
            "Shutting down metrics aggregator {}",
            self.aggregator.name()
        );

        // Create a new aggregator with very aggressive flush parameters.
        let aggregator = aggregator::Aggregator::named(
            self.aggregator.name().to_owned(),
            &AggregatorConfig {
                bucket_interval: 1,
                aggregator_size: 1,
                initial_delay: 0,
                ..self.config.aggregator
            },
        );

        let previous = std::mem::replace(&mut self.aggregator, aggregator);

        let mut partitions = 0;
        for partition in previous.into_partitions() {
            self.flush_partition(partition);
            partitions += 1;
        }
        relay_log::debug!("Force flushed {partitions} partitions");

        // Reset the next flush time, to the time of the new aggregator.
        self.next_flush
            .as_mut()
            .reset(Instant::now() + self.aggregator.next_flush_at(SystemTime::now()));
    }
}

impl Service for AggregatorService {
    type Interface = Aggregator;

    async fn run(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
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

        loop {
            tokio::select! {
                biased;

                _ = &mut self.next_flush => timed!(
                    "try_flush", {
                        let next = self.try_flush();
                        self.next_flush.as_mut().reset(Instant::now() + next);
                    }
                ),
                Some(message) = rx.recv() => timed!(message.variant(), self.handle_message(message)),
                _ = shutdown.notified() => timed!("shutdown", self.handle_shutdown()),

                else => break,
            }
        }
    }
}

impl Drop for AggregatorService {
    fn drop(&mut self) {
        if !self.aggregator.is_empty() {
            relay_log::error!(
                tags.aggregator = self.aggregator.name(),
                "metrics aggregator dropping buckets"
            );
            relay_statsd::metric!(
                counter(RelayCounters::BucketsDropped) += 1,
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

/// Validates the metric name and its tags are correct.
///
/// Returns `false` if the metric should be dropped.
fn validate_bucket(bucket: &mut Bucket, config: &AggregatorServiceConfig) -> bool {
    if bucket.name.len() > config.max_name_length {
        relay_log::debug!(
            "Invalid metric name, too long (> {}): {:?}",
            config.max_name_length,
            bucket.name
        );
        return false;
    }

    bucket.tags.retain(|tag_key, tag_value| {
        if tag_key.len() > config.max_tag_key_length {
            relay_log::debug!("Invalid metric tag key {tag_key:?}");
            return false;
        }
        if bytecount::num_chars(tag_value.as_bytes()) > config.max_tag_value_length {
            relay_log::debug!("Invalid metric tag value");
            return false;
        }

        true
    });

    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, RwLock};

    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::ProjectId;
    use relay_common::time::UnixTimestamp;
    use relay_metrics::{BucketMetadata, BucketValue, aggregator::AggregatorConfig};

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

        // Nothing flushed.
        assert_eq!(receiver.bucket_count(), 0);

        // Wait until flush delay has passed. It is up to 2s: 1s for the current bucket
        // and 1s for the flush shift. Adding 100ms buffer.
        tokio::time::sleep(Duration::from_millis(2100)).await;
        // receiver must have 1 bucket flushed
        assert_eq!(receiver.bucket_count(), 1);
    }

    fn test_config() -> AggregatorServiceConfig {
        AggregatorServiceConfig {
            max_name_length: 200,
            max_tag_key_length: 200,
            max_tag_value_length: 200,
            ..Default::default()
        }
    }

    #[test]
    fn test_validate_bucket_key_str_length() {
        relay_test::setup();
        let mut short_metric = Bucket {
            timestamp: UnixTimestamp::now(),
            name: "c:transactions/a_short_metric".into(),
            tags: BTreeMap::new(),
            metadata: Default::default(),
            width: 0,
            value: BucketValue::Counter(0.into()),
        };
        assert!(validate_bucket(&mut short_metric, &test_config()));

        let mut long_metric = Bucket {
            timestamp: UnixTimestamp::now(),
            name: "c:transactions/long_name_a_very_long_name_its_super_long_really_but_like_super_long_probably_the_longest_name_youve_seen_and_even_the_longest_name_ever_its_extremly_long_i_cant_tell_how_long_it_is_because_i_dont_have_that_many_fingers_thus_i_cant_count_the_many_characters_this_long_name_is".into(),
            tags: BTreeMap::new(),
            metadata: Default::default(),
            width: 0,
            value: BucketValue::Counter(0.into()),
        };
        assert!(!validate_bucket(&mut long_metric, &test_config()));

        let mut short_metric_long_tag_key  = Bucket {
            timestamp: UnixTimestamp::now(),
            name: "c:transactions/a_short_metric_with_long_tag_key".into(),
            tags: BTreeMap::from([("i_run_out_of_creativity_so_here_we_go_Lorem_Ipsum_is_simply_dummy_text_of_the_printing_and_typesetting_industry_Lorem_Ipsum_has_been_the_industrys_standard_dummy_text_ever_since_the_1500s_when_an_unknown_printer_took_a_galley_of_type_and_scrambled_it_to_make_a_type_specimen_book".into(), "tag_value".into())]),
            metadata: Default::default(),
            width: 0,
            value: BucketValue::Counter(0.into()),
        };
        assert!(validate_bucket(
            &mut short_metric_long_tag_key,
            &test_config()
        ));
        assert_eq!(short_metric_long_tag_key.tags.len(), 0);

        let mut short_metric_long_tag_value  = Bucket {
            timestamp: UnixTimestamp::now(),
            name: "c:transactions/a_short_metric_with_long_tag_value".into(),
            tags: BTreeMap::from([("tag_key".into(), "i_run_out_of_creativity_so_here_we_go_Lorem_Ipsum_is_simply_dummy_text_of_the_printing_and_typesetting_industry_Lorem_Ipsum_has_been_the_industrys_standard_dummy_text_ever_since_the_1500s_when_an_unknown_printer_took_a_galley_of_type_and_scrambled_it_to_make_a_type_specimen_book".into())]),
            metadata: Default::default(),
            width: 0,
            value: BucketValue::Counter(0.into()),
        };
        assert!(validate_bucket(
            &mut short_metric_long_tag_value,
            &test_config()
        ));
        assert_eq!(short_metric_long_tag_value.tags.len(), 0);
    }

    #[test]
    fn test_validate_tag_values_special_chars() {
        relay_test::setup();

        let tag_value = "x".repeat(199) + "ø";
        assert_eq!(tag_value.chars().count(), 200); // Should be allowed

        let mut short_metric = Bucket {
            timestamp: UnixTimestamp::now(),
            name: "c:transactions/a_short_metric".into(),
            tags: BTreeMap::from([("foo".into(), tag_value.clone())]),
            metadata: Default::default(),
            width: 0,
            value: BucketValue::Counter(0.into()),
        };
        assert!(validate_bucket(&mut short_metric, &test_config()));
        assert_eq!(short_metric.tags["foo"], tag_value);
    }
}
