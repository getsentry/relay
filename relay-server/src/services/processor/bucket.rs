use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use relay_base_schema::project::ProjectKey;
use relay_cogs::{Cogs, FeatureWeights, ResourceId};
use relay_config::{Config, EmitOutcomes, HttpEncoding, UpstreamDescriptor};
use relay_metrics::{BucketView, BucketsView};
use relay_quotas::Scoping;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, MessageHandler, NoResponse, Service};
use relay_threading::{AsyncPoolService, ServicePool};
use reqwest::header;

use super::common::{self, SubmitClientReports};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{PartialDsn, RequestMeta};
use crate::http;
use crate::managed::ManagedEnvelope;
use crate::metrics::{MetricOutcomes, MinimalTrackableBucket};
use crate::services::global_config::GlobalConfigHandle;
use crate::services::metrics::{FlushBuckets, ProjectBuckets};
use crate::services::outcome::{self, DiscardReason, Outcome};
use crate::services::projects::cache::ProjectCacheHandle;
use crate::services::upstream::{
    SendRequest, Sign, SignatureType, UpstreamRelay, UpstreamRequest, UpstreamRequestError,
};
use crate::statsd::{RelayDistributions, RelayTimers};
#[cfg(feature = "processing")]
use {
    crate::metrics::MetricsLimiter,
    crate::services::projects::project::ProjectInfo,
    crate::services::store::Store,
    crate::utils,
    itertools::Itertools,
    relay_dynamic_config::GlobalConfig,
    relay_metrics::{Bucket, MetricNamespace},
    relay_quotas::{Quota, RateLimits, RedisRateLimiter},
    std::time::Instant,
};

/// CPU-intensive processing tasks for metric buckets.
#[derive(Debug)]
pub enum BucketProcessor {
    Flush(FlushBuckets),
}

impl Interface for BucketProcessor {}

impl FromMessage<FlushBuckets> for BucketProcessor {
    type Response = NoResponse;

    fn from_message(message: FlushBuckets, _: ()) -> Self {
        Self::Flush(message)
    }
}

/// Contains the addresses of services that the bucket processor publishes to.
pub struct Addrs {
    pub upstream_relay: Addr<UpstreamRelay>,
    #[cfg(feature = "processing")]
    pub store_forwarder: Option<Addr<Store>>,
}

/// Creates a multi-threaded bucket processor.
#[cfg_attr(feature = "processing", expect(clippy::too_many_arguments))]
pub fn create_service(
    pool: ServicePool,
    config: Arc<Config>,
    global_config: GlobalConfigHandle,
    project_cache: ProjectCacheHandle,
    cogs: Cogs,
    #[cfg(feature = "processing")] rate_limiter: Option<RedisRateLimiter>,
    addrs: Addrs,
) -> impl Service<Interface = BucketProcessor> {
    AsyncPoolService::new(
        Arc::new(InnerProcessor::new(
            config,
            global_config,
            project_cache,
            cogs,
            #[cfg(feature = "processing")]
            rate_limiter,
            addrs,
        )),
        pool,
    )
}

struct InnerProcessor {
    config: Arc<Config>,
    #[cfg(feature = "processing")]
    global_config: GlobalConfigHandle,
    project_cache: ProjectCacheHandle,
    cogs: Cogs,
    addrs: Addrs,
    #[cfg(feature = "processing")]
    rate_limiter: Option<RedisRateLimiter>,
}

impl InnerProcessor {
    fn new(
        config: Arc<Config>,
        global_config: GlobalConfigHandle,
        project_cache: ProjectCacheHandle,
        cogs: Cogs,
        #[cfg(feature = "processing")] rate_limiter: Option<RedisRateLimiter>,
        addrs: Addrs,
    ) -> Self {
        #[cfg(not(feature = "processing"))]
        let _ = global_config;

        Self {
            config,
            #[cfg(feature = "processing")]
            global_config,
            project_cache,
            cogs,
            addrs,
            #[cfg(feature = "processing")]
            rate_limiter,
        }
    }

    #[cfg(feature = "processing")]
    async fn rate_limit_buckets(
        &self,
        scoping: Scoping,
        project_info: &ProjectInfo,
        mut buckets: Vec<Bucket>,
        metric_outcomes: &MetricOutcomes,
    ) -> Vec<Bucket> {
        let Some(rate_limiter) = &self.rate_limiter else {
            return buckets;
        };

        let global_config = self.global_config.current().unwrap_or_default();
        let mut namespaces = buckets
            .iter()
            .filter_map(|bucket| bucket.name.try_namespace())
            .counts();

        // Never rate limit outcomes.
        namespaces.remove(&MetricNamespace::Outcomes);

        let quotas = CombinedQuotas::new(&global_config, project_info.get_quotas());

        for (namespace, quantity) in namespaces {
            let item_scoping = scoping.metric_bucket(namespace);

            let limits = match rate_limiter
                .is_rate_limited(quotas, &item_scoping, quantity, false)
                .await
            {
                Ok(limits) => limits,
                Err(err) => {
                    relay_log::error!(
                        error = &err as &dyn std::error::Error,
                        "failed to check redis rate limits"
                    );
                    break;
                }
            };

            if limits.is_limited() {
                let rejected;
                (buckets, rejected) = utils::split_off(buckets, |bucket| {
                    bucket.name.try_namespace() == Some(namespace)
                });

                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                metric_outcomes.track(scoping, &rejected, Outcome::RateLimited(reason_code));

                self.project_cache
                    .get(item_scoping.scoping.project_key)
                    .rate_limits()
                    .merge(limits);
            }
        }

        match MetricsLimiter::create(buckets, project_info.config.quotas.clone(), scoping) {
            Err(buckets) => buckets,
            Ok(bucket_limiter) => {
                self.apply_other_rate_limits(bucket_limiter, metric_outcomes)
                    .await
            }
        }
    }

    /// Check and apply rate limits to metrics buckets for transactions and spans.
    #[cfg(feature = "processing")]
    async fn apply_other_rate_limits(
        &self,
        mut bucket_limiter: MetricsLimiter,
        metric_outcomes: &MetricOutcomes,
    ) -> Vec<Bucket> {
        relay_log::trace!("handle_rate_limit_buckets");

        let scoping = *bucket_limiter.scoping();

        if let Some(rate_limiter) = self.rate_limiter.as_ref() {
            let global_config = self.global_config.current().unwrap_or_default();
            let quotas = CombinedQuotas::new(&global_config, bucket_limiter.quotas());

            // We set over_accept_once such that the limit is actually reached, which allows subsequent
            // calls with quantity=0 to be rate limited.
            let over_accept_once = true;
            let mut rate_limits = RateLimits::new();

            let (category, count) = bucket_limiter.count();

            let timer = Instant::now();
            let mut is_limited = false;

            if let Some(count) = count {
                match rate_limiter
                    .is_rate_limited(quotas, &scoping.item(category), count, over_accept_once)
                    .await
                {
                    Ok(limits) => {
                        is_limited = limits.is_limited();
                        rate_limits.merge(limits)
                    }
                    Err(e) => {
                        relay_log::error!(error = &e as &dyn Error, "rate limiting error")
                    }
                }
            }

            relay_statsd::metric!(
                timer(RelayTimers::RateLimitBucketsDuration) = timer.elapsed(),
                category = category.name(),
                limited = if is_limited { "true" } else { "false" },
                count = match count {
                    None => "none",
                    Some(0) => "0",
                    Some(1) => "1",
                    Some(1..=10) => "10",
                    Some(1..=25) => "25",
                    Some(1..=50) => "50",
                    Some(51..=100) => "100",
                    Some(101..=500) => "500",
                    _ => "> 500",
                },
            );

            if rate_limits.is_limited() {
                let was_enforced = bucket_limiter.enforce_limits(&rate_limits, metric_outcomes);

                if was_enforced {
                    // Update the rate limits in the project cache.
                    self.project_cache
                        .get(scoping.project_key)
                        .rate_limits()
                        .merge(rate_limits);
                }
            }
        }

        bucket_limiter.into_buckets()
    }

    /// Processes metric buckets and sends them to Kafka.
    ///
    /// This function runs the following steps:
    ///  - rate limiting
    ///  - emit billing outcomes
    ///  - submit to `StoreForwarder`
    #[cfg(feature = "processing")]
    async fn encode_metrics_processing(
        &self,
        message: FlushBuckets,
        store_forwarder: &Addr<Store>,
    ) {
        use crate::constants::DEFAULT_EVENT_RETENTION;
        use crate::services::store::StoreMetrics;

        let FlushBuckets {
            partition_key: _,
            buckets,
            outcome_aggregator,
        } = message;

        let metric_outcomes = MetricOutcomes::new(outcome_aggregator);

        for ProjectBuckets {
            buckets,
            scoping,
            project_info,
            ..
        } in buckets.into_values()
        {
            let mut buckets = self
                .rate_limit_buckets(scoping, &project_info, buckets, &metric_outcomes)
                .await;

            if buckets.is_empty() {
                continue;
            }

            // Emit metric billing outcomes.
            metric_outcomes.track_accepted_outcome(scoping, &mut buckets);

            let retention = project_info
                .config
                .event_retention
                .unwrap_or(DEFAULT_EVENT_RETENTION);

            // The store forwarder takes care of bucket splitting internally, so we can submit the
            // entire list of buckets. There is no batching needed here.
            store_forwarder.send(StoreMetrics {
                buckets,
                scoping,
                retention,
                metric_outcomes: metric_outcomes.clone(),
            });
        }
    }

    /// Serializes metric buckets to JSON and sends them to the upstream.
    ///
    /// This function runs the following steps:
    ///  - partitioning
    ///  - batching by configured size limit
    ///  - serialize to JSON and pack in an envelope
    ///
    /// Rate limiting runs only in processing Relays as it requires access to the central Redis instance.
    /// Cached rate limits are applied in the project cache already.
    fn encode_metrics_envelope(&self, message: FlushBuckets) {
        let FlushBuckets {
            partition_key,
            buckets,
            outcome_aggregator,
        } = message;

        let config = self.config.current();
        let batch_size = config.metrics_max_batch_size_bytes();
        let upstream = config.upstream();

        for ProjectBuckets {
            buckets,
            scoping,
            project_info,
            ..
        } in buckets.values()
        {
            let dsn = PartialDsn::outbound(scoping, upstream);

            relay_statsd::metric!(
                distribution(RelayDistributions::PartitionKeys) = u64::from(partition_key)
            );

            let mut num_batches = 0;
            for buckets in BucketsView::from(buckets).by_size(batch_size) {
                let mut envelope = Envelope::from_request(None, RequestMeta::outbound(dsn.clone()));

                let mut item = Item::new(ItemType::MetricBuckets);
                item.set_source_quantities(crate::metrics::extract_quantities(buckets));
                item.set_payload(ContentType::Json, serde_json::to_vec(&buckets).unwrap());
                envelope.add_item(item);

                let mut envelope = ManagedEnvelope::new(envelope, outcome_aggregator.clone());
                envelope
                    .set_partition_key(Some(partition_key))
                    .scope(*scoping);

                relay_statsd::metric!(
                    distribution(RelayDistributions::BucketsPerBatch) = buckets.len() as u64
                );

                common::submit_envelope_upstream(
                    envelope,
                    &config,
                    project_info.upstream.clone(),
                    &self.addrs.upstream_relay,
                    &self.project_cache,
                );
                num_batches += 1;
            }

            relay_statsd::metric!(
                distribution(RelayDistributions::BatchesPerPartition) = num_batches
            );
        }
    }

    /// Creates a [`SendMetricsRequest`] and sends it to the upstream relay.
    fn send_global_partition(
        &self,
        upstream: Option<UpstreamDescriptor>,
        partition_key: u32,
        partition: &mut Partition<'_>,
        metric_outcomes: &MetricOutcomes,
    ) {
        if partition.is_empty() {
            return;
        }

        let (unencoded, project_info) = partition.take();
        let http_encoding = self.config.current().http_encoding();
        let encoded = match common::encode_payload(&unencoded, http_encoding) {
            Ok(payload) => payload,
            Err(error) => {
                let error = &error as &dyn std::error::Error;
                relay_log::error!(error, "failed to encode metrics payload");
                return;
            }
        };

        let request = SendMetricsRequest {
            upstream,
            partition_key: partition_key.to_string(),
            unencoded,
            encoded,
            project_info,
            http_encoding,
            metric_outcomes: metric_outcomes.clone(),
        };

        self.addrs.upstream_relay.send(SendRequest(request));
    }

    /// Serializes metric buckets to JSON and sends them to the upstream via the global endpoint.
    ///
    /// This function is similar to [`Self::encode_metrics_envelope`], but sends a global batched
    /// payload directly instead of per-project Envelopes.
    ///
    /// This function runs the following steps:
    ///  - partitioning
    ///  - batching by configured size limit
    ///  - serialize to JSON
    ///  - submit directly to the upstream
    fn encode_metrics_global(&self, message: FlushBuckets) {
        let FlushBuckets {
            partition_key,
            buckets,
            outcome_aggregator,
        } = message;
        let metric_outcomes = MetricOutcomes::new(outcome_aggregator);

        let batch_size = self.config.current().metrics_max_batch_size_bytes();
        let mut partitions = BTreeMap::new();
        let mut partition_splits = 0;

        for ProjectBuckets {
            buckets,
            scoping,
            project_info,
            ..
        } in buckets.values()
        {
            let partition = match partitions.get_mut(&project_info.upstream) {
                Some(partition) => partition,
                None => partitions
                    .entry(project_info.upstream.clone())
                    .or_insert_with(|| Partition::new(batch_size)),
            };

            for bucket in buckets {
                let mut remaining = Some(BucketView::new(bucket));

                while let Some(bucket) = remaining.take() {
                    if let Some(next) = partition.insert(bucket, *scoping) {
                        // A part of the bucket could not be inserted. Take the partition and submit
                        // it immediately. Repeat until the final part was inserted. This should
                        // always result in a request, otherwise we would enter an endless loop.
                        self.send_global_partition(
                            project_info.upstream.clone(),
                            partition_key,
                            partition,
                            &metric_outcomes,
                        );
                        remaining = Some(next);
                        partition_splits += 1;
                    }
                }
            }
        }

        if partition_splits > 0 {
            metric!(distribution(RelayDistributions::PartitionSplits) = partition_splits);
        }

        for (upstream, mut partition) in partitions {
            self.send_global_partition(upstream, partition_key, &mut partition, &metric_outcomes);
        }
    }

    /// Removes all outcome metrics from `message` and sends them as client reports.
    ///
    /// Returns a new [`FlushBuckets`] message, without any outcome metrics remaining.
    fn encode_metrics_client_reports(&self, message: FlushBuckets) -> FlushBuckets {
        let FlushBuckets {
            partition_key,
            mut buckets,
            outcome_aggregator,
        } = message;
        for ProjectBuckets {
            buckets, scoping, ..
        } in buckets.values_mut()
        {
            let client_reports = outcome::metric::extract_client_reports(buckets).collect();

            common::submit_client_reports(
                SubmitClientReports {
                    client_reports,
                    scoping: *scoping,
                    outcome_aggregator: outcome_aggregator.clone(),
                },
                &self.config.current(),
                &self.addrs.upstream_relay,
                &self.project_cache,
            );
        }

        FlushBuckets {
            partition_key,
            buckets,
            outcome_aggregator,
        }
    }

    async fn handle_flush_buckets(&self, mut message: FlushBuckets) {
        let metric_outcomes = MetricOutcomes::new(message.outcome_aggregator.clone());
        for (project_key, pb) in message.buckets.iter_mut() {
            let buckets = std::mem::take(&mut pb.buckets);

            pb.buckets = common::check_buckets(
                *project_key,
                &pb.project_info,
                &pb.rate_limits,
                buckets,
                &metric_outcomes,
            );
        }

        let config = self.config.current();

        #[cfg(feature = "processing")]
        if config.processing_enabled()
            && let Some(ref store_forwarder) = self.addrs.store_forwarder
        {
            return self
                .encode_metrics_processing(message, store_forwarder)
                .await;
        }

        // Processing Relays never send outcomes as client reports, which is why this check is after
        // the processing check.
        if config.emit_outcomes() == EmitOutcomes::AsClientReports {
            // Remove client reports from metrics to be sent, if configured as client reports
            // and send them separately.
            message = self.encode_metrics_client_reports(message);
        }

        if config.http_global_metrics() {
            self.encode_metrics_global(message)
        } else {
            self.encode_metrics_envelope(message)
        }
    }

    #[cfg(all(test, feature = "processing"))]
    fn redis_rate_limiter_enabled(&self) -> bool {
        self.rate_limiter.is_some()
    }

    fn feature_weights(&self, message: &FlushBuckets) -> FeatureWeights {
        message
            .buckets
            .values()
            .map(|s| {
                if self.config.current().processing_enabled() {
                    // Processing does not encode the metrics but instead rate limit the metrics,
                    // which scales by count and not size.
                    relay_metrics::cogs::ByCount(&s.buckets).into()
                } else {
                    relay_metrics::cogs::BySize(&s.buckets).into()
                }
            })
            .fold(FeatureWeights::none(), FeatureWeights::merge)
    }
}

impl MessageHandler for InnerProcessor {
    type Interface = BucketProcessor;

    async fn handle_message(&self, BucketProcessor::Flush(message): Self::Interface) {
        let feature_weights = self.feature_weights(&message);

        metric!(
            timer(RelayTimers::ProcessMessageDuration),
            message = "FlushBuckets",
            {
                let _cogs = self.cogs.timed(ResourceId::Relay, feature_weights);
                self.handle_flush_buckets(message).await
            }
        )
    }
}

/// A container for metric buckets from multiple projects.
///
/// This container is used to send metrics to the upstream in global batches as part of the
/// [`FlushBuckets`] message if the `http.global_metrics` option is enabled. The container monitors
/// the size of all metrics and allows to split them into multiple batches. See
/// [`insert`](Self::insert) for more information.
#[derive(Debug)]
struct Partition<'a> {
    max_size: usize,
    remaining: usize,
    views: HashMap<ProjectKey, Vec<BucketView<'a>>>,
    project_info: HashMap<ProjectKey, Scoping>,
}

impl<'a> Partition<'a> {
    /// Creates a new partition with the given maximum size in bytes.
    pub fn new(size: usize) -> Self {
        Self {
            max_size: size,
            remaining: size,
            views: HashMap::new(),
            project_info: HashMap::new(),
        }
    }

    /// Inserts a bucket into the partition, splitting it if necessary.
    ///
    /// This function attempts to add the bucket to this partition. If the bucket does not fit
    /// entirely into the partition given its maximum size, the remaining part of the bucket is
    /// returned from this function call.
    ///
    /// If this function returns `Some(_)`, the partition is full and should be submitted to the
    /// upstream immediately. Use [`Self::take`] to retrieve the contents of the
    /// partition. Afterwards, the caller is responsible to call this function again with the
    /// remaining bucket until it is fully inserted.
    pub fn insert(&mut self, bucket: BucketView<'a>, scoping: Scoping) -> Option<BucketView<'a>> {
        let (current, next) = bucket.split(self.remaining, Some(self.max_size));

        if let Some(current) = current {
            self.remaining = self.remaining.saturating_sub(current.estimated_size());
            self.views
                .entry(scoping.project_key)
                .or_default()
                .push(current);

            self.project_info
                .entry(scoping.project_key)
                .or_insert(scoping);
        }

        next
    }

    /// Returns `true` if the partition does not hold any data.
    fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    /// Returns the serialized buckets for this partition.
    ///
    /// This empties the partition, so that it can be reused.
    fn take(&mut self) -> (Bytes, HashMap<ProjectKey, Scoping>) {
        #[derive(serde::Serialize)]
        struct Wrapper<'a> {
            buckets: &'a HashMap<ProjectKey, Vec<BucketView<'a>>>,
        }

        let buckets = &self.views;
        let payload = serde_json::to_vec(&Wrapper { buckets }).unwrap().into();

        let scopings = std::mem::take(&mut self.project_info);

        self.views.clear();
        self.remaining = self.max_size;

        (payload, scopings)
    }
}

/// An upstream request that submits metric buckets via HTTP.
///
/// This request is not awaited. It automatically tracks outcomes if the request is not received.
#[derive(Debug)]
struct SendMetricsRequest {
    /// Optional upstream override where the request will be sent to.
    upstream: Option<UpstreamDescriptor>,
    /// If the partition key is set, the request is marked with `X-Sentry-Relay-Shard`.
    partition_key: String,
    /// Serialized metric buckets without encoding applied, used for signing.
    unencoded: Bytes,
    /// Serialized metric buckets with the stated HTTP encoding applied.
    encoded: Bytes,
    /// Mapping of all contained project keys to their scoping and extraction mode.
    ///
    /// Used to track outcomes for transmission failures.
    project_info: HashMap<ProjectKey, Scoping>,
    /// Encoding (compression) of the payload.
    http_encoding: HttpEncoding,
    /// Metric outcomes instance to send outcomes on error.
    metric_outcomes: MetricOutcomes,
}

impl SendMetricsRequest {
    fn create_error_outcomes(self) {
        #[derive(serde::Deserialize)]
        struct Wrapper {
            buckets: HashMap<ProjectKey, Vec<MinimalTrackableBucket>>,
        }

        let buckets = match serde_json::from_slice(&self.unencoded) {
            Ok(Wrapper { buckets }) => buckets,
            Err(err) => {
                relay_log::error!(
                    error = &err as &dyn std::error::Error,
                    "failed to parse buckets from failed transmission"
                );
                return;
            }
        };

        for (key, buckets) in buckets {
            let Some(&scoping) = self.project_info.get(&key) else {
                relay_log::error!("missing scoping for project key");
                continue;
            };

            self.metric_outcomes.track(
                scoping,
                &buckets,
                Outcome::Invalid(DiscardReason::Internal),
            );
        }
    }
}

impl UpstreamRequest for SendMetricsRequest {
    fn upstream(&self) -> Option<&UpstreamDescriptor> {
        self.upstream.as_ref()
    }

    fn set_relay_id(&self) -> bool {
        true
    }

    fn sign(&mut self) -> Option<Sign> {
        Some(Sign::Required(SignatureType::Body(self.unencoded.clone())))
    }

    fn method(&self) -> reqwest::Method {
        reqwest::Method::POST
    }

    fn path(&self) -> Cow<'_, str> {
        "/api/0/relays/metrics/".into()
    }

    fn route(&self) -> &'static str {
        "global_metrics"
    }

    fn build(&mut self, builder: &mut http::RequestBuilder) -> Result<(), http::HttpError> {
        builder
            .content_encoding(self.http_encoding)
            .header("X-Sentry-Relay-Shard", self.partition_key.as_bytes())
            .header(header::CONTENT_TYPE, b"application/json")
            .body(self.encoded.clone());

        Ok(())
    }

    fn respond(
        self: Box<Self>,
        result: Result<http::Response, UpstreamRequestError>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        Box::pin(async {
            match result {
                Ok(mut response) => {
                    response.consume().await.ok();
                }
                Err(error) => {
                    relay_log::error!(error = &error as &dyn Error, "Failed to send metrics batch");

                    // If the request did not arrive at the upstream, we are responsible for outcomes.
                    // Otherwise, the upstream is responsible to log outcomes.
                    if error.is_received() {
                        return;
                    }

                    self.create_error_outcomes()
                }
            }
        })
    }
}

/// Container for global and project level [`Quota`].
#[derive(Copy, Clone, Debug)]
#[cfg(feature = "processing")]
struct CombinedQuotas<'a> {
    global_quotas: &'a [Quota],
    project_quotas: &'a [Quota],
}

#[cfg(feature = "processing")]
impl<'a> CombinedQuotas<'a> {
    /// Returns a new [`CombinedQuotas`].
    pub fn new(global_config: &'a GlobalConfig, project_quotas: &'a [Quota]) -> Self {
        Self {
            global_quotas: &global_config.quotas,
            project_quotas,
        }
    }
}

#[cfg(feature = "processing")]
impl<'a> IntoIterator for CombinedQuotas<'a> {
    type Item = &'a Quota;
    type IntoIter = std::iter::Chain<std::slice::Iter<'a, Quota>, std::slice::Iter<'a, Quota>>;

    fn into_iter(self) -> Self::IntoIter {
        self.global_quotas.iter().chain(self.project_quotas.iter())
    }
}

#[cfg(test)]
mod tests {
    use relay_base_schema::project::ProjectId;
    use relay_common::time::UnixTimestamp;
    use relay_metrics::{Bucket, BucketMetadata, BucketValue};
    use similar_asserts::assert_eq;
    #[cfg(feature = "processing")]
    use {
        relay_dynamic_config::ProjectConfig,
        relay_quotas::{DataCategory, QuotaScope, ReasonCode},
        relay_test::mock_service,
    };

    #[cfg(feature = "processing")]
    use crate::service::create_redis_clients;
    use crate::services::projects::project::ProjectInfo;

    use super::*;

    #[cfg(feature = "processing")]
    async fn create_test_processor(config: Config) -> InnerProcessor {
        let (upstream_relay, _) = mock_service("upstream_relay", (), |&mut (), _| {});

        let redis_clients = config
            .current()
            .redis()
            .map(|c| create_redis_clients(c))
            .transpose()
            .unwrap();
        let rate_limiter = common::create_rate_limiter(&config.current(), redis_clients);

        let config = Arc::new(config);
        InnerProcessor::new(
            Arc::clone(&config),
            GlobalConfigHandle::fixed(Default::default()),
            ProjectCacheHandle::for_test(),
            Cogs::noop(),
            rate_limiter,
            Addrs {
                upstream_relay,
                store_forwarder: None,
            },
        )
    }

    #[cfg(feature = "processing")]
    fn mock_quota(id: &str) -> Quota {
        Quota {
            id: Some(id.into()),
            categories: [DataCategory::MetricBucket].into(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(0),
            window: None,
            reason_code: None,
            namespace: None,
            group_by: None,
        }
    }

    #[cfg(feature = "processing")]
    #[test]
    fn test_dynamic_quotas() {
        let global_config = relay_dynamic_config::GlobalConfig {
            quotas: vec![mock_quota("foo"), mock_quota("bar")],
            ..Default::default()
        };

        let project_quotas = vec![mock_quota("baz"), mock_quota("qux")];

        let dynamic_quotas = CombinedQuotas::new(&global_config, &project_quotas);

        let quota_ids = dynamic_quotas.into_iter().filter_map(|q| q.id.as_deref());
        assert!(quota_ids.eq(["foo", "bar", "baz", "qux"]));
    }

    /// Ensures that if we ratelimit one batch of buckets in [`FlushBuckets`] message, it won't
    /// also ratelimit the next batches in the same message automatically.
    #[cfg(feature = "processing")]
    #[tokio::test]
    async fn test_ratelimit_per_batch() {
        use relay_base_schema::organization::OrganizationId;
        use relay_protocol::FiniteF64;

        let rate_limited_org = Scoping {
            organization_id: OrganizationId::new(1),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("00000000000000000000000000000000").unwrap(),
            key_id: Some(17),
        };

        let not_rate_limited_org = Scoping {
            organization_id: OrganizationId::new(2),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("11111111111111111111111111111111").unwrap(),
            key_id: Some(17),
        };

        let message = {
            let project_info = {
                let quota = Quota {
                    id: Some("testing".into()),
                    categories: [DataCategory::MetricBucket].into(),
                    scope: relay_quotas::QuotaScope::Organization,
                    scope_id: Some(rate_limited_org.organization_id.to_string().into()),
                    limit: Some(0),
                    window: None,
                    reason_code: Some(ReasonCode::new("test")),
                    namespace: None,
                    group_by: None,
                };

                let mut config = ProjectConfig::default();
                config.quotas.push(quota);

                Arc::new(ProjectInfo {
                    config,
                    ..Default::default()
                })
            };

            let project_metrics = |scoping| ProjectBuckets {
                buckets: vec![Bucket {
                    name: "d:spans/bar".into(),
                    value: BucketValue::Counter(FiniteF64::new(1.0).unwrap()),
                    timestamp: UnixTimestamp::now(),
                    tags: Default::default(),
                    width: 10,
                    metadata: BucketMetadata::default(),
                }],
                rate_limits: Default::default(),
                project_info: project_info.clone(),
                scoping,
            };

            let buckets = hashbrown::HashMap::from([
                (
                    rate_limited_org.project_key,
                    project_metrics(rate_limited_org),
                ),
                (
                    not_rate_limited_org.project_key,
                    project_metrics(not_rate_limited_org),
                ),
            ]);

            FlushBuckets {
                partition_key: 0,
                buckets,
                outcome_aggregator: Addr::dummy(),
            }
        };

        // ensure the order of the map while iterating is as expected.
        assert_eq!(message.buckets.keys().count(), 2);

        let config = {
            let config_json = serde_json::json!({
                "processing": {
                    "enabled": true,
                    "kafka_config": [],
                    "redis": {
                        "server": std::env::var("RELAY_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned()),
                    }
                }
            });
            Config::from_json_value(config_json).unwrap()
        };

        let (store, handle) = {
            let f = |org_ids: &mut Vec<OrganizationId>, msg: Store| {
                let org_id = match msg {
                    Store::Metrics(x) => x.scoping.organization_id,
                    _ => panic!("received envelope when expecting only metrics"),
                };
                org_ids.push(org_id);
            };

            mock_service("store_forwarder", vec![], f)
        };

        let processor = create_test_processor(config).await;
        assert!(processor.redis_rate_limiter_enabled());

        processor.encode_metrics_processing(message, &store).await;

        drop(store);
        let orgs_not_ratelimited = handle.await.unwrap();

        assert_eq!(
            orgs_not_ratelimited,
            vec![not_rate_limited_org.organization_id]
        );
    }

    #[tokio::test]
    async fn test_flush_buckets_http_forwarding() {
        for global_metrics in [false, true] {
            let config = Config::from_json_value(serde_json::json!({
                "http": {
                    "global_metrics": global_metrics,
                    "encoding": "identity",
                },
            }))
            .unwrap();
            let (upstream_relay, mut upstream_rx) = Addr::custom();
            let outcome_aggregator = Addr::dummy();
            let pool = relay_threading::AsyncPoolBuilder::new(tokio::runtime::Handle::current())
                .build()
                .unwrap();
            let processor = create_service(
                pool,
                Arc::new(config),
                GlobalConfigHandle::fixed(Default::default()),
                ProjectCacheHandle::for_test(),
                Cogs::noop(),
                #[cfg(feature = "processing")]
                None,
                Addrs {
                    upstream_relay,
                    #[cfg(feature = "processing")]
                    store_forwarder: None,
                },
            )
            .start_detached();

            let project_key = ProjectKey::parse("00000000000000000000000000000000").unwrap();
            let upstream = "https://metrics.example/"
                .parse::<UpstreamDescriptor>()
                .unwrap();
            let project_info = Arc::new(ProjectInfo {
                project_id: Some(ProjectId::new(42)),
                upstream: Some(upstream.clone()),
                ..Default::default()
            });
            let scoping = project_info.scoping(project_key).unwrap();
            let bucket = Bucket {
                name: "c:sessions/session@none".into(),
                value: BucketValue::Counter(1.into()),
                timestamp: UnixTimestamp::now(),
                tags: Default::default(),
                width: 10,
                metadata: BucketMetadata::default(),
            };
            let message = FlushBuckets {
                partition_key: 17,
                buckets: hashbrown::HashMap::from([(
                    project_key,
                    ProjectBuckets {
                        buckets: vec![bucket.clone()],
                        scoping,
                        project_info,
                        rate_limits: Default::default(),
                    },
                )]),
                outcome_aggregator,
            };

            processor.send(message);
            drop(processor);

            let Some(UpstreamRelay::SendRequest(mut request)) = upstream_rx.recv().await else {
                panic!("expected an upstream metrics request");
            };
            assert!(upstream_rx.recv().await.is_none());
            assert_eq!(request.upstream(), Some(&upstream));
            assert_eq!(request.method(), reqwest::Method::POST);
            assert_eq!(
                request.path(),
                if global_metrics {
                    "/api/0/relays/metrics/"
                } else {
                    "/api/42/envelope/"
                }
            );

            let client = reqwest::Client::new();
            let mut builder = http::RequestBuilder::reqwest(client.post("http://localhost/"));
            request.build(&mut builder).unwrap();
            let built_request = builder.finish().unwrap().0;
            assert_eq!(built_request.headers()["X-Sentry-Relay-Shard"], "17");
            let body = built_request.body().unwrap().as_bytes().unwrap();
            let buckets: Vec<Bucket> = if global_metrics {
                let payload: serde_json::Value = serde_json::from_slice(body).unwrap();
                serde_json::from_value(payload["buckets"][project_key.as_str()].clone()).unwrap()
            } else {
                let envelope = Envelope::parse_bytes(Bytes::copy_from_slice(body)).unwrap();
                let item = envelope.items().next().unwrap();
                assert_eq!(item.ty(), &ItemType::MetricBuckets);
                serde_json::from_slice(&item.payload()).unwrap()
            };
            assert_eq!(buckets, vec![bucket]);

            let response = ::http::Response::builder()
                .status(200)
                .body(Vec::<u8>::new())
                .unwrap();
            request.respond(Ok(http::Response(response.into()))).await;
        }
    }
}
