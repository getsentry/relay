use relay_statsd::{CounterMetric, GaugeMetric, HistogramMetric, TimerMetric};
#[cfg(doc)]
use tokio::runtime::RuntimeMetrics;

/// Gauge metrics used by Relay
pub enum RelayGauges {
    /// The state of Relay with respect to the upstream connection.
    /// Possible values are `0` for normal operations and `1` for a network outage.
    NetworkOutage,
    /// The number of items currently in the garbage disposal queue.
    ProjectCacheGarbageQueueSize,
    /// The number of envelopes waiting for project states in memory.
    ///
    /// This number is always <= `EnvelopeQueueSize`.
    ///
    /// The memory buffer size can be configured with `spool.envelopes.max_memory_size`.
    BufferEnvelopesMemoryCount,
    /// The number of envelopes waiting for project states on disk.
    ///
    /// Note this metric *will not be logged* when we encounter envelopes in the database on startup,
    /// because counting those envelopes reliably would risk locking the db for multiple seconds.
    ///
    /// The disk buffer size can be configured with `spool.envelopes.max_disk_size`.
    BufferEnvelopesDiskCount,
    /// Number of queue keys (project key pairs) unspooled during proactive unspool.
    /// This metric is tagged with:
    /// - `reason`: Why keys are / are not unspooled.
    BufferPeriodicUnspool,
    /// The number of individual stacks in the priority queue.
    ///
    /// Per combination of `(own_key, sampling_key)`, a new stack is created.
    BufferStackCount,
    /// The used disk for the buffer.
    BufferDiskUsed,
    /// The currently used memory by the entire system.
    ///
    /// Relay uses the same value for its memory health check.
    SystemMemoryUsed,
    /// The total system memory.
    ///
    /// Relay uses the same value for its memory health check.
    SystemMemoryTotal,
    /// The number of connections currently being managed by the Redis Pool.
    #[cfg(feature = "processing")]
    RedisPoolConnections,
    /// The number of idle connections in the Redis Pool.
    #[cfg(feature = "processing")]
    RedisPoolIdleConnections,
}

impl GaugeMetric for RelayGauges {
    fn name(&self) -> &'static str {
        match self {
            RelayGauges::NetworkOutage => "upstream.network_outage",
            RelayGauges::ProjectCacheGarbageQueueSize => "project_cache.garbage.queue_size",
            RelayGauges::BufferEnvelopesMemoryCount => "buffer.envelopes_mem_count",
            RelayGauges::BufferEnvelopesDiskCount => "buffer.envelopes_disk_count",
            RelayGauges::BufferPeriodicUnspool => "buffer.unspool.periodic",
            RelayGauges::BufferStackCount => "buffer.stack_count",
            RelayGauges::BufferDiskUsed => "buffer.disk_used",
            RelayGauges::SystemMemoryUsed => "health.system_memory.used",
            RelayGauges::SystemMemoryTotal => "health.system_memory.total",
            #[cfg(feature = "processing")]
            RelayGauges::RedisPoolConnections => "redis.pool.connections",
            #[cfg(feature = "processing")]
            RelayGauges::RedisPoolIdleConnections => "redis.pool.idle_connections",
        }
    }
}

/// Gauge metrics collected from the Tokio Runtime.
pub enum TokioGauges {
    /// Exposes [`RuntimeMetrics::active_tasks_count`].
    ActiveTasksCount,
    /// Exposes [`RuntimeMetrics::blocking_queue_depth`].
    BlockingQueueDepth,
    /// Exposes [`RuntimeMetrics::budget_forced_yield_count`].
    BudgetForcedYieldCount,
    /// Exposes [`RuntimeMetrics::num_blocking_threads`].
    NumBlockingThreads,
    /// Exposes [`RuntimeMetrics::num_idle_blocking_threads`].
    NumIdleBlockingThreads,
    /// Exposes [`RuntimeMetrics::num_workers`].
    NumWorkers,
    /// Exposes [`RuntimeMetrics::worker_local_queue_depth`].
    ///
    /// This metric is tagged with:
    /// - `worker`: the worker id.
    WorkerLocalQueueDepth,
    /// Exposes [`RuntimeMetrics::worker_local_schedule_count`].
    ///
    /// This metric is tagged with:
    /// - `worker`: the worker id.
    WorkerLocalScheduleCount,
    /// Exposes [`RuntimeMetrics::worker_mean_poll_time`].
    ///
    /// This metric is tagged with:
    /// - `worker`: the worker id.
    WorkerMeanPollTime,
    /// Exposes [`RuntimeMetrics::worker_noop_count`].
    ///
    /// This metric is tagged with:
    /// - `worker`: the worker id.
    WorkerNoopCount,
    /// Exposes [`RuntimeMetrics::worker_overflow_count`].
    ///
    /// This metric is tagged with:
    /// - `worker`: the worker id.
    WorkerOverflowCount,
    /// Exposes [`RuntimeMetrics::worker_park_count`].
    ///
    /// This metric is tagged with:
    /// - `worker`: the worker id.
    WorkerParkCount,
    /// Exposes [`RuntimeMetrics::worker_poll_count`].
    ///
    /// This metric is tagged with:
    /// - `worker`: the worker id.
    WorkerPollCount,
    /// Exposes [`RuntimeMetrics::worker_steal_count`].
    ///
    /// This metric is tagged with:
    /// - `worker`: the worker id.
    WorkerStealCount,
    /// Exposes [`RuntimeMetrics::worker_steal_operations`].
    ///
    /// This metric is tagged with:
    /// - `worker`: the worker id.
    WorkerStealOperations,
    /// Exposes [`RuntimeMetrics::worker_total_busy_duration`].
    ///
    /// This metric is tagged with:
    /// - `worker`: the worker id.
    WorkerTotalBusyDuration,
}

impl GaugeMetric for TokioGauges {
    fn name(&self) -> &'static str {
        match self {
            TokioGauges::ActiveTasksCount => "tokio.active_task_count",
            TokioGauges::BlockingQueueDepth => "tokio.blocking_queue_depth",
            TokioGauges::BudgetForcedYieldCount => "tokio.budget_forced_yield_count",
            TokioGauges::NumBlockingThreads => "tokio.num_blocking_threads",
            TokioGauges::NumIdleBlockingThreads => "tokio.num_idle_blocking_threads",
            TokioGauges::NumWorkers => "tokio.num_workers",
            TokioGauges::WorkerLocalQueueDepth => "tokio.worker_local_queue_depth",
            TokioGauges::WorkerLocalScheduleCount => "tokio.worker_local_schedule_count",
            TokioGauges::WorkerMeanPollTime => "tokio.worker_mean_poll_time",
            TokioGauges::WorkerNoopCount => "tokio.worker_noop_count",
            TokioGauges::WorkerOverflowCount => "tokio.worker_overflow_count",
            TokioGauges::WorkerParkCount => "tokio.worker_park_count",
            TokioGauges::WorkerPollCount => "tokio.worker_poll_count",
            TokioGauges::WorkerStealCount => "tokio.worker_steal_count",
            TokioGauges::WorkerStealOperations => "tokio.worker_steal_operations",
            TokioGauges::WorkerTotalBusyDuration => "tokio.worker_total_busy_duration",
        }
    }
}

/// Histogram metrics used by Relay.
pub enum RelayHistograms {
    /// The number of bytes received by Relay for each individual envelope item type.
    ///
    /// Metric is tagged by the item type.
    EnvelopeItemSize,
    /// The estimated number of envelope bytes buffered in memory.
    ///
    /// The memory buffer size can be configured with `spool.envelopes.max_memory_size`.
    BufferEnvelopesMemoryBytes,
    /// The file size of the buffer db on disk, in bytes.
    ///
    /// This metric is computed by multiplying `page_count * page_size`.
    BufferDiskSize,
    /// Number of attempts needed to dequeue spooled envelopes from disk.
    BufferDequeueAttempts,
    /// Number of elements in the envelope buffer across all the stacks.
    ///
    /// This metric is tagged with:
    /// - `storage_type`: The type of storage used in the envelope buffer.
    BufferEnvelopesCount,
    /// The number of batches emitted per partition.
    BatchesPerPartition,
    /// The number of buckets in a batch emitted.
    ///
    /// This corresponds to the number of buckets that will end up in an envelope.
    BucketsPerBatch,
    /// The number of spans per processed transaction event.
    ///
    /// This metric is tagged with:
    ///  - `platform`: The event's platform, such as `"javascript"`.
    ///  - `sdk`: The name of the Sentry SDK sending the transaction. This tag is only set for
    ///    Sentry's SDKs and defaults to "proprietary".
    EventSpans,
    /// Number of projects in the in-memory project cache that are waiting for their state to be
    /// updated.
    ///
    /// See `project_cache.size` for more description of the project cache.
    ProjectStatePending,
    /// Number of project states **requested** from the upstream for each batch request.
    ///
    /// If multiple batches are updated concurrently, this metric is reported multiple times.
    ///
    /// The batch size can be configured with `cache.batch_size`. See `project_cache.size` for more
    /// description of the project cache.
    ProjectStateRequestBatchSize,
    /// Number of project states **returned** from the upstream for each batch request.
    ///
    /// If multiple batches are updated concurrently, this metric is reported multiple times.
    ///
    /// See `project_cache.size` for more description of the project cache.
    ProjectStateReceived,
    /// Number of attempts required to fetch the config for a given project key.
    ProjectStateAttempts,
    /// Number of project states currently held in the in-memory project cache.
    ///
    /// The cache duration for project states can be configured with the following options:
    ///
    ///  - `cache.project_expiry`: The time after which a project state counts as expired. It is
    ///    automatically refreshed if a request references the project after it has expired.
    ///  - `cache.project_grace_period`: The time after expiry at which the project state will still
    ///    be used to ingest events. Once the grace period expires, the cache is evicted and new
    ///    requests wait for an update.
    ///
    /// There is no limit to the number of cached projects.
    ProjectStateCacheSize,
    /// The size of the compressed project config in the redis cache, in bytes.
    #[cfg(feature = "processing")]
    ProjectStateSizeBytesCompressed,
    /// The size of the uncompressed project config in the redis cache, in bytes.
    #[cfg(feature = "processing")]
    ProjectStateSizeBytesDecompressed,
    /// The number of upstream requests queued up for sending.
    ///
    /// Relay employs connection keep-alive whenever possible. Connections are kept open for _15_
    /// seconds of inactivity or _75_ seconds of activity. If all connections are busy, they are
    /// queued, which is reflected in this metric.
    ///
    /// This metric is tagged with:
    ///  - `priority`: The queueing priority of the request, either `"high"` or `"low"`. The
    ///    priority determines precedence in executing requests.
    ///
    /// The number of concurrent connections can be configured with:
    ///  - `limits.max_concurrent_requests` for the overall number of connections
    ///  - `limits.max_concurrent_queries` for the number of concurrent high-priority requests
    UpstreamMessageQueueSize,
    /// Counts the number of retries for each upstream http request.
    ///
    /// This metric is tagged with:
    ///
    ///   - `result`: What happened to the request, an enumeration with the following values:
    ///     * `success`: The request was sent and returned a success code `HTTP 2xx`
    ///     * `response_error`: The request was sent and it returned an HTTP error.
    ///     * `payload_failed`: The request was sent but there was an error in interpreting the response.
    ///     * `send_failed`: Failed to send the request due to a network error.
    ///     * `rate_limited`: The request was rate limited.
    ///     * `invalid_json`: The response could not be parsed back into JSON.
    ///   - `route`: The endpoint that was called on the upstream.
    ///   - `status-code`: The status code of the request when available, otherwise "-".
    UpstreamRetries,
    /// Size of envelopes sent over HTTP in bytes.
    UpstreamQueryBodySize,
    /// Size of queries (projectconfig queries, i.e. the request payload, not the response) sent by
    /// Relay over HTTP in bytes.
    UpstreamEnvelopeBodySize,
    /// Size of batched global metrics requests sent by Relay over HTTP in bytes.
    UpstreamMetricsBodySize,
    /// Distribution of flush buckets over partition keys.
    ///
    /// The distribution of buckets should be even.
    /// If it is not, this metric should expose it.
    PartitionKeys,
    /// Measures how many splits were performed when sending out a partition.
    PartitionSplits,
    /// The total number of metric buckets flushed in a cycle across all projects.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    BucketsFlushed,
    /// The number of metric buckets flushed in a cycle for each project.
    ///
    /// Relay scans metric buckets in regular intervals and flushes expired buckets. This histogram
    /// is logged for each project that is being flushed. The count of the histogram values is
    /// equivalent to the number of projects being flushed.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    BucketsFlushedPerProject,
    /// The number of metric partitions flushed in a cycle.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    PartitionsFlushed,
}

impl HistogramMetric for RelayHistograms {
    fn name(&self) -> &'static str {
        match self {
            RelayHistograms::EnvelopeItemSize => "event.item_size",
            RelayHistograms::EventSpans => "event.spans",
            RelayHistograms::BatchesPerPartition => "metrics.buckets.batches_per_partition",
            RelayHistograms::BucketsPerBatch => "metrics.buckets.per_batch",
            RelayHistograms::BufferEnvelopesMemoryBytes => "buffer.envelopes_mem",
            RelayHistograms::BufferDiskSize => "buffer.disk_size",
            RelayHistograms::BufferDequeueAttempts => "buffer.dequeue_attempts",
            RelayHistograms::BufferEnvelopesCount => "buffer.envelopes_count",
            RelayHistograms::ProjectStatePending => "project_state.pending",
            RelayHistograms::ProjectStateAttempts => "project_state.attempts",
            RelayHistograms::ProjectStateRequestBatchSize => "project_state.request.batch_size",
            RelayHistograms::ProjectStateReceived => "project_state.received",
            RelayHistograms::ProjectStateCacheSize => "project_cache.size",
            #[cfg(feature = "processing")]
            RelayHistograms::ProjectStateSizeBytesCompressed => {
                "project_state.size_bytes.compressed"
            }
            #[cfg(feature = "processing")]
            RelayHistograms::ProjectStateSizeBytesDecompressed => {
                "project_state.size_bytes.decompressed"
            }
            RelayHistograms::UpstreamMessageQueueSize => "http_queue.size",
            RelayHistograms::UpstreamRetries => "upstream.retries",
            RelayHistograms::UpstreamQueryBodySize => "upstream.query.body_size",
            RelayHistograms::UpstreamEnvelopeBodySize => "upstream.envelope.body_size",
            RelayHistograms::UpstreamMetricsBodySize => "upstream.metrics.body_size",
            RelayHistograms::PartitionKeys => "metrics.buckets.partition_keys",
            RelayHistograms::PartitionSplits => "partition_splits",
            RelayHistograms::BucketsFlushed => "metrics.buckets.flushed",
            RelayHistograms::BucketsFlushedPerProject => "metrics.buckets.flushed_per_project",
            RelayHistograms::PartitionsFlushed => "metrics.partitions.flushed",
        }
    }
}

/// Timer metrics used by Relay
pub enum RelayTimers {
    /// Time in milliseconds spent deserializing an event from JSON bytes into the native data
    /// structure on which Relay operates.
    EventProcessingDeserialize,
    /// Time in milliseconds spent running normalization on an event. Normalization
    /// happens before envelope filtering and metric extraction.
    EventProcessingNormalization,
    /// Time in milliseconds spent running inbound data filters on an event.
    EventProcessingFiltering,
    /// Time in milliseconds spent checking for organization, project, and DSN rate limits.
    ///
    /// Not all events reach this point. After an event is rate limited for the first time, the rate
    /// limit is cached. Events coming in after this will be discarded earlier in the request queue
    /// and do not reach the processing queue.
    #[cfg(feature = "processing")]
    EventProcessingRateLimiting,
    /// Time in milliseconds spent in data scrubbing for the current event. Data scrubbing happens
    /// last before serializing the event back to JSON.
    EventProcessingPii,
    /// Time spent converting the event from its in-memory reprsentation into a JSON string.
    EventProcessingSerialization,
    /// Time used to extract span metrics from an event.
    EventProcessingSpanMetricsExtraction,
    /// Time spent between the start of request handling and processing of the envelope.
    ///
    /// This includes streaming the request body, scheduling overheads, project config fetching,
    /// batched requests and congestions in the internal processor. This does not include delays in
    /// the incoming request (body upload) and skips all envelopes that are fast-rejected.
    EnvelopeWaitTime,
    /// Time in milliseconds spent in synchronous processing of envelopes.
    ///
    /// This timing covers the end-to-end processing in the CPU pool and comprises:
    ///
    ///  - `event_processing.deserialize`
    ///  - `event_processing.pii`
    ///  - `event_processing.serialization`
    ///
    /// With Relay in processing mode, this also includes the following timings:
    ///
    ///  - `event_processing.process`
    ///  - `event_processing.filtering`
    ///  - `event_processing.rate_limiting`
    EnvelopeProcessingTime,
    /// Total time in milliseconds an envelope spends in Relay from the time it is received until it
    /// finishes processing and has been submitted to the upstream.
    EnvelopeTotalTime,
    /// Total time in milliseconds spent evicting outdated and unused projects happens.
    ProjectStateEvictionDuration,
    /// Total time in milliseconds spent fetching queued project configuration updates requests to
    /// resolve.
    ///
    /// Relay updates projects in batches. Every update cycle, Relay requests
    /// `limits.max_concurrent_queries * cache.batch_size` projects from the upstream. This metric
    /// measures the wall clock time for all concurrent requests in this loop.
    ///
    /// Note that after an update loop has completed, there may be more projects pending updates.
    /// This is indicated by `project_state.pending`.
    ProjectStateRequestDuration,
    /// Time in milliseconds required to decompress a project config from redis.
    ///
    /// Note that this also times the cases where project config is uncompressed,
    /// in which case the timer should be very close to zero.
    #[cfg(feature = "processing")]
    ProjectStateDecompression,
    /// Total duration in milliseconds for handling inbound web requests until the HTTP response is
    /// returned to the client.
    ///
    /// This does **not** correspond to the full event ingestion time. Requests for events that are
    /// not immediately rejected due to bad data or cached rate limits always return `200 OK`. Full
    /// validation and normalization occur asynchronously, which is reported by
    /// `event.processing_time`.
    ///
    /// This metric is tagged with:
    ///  - `method`: The HTTP method of the request.
    ///  - `route`: Unique dashed identifier of the endpoint.
    RequestsDuration,
    /// Time spent on minidump scrubbing.
    ///
    /// This is the total time spent on parsing and scrubbing the minidump.  Even if no PII
    /// scrubbing rules applied the minidump will still be parsed and the rules evaluated on
    /// the parsed minidump, this duration is reported here with status of "n/a".
    ///
    /// This metric is tagged with:
    ///
    /// - `status`: Scrubbing status: "ok" means successful scrubbed, "error" means there
    ///       was an error during scrubbing and finally "n/a" means scrubbing was successful
    ///       but no scurbbing rules applied.
    MinidumpScrubbing,
    /// Time spend on attachment scrubbing.
    ///
    /// This represents the total time spent on evaluating the scrubbing rules for an
    /// attachment and the attachment scrubbing itself, regardless of whether any rules were
    /// applied.  Note that minidumps which failed to be parsed (status="error" in
    /// scrubbing.minidumps.duration) will be scrubbed as plain attachments and count
    /// towards this.
    AttachmentScrubbing,
    /// Total time spent to send request to upstream Relay and handle the response.
    ///
    /// This metric is tagged with:
    ///
    ///   - `result`: What happened to the request, an enumeration with the following values:
    ///     * `success`: The request was sent and returned a success code `HTTP 2xx`
    ///     * `response_error`: The request was sent and it returned an HTTP error.
    ///     * `payload_failed`: The request was sent but there was an error in interpreting the response.
    ///     * `send_failed`: Failed to send the request due to a network error.
    ///     * `rate_limited`: The request was rate limited.
    ///     * `invalid_json`: The response could not be parsed back into JSON.
    ///   - `route`: The endpoint that was called on the upstream.
    ///   - `status-code`: The status code of the request when available, otherwise "-".
    ///   - `retries`: Number of retries bucket 0, 1, 2, few (3 - 10), many (more than 10).
    UpstreamRequestsDuration,
    /// The delay between the timestamp stated in a payload and the receive time.
    ///
    /// SDKs cannot transmit payloads immediately in all cases. Sometimes, crashes require that
    /// events are sent after restarting the application. Similarly, SDKs buffer events during
    /// network downtimes for later transmission. This metric measures the delay between the time of
    /// the event and the time it arrives in Relay. The delay is measured after clock drift
    /// correction is applied.
    ///
    /// Only payloads with a delay of more than 1 minute are captured.
    ///
    /// This metric is tagged with:
    ///
    ///  - `category`: The data category of the payload. Can be one of: `event`, `transaction`,
    ///    `security`, or `session`.
    TimestampDelay,
    /// The time it takes the outcome aggregator to flush aggregated outcomes.
    OutcomeAggregatorFlushTime,
    /// Time in milliseconds spent on parsing, normalizing and scrubbing replay recordings.
    ReplayRecordingProcessing,
    /// Total time spent to send a request and receive the response from upstream.
    GlobalConfigRequestDuration,
    /// Timing in milliseconds for processing a message in the internal CPU pool.
    ///
    /// This metric is tagged with:
    ///
    ///  - `message`: The type of message that was processed.
    ProcessMessageDuration,
    /// Timing in milliseconds for handling a project cache message.
    ///
    /// This metric is tagged with:
    ///  - `message`: The type of message that was processed.
    ProjectCacheMessageDuration,
    /// Timing in milliseconds for processing a message in the buffer service.
    ///
    /// This metric is tagged with:
    ///
    ///  - `message`: The type of message that was processed.
    BufferMessageProcessDuration,
    /// Timing in milliseconds for processing a task in the project cache service.
    ///
    /// A task is a unit of work the service does. Each branch of the
    /// `tokio::select` is a different task type.
    ///
    /// This metric is tagged with:
    /// - `task`: The type of the task the processor does.
    ProjectCacheTaskDuration,
    /// Timing in milliseconds for handling and responding to a health check request.
    ///
    /// This metric is tagged with:
    ///  - `type`: The type of the health check, `liveness` or `readiness`.
    HealthCheckDuration,
    /// Temporary timing metric for how much time was spent evaluating span and transaction
    /// rate limits using the `RateLimitBuckets` message in the processor.
    ///
    /// This metric is tagged with:
    ///  - `category`: The data category evaluated.
    ///  - `limited`: Whether the batch is rate limited.
    ///  - `count`: How many items matching the data category are contained in the batch.
    #[cfg(feature = "processing")]
    RateLimitBucketsDuration,
    /// Timing in milliseconds for processing a message in the aggregator service.
    ///
    /// This metric is tagged with:
    ///  - `message`: The type of message that was processed.
    AggregatorServiceDuration,
    /// Timing in milliseconds for processing a message in the metric router service.
    ///
    /// This metric is tagged with:
    ///  - `message`: The type of message that was processed.
    MetricRouterServiceDuration,
    /// Timing in milliseconds for processing a message in the metric store service.
    ///
    /// This metric is tagged with:
    ///  - `message`: The type of message that was processed.
    #[cfg(feature = "processing")]
    StoreServiceDuration,
    /// Timing in milliseconds for the time it takes for initialize the buffer.
    BufferInitialization,
    /// Timing in milliseconds for the time it takes for the buffer to spool data to disk.
    BufferSpool,
    /// Timing in milliseconds for the time it takes for the buffer to unspool data from disk.
    BufferUnspool,
    /// Timing in milliseconds for the time it takes for the buffer to push.
    BufferPush,
    /// Timing in milliseconds for the time it takes for the buffer to peek.
    BufferPeek,
    /// Timing in milliseconds for the time it takes for the buffer to pop.
    BufferPop,
}

impl TimerMetric for RelayTimers {
    fn name(&self) -> &'static str {
        match self {
            RelayTimers::EventProcessingDeserialize => "event_processing.deserialize",
            RelayTimers::EventProcessingNormalization => "event_processing.normalization",
            RelayTimers::EventProcessingFiltering => "event_processing.filtering",
            #[cfg(feature = "processing")]
            RelayTimers::EventProcessingRateLimiting => "event_processing.rate_limiting",
            RelayTimers::EventProcessingPii => "event_processing.pii",
            RelayTimers::EventProcessingSpanMetricsExtraction => {
                "event_processing.span_metrics_extraction"
            }
            RelayTimers::EventProcessingSerialization => "event_processing.serialization",
            RelayTimers::EnvelopeWaitTime => "event.wait_time",
            RelayTimers::EnvelopeProcessingTime => "event.processing_time",
            RelayTimers::EnvelopeTotalTime => "event.total_time",
            RelayTimers::ProjectStateEvictionDuration => "project_state.eviction.duration",
            RelayTimers::ProjectStateRequestDuration => "project_state.request.duration",
            #[cfg(feature = "processing")]
            RelayTimers::ProjectStateDecompression => "project_state.decompression",
            RelayTimers::RequestsDuration => "requests.duration",
            RelayTimers::MinidumpScrubbing => "scrubbing.minidumps.duration",
            RelayTimers::AttachmentScrubbing => "scrubbing.attachments.duration",
            RelayTimers::UpstreamRequestsDuration => "upstream.requests.duration",
            RelayTimers::TimestampDelay => "requests.timestamp_delay",
            RelayTimers::OutcomeAggregatorFlushTime => "outcomes.aggregator.flush_time",
            RelayTimers::ReplayRecordingProcessing => "replay.recording.process",
            RelayTimers::GlobalConfigRequestDuration => "global_config.requests.duration",
            RelayTimers::ProcessMessageDuration => "processor.message.duration",
            RelayTimers::ProjectCacheMessageDuration => "project_cache.message.duration",
            RelayTimers::BufferMessageProcessDuration => "buffer.message.duration",
            RelayTimers::ProjectCacheTaskDuration => "project_cache.task.duration",
            RelayTimers::HealthCheckDuration => "health.message.duration",
            #[cfg(feature = "processing")]
            RelayTimers::RateLimitBucketsDuration => "processor.rate_limit_buckets",
            RelayTimers::AggregatorServiceDuration => "metrics.aggregator.message.duration",
            RelayTimers::MetricRouterServiceDuration => "metrics.router.message.duration",
            #[cfg(feature = "processing")]
            RelayTimers::StoreServiceDuration => "store.message.duration",
            RelayTimers::BufferInitialization => "buffer.initialization.duration",
            RelayTimers::BufferSpool => "buffer.spool.duration",
            RelayTimers::BufferUnspool => "buffer.unspool.duration",
            RelayTimers::BufferPush => "buffer.push.duration",
            RelayTimers::BufferPeek => "buffer.peek.duration",
            RelayTimers::BufferPop => "buffer.pop.duration",
        }
    }
}

/// Counter metrics used by Relay
pub enum RelayCounters {
    /// Number of Events that had corrupted (unprintable) event attributes.
    ///
    /// This currently checks for `environment` and `release`, for which we know that
    /// some SDKs may send corrupted values.
    EventCorrupted,
    /// Number of envelopes accepted in the current time slot.
    ///
    /// This represents requests that have successfully passed rate limits and filters, and have
    /// been sent to the upstream.
    ///
    /// This metric is tagged with:
    ///  - `handling`: Either `"success"` if the envelope was handled correctly, or `"failure"` if
    ///    there was an error or bug.
    EnvelopeAccepted,
    /// Number of envelopes rejected in the current time slot.
    ///
    /// This includes envelopes being rejected because they are malformed or any other errors during
    /// processing (including filtered events, invalid payloads, and rate limits).
    ///
    /// To check the rejection reason, check `events.outcomes`, instead.
    ///
    /// This metric is tagged with:
    ///  - `handling`: Either `"success"` if the envelope was handled correctly, or `"failure"` if
    ///    there was an error or bug.
    EnvelopeRejected,
    /// Number of times the envelope buffer spools to disk.
    BufferWritesDisk,
    /// Number of times the envelope buffer reads back from disk.
    BufferReadsDisk,
    /// Number of _envelopes_ the envelope buffer ingests.
    BufferEnvelopesWritten,
    /// Number of _envelopes_ the envelope buffer produces.
    BufferEnvelopesRead,
    /// Number of state changes in the envelope buffer.
    /// This metric is tagged with:
    ///  - `state_in`: The previous state. `memory`, `memory_file_standby`, or `disk`.
    ///  - `state_out`: The new state. `memory`, `memory_file_standby`, or `disk`.
    ///  - `reason`: Why a transition was made (or not made).
    BufferStateTransition,
    /// Number of envelopes that were returned to the envelope buffer by the project cache.
    ///
    /// This happens when the envelope buffer falsely assumes that the envelope's projects are loaded
    /// in the cache and sends the envelope onward, even though the project cache cannot handle it.
    BufferEnvelopesReturned,
    /// Number of times an envelope stack is popped from the priority queue of stacks in the
    /// envelope buffer.
    BufferEnvelopeStacksPopped,
    /// Number of times an envelope from the buffer is trying to be popped.
    BufferTryPop,
    /// Number of envelopes spool to disk.
    BufferSpooledEnvelopes,
    /// Number of envelopes unspooled from disk.
    BufferUnspooledEnvelopes,
    ///
    /// Number of outcomes and reasons for rejected Envelopes.
    ///
    /// This metric is tagged with:
    ///  - `outcome`: The basic cause for rejecting the event.
    ///  - `reason`: A more detailed identifier describing the rule or mechanism leading to the
    ///    outcome.
    ///  - `to`: Describes the destination of the outcome. Can be either 'kafka' (when in
    ///    processing mode) or 'http' (when outcomes are enabled in an external relay).
    ///
    /// Possible outcomes are:
    ///  - `filtered`: Dropped by inbound data filters. The reason specifies the filter that
    ///    matched.
    ///  - `rate_limited`: Dropped by organization, project, or DSN rate limit, as well as exceeding
    ///    the Sentry plan quota. The reason contains the rate limit or quota that was exceeded.
    ///  - `invalid`: Data was considered invalid and could not be recovered. The reason indicates
    ///    the validation that failed.
    Outcomes,
    /// Number of times a project state is looked up from the cache.
    ///
    /// This includes lookups for both cached and new projects. As part of this, updates for
    /// outdated or expired project caches are triggered.
    ///
    /// Related metrics:
    ///  - `project_cache.hit`: For successful cache lookups, even for outdated projects.
    ///  - `project_cache.miss`: For failed lookups resulting in an update.
    ProjectStateGet,
    /// Number of project state HTTP requests.
    ///
    /// Relay updates projects in batches. Every update cycle, Relay requests
    /// `limits.max_concurrent_queries` batches of `cache.batch_size` projects from the upstream.
    /// The duration of these requests is reported via `project_state.request.duration`.
    ///
    /// Note that after an update loop has completed, there may be more projects pending updates.
    /// This is indicated by `project_state.pending`.
    ProjectStateRequest,
    /// Number of times a project config was requested with `.no-cache`.
    ///
    /// This effectively counts the number of envelopes or events that have been sent with a
    /// corresponding DSN. Actual queries to the upstream may still be deduplicated for these
    /// project state requests.
    ///
    /// A maximum of 1 such requests per second is allowed per project key. This metric counts only
    /// permitted requests.
    ProjectStateNoCache,
    /// Number of times a project state is requested from the central Redis cache.
    ///
    /// This metric is tagged with:
    ///  - `hit`: One of:
    ///     - `revision`: the cached version was validated to be up to date using its revision.
    ///     - `project_config`: the request was handled by the cache.
    ///     - `project_config_revision`: the request was handled by the cache and the revision did
    ///        not change.
    ///     - `false`: the request will be sent to the sentry endpoint.
    #[cfg(feature = "processing")]
    ProjectStateRedis,
    /// Number of times a project is looked up from the cache.
    ///
    /// The cache may contain and outdated or expired project state. In that case, the project state
    /// is updated even after a cache hit.
    ProjectCacheHit,
    /// Number of times a project lookup failed.
    ///
    /// A cache entry is created immediately and the project state requested from the upstream.
    ProjectCacheMiss,
    /// Number of times an upstream request for a project config is completed.
    ///
    /// Completion can be because a result was returned or because the config request was
    /// dropped after there still was no response after a timeout.  This metrics has tags
    /// for `result` and `attempts` indicating whether it was succesful or a timeout and how
    /// many attempts were made respectively.
    ProjectUpstreamCompleted,
    /// Number of times an upstream request for a project config failed.
    ///
    /// Failure can happen, for example, when there's a network error. Refer to
    /// [`UpstreamRequestError`](crate::services::upstream::UpstreamRequestError) for all cases.
    ProjectUpstreamFailed,
    /// Number of full metric data flushes.
    ///
    /// A full flush takes all contained items of the aggregator and flushes them upstream,
    /// at best this happens once per freshly loaded project.
    ProjectStateFlushAllMetricMeta,
    /// Number of Relay server starts.
    ///
    /// This can be used to track unwanted restarts due to crashes or termination.
    ServerStarting,
    /// Number of messages placed on the Kafka queues.
    ///
    /// When Relay operates as Sentry service and an Envelope item is successfully processed, each
    /// Envelope item results in a dedicated message on one of the ingestion topics on Kafka.
    ///
    /// This metric is tagged with:
    ///  - `event_type`: The kind of message produced to Kafka.
    ///  - `namespace` (only for metrics): The namespace that the metric belongs to.
    ///  - `is_segment` (only for event_type span): `true` the span is the root of a segment.
    ///  - `has_parent` (only for event_type span): `false` if the span is the root of a trace.
    ///  - `platform` (only for event_type span): The platform from which the span was spent.
    ///  - `metric_type` (only for event_type metric): The metric type, counter, distribution,
    ///    gauge or set.
    ///  - `metric_encoding` (only for event_type metric): The encoding used for distribution and
    ///    set metrics.
    ///
    /// The message types can be:
    ///
    ///  - `event`: An error or transaction event. Error events are sent to `ingest-events`,
    ///    transactions to `ingest-transactions`, and errors with attachments are sent to
    ///    `ingest-attachments`.
    ///  - `attachment`: An attachment file associated with an error event, sent to
    ///    `ingest-attachments`.
    ///  - `user_report`: A message from the user feedback dialog, sent to `ingest-events`.
    ///  - `session`: A release health session update, sent to `ingest-sessions`.
    #[cfg(feature = "processing")]
    ProcessingMessageProduced,
    /// Number of events that hit any of the store-like endpoints: Envelope, Store, Security,
    /// Minidump, Unreal.
    ///
    /// The events are counted before they are rate limited, filtered, or processed in any way.
    ///
    /// This metric is tagged with:
    ///  - `version`: The event protocol version number defaulting to `7`.
    EventProtocol,
    /// The number of transaction events processed by the source of the transaction name.
    ///
    /// This metric is tagged with:
    ///  - `platform`: The event's platform, such as `"javascript"`.
    ///  - `source`: The source of the transaction name on the client. See the [transaction source
    ///    documentation](https://develop.sentry.dev/sdk/event-payloads/properties/transaction_info/)
    ///    for all valid values.
    ///  - `contains_slashes`: Whether the transaction name contains `/`. We use this as a heuristic
    ///    to represent URL transactions.
    EventTransaction,
    /// The number of transaction events processed grouped by transaction name modifications.
    /// This metric is tagged with:
    ///  - `source_in`: The source of the transaction name before normalization.
    ///    See the [transaction source
    ///    documentation](https://develop.sentry.dev/sdk/event-payloads/properties/transaction_info/)
    ///    for all valid values.
    ///  - `change`: The mechanism that changed the transaction name.
    ///    Either `"none"`, `"pattern"`, `"rule"`, or `"both"`.
    ///  - `source_out`: The source of the transaction name after normalization.
    TransactionNameChanges,
    /// Number of HTTP requests reaching Relay.
    Requests,
    /// Number of completed HTTP requests.
    ///
    /// This metric is tagged with:
    ///
    ///  - `status_code`: The HTTP status code number.
    ///  - `method`: The HTTP method used in the request in uppercase.
    ///  - `route`: Unique dashed identifier of the endpoint.
    ResponsesStatusCodes,
    /// Number of evicted stale projects from the cache.
    ///
    /// Relay scans the in-memory project cache for stale entries in a regular interval configured
    /// by `cache.eviction_interval`.
    ///
    /// The cache duration for project states can be configured with the following options:
    ///
    ///  - `cache.project_expiry`: The time after which a project state counts as expired. It is
    ///    automatically refreshed if a request references the project after it has expired.
    ///  - `cache.project_grace_period`: The time after expiry at which the project state will still
    ///    be used to ingest events. Once the grace period expires, the cache is evicted and new
    ///    requests wait for an update.
    EvictingStaleProjectCaches,
    /// Number of times that parsing a metrics bucket item from an envelope failed.
    MetricBucketsParsingFailed,
    /// Number of times that parsing a metric meta item from an envelope failed.
    MetricMetaParsingFailed,
    /// Count extraction of transaction names. Tag with the decision to drop / replace / use original.
    MetricsTransactionNameExtracted,
    /// Number of Events with an OpenTelemetry Context
    ///
    /// This metric is tagged with:
    ///  - `platform`: The event's platform, such as `"javascript"`.
    ///  - `sdk`: The name of the Sentry SDK sending the transaction. This tag is only set for
    ///    Sentry's SDKs and defaults to "proprietary".
    OpenTelemetryEvent,
    /// Number of global config fetches from upstream. Only 2XX responses are
    /// considered and ignores send errors (e.g. auth or network errors).
    ///
    /// This metric is tagged with:
    ///  - `success`: whether deserializing the global config succeeded.
    GlobalConfigFetched,
    /// The number of attachments processed in the same envelope as a user_report_v2 event.
    FeedbackAttachments,
    /// All COGS tracked values.
    ///
    /// This metric is tagged with:
    /// - `resource_id`: The COGS resource id.
    /// - `app_feature`: The COGS app feature.
    CogsUsage,
    /// The amount of times metrics of a project have been flushed without the project being
    /// fetched/available.
    ProjectStateFlushMetricsNoProject,
    /// Incremented every time a bucket is dropped.
    ///
    /// This should only happen when a project state is invalid during graceful shutdown.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    BucketsDropped,
    /// Incremented every time a segment exceeds the expected limit.
    ReplayExceededSegmentLimit,
}

impl CounterMetric for RelayCounters {
    fn name(&self) -> &'static str {
        match self {
            RelayCounters::EventCorrupted => "event.corrupted",
            RelayCounters::EnvelopeAccepted => "event.accepted",
            RelayCounters::EnvelopeRejected => "event.rejected",
            RelayCounters::BufferWritesDisk => "buffer.writes",
            RelayCounters::BufferReadsDisk => "buffer.reads",
            RelayCounters::BufferEnvelopesWritten => "buffer.envelopes_written",
            RelayCounters::BufferEnvelopesRead => "buffer.envelopes_read",
            RelayCounters::BufferEnvelopesReturned => "buffer.envelopes_returned",
            RelayCounters::BufferStateTransition => "buffer.state.transition",
            RelayCounters::BufferEnvelopeStacksPopped => "buffer.envelope_stacks_popped",
            RelayCounters::BufferTryPop => "buffer.try_pop",
            RelayCounters::BufferSpooledEnvelopes => "buffer.spooled_envelopes",
            RelayCounters::BufferUnspooledEnvelopes => "buffer.unspooled_envelopes",
            RelayCounters::Outcomes => "events.outcomes",
            RelayCounters::ProjectStateGet => "project_state.get",
            RelayCounters::ProjectStateRequest => "project_state.request",
            RelayCounters::ProjectStateNoCache => "project_state.no_cache",
            RelayCounters::ProjectStateFlushAllMetricMeta => "project_state.flush_all_metric_meta",
            #[cfg(feature = "processing")]
            RelayCounters::ProjectStateRedis => "project_state.redis.requests",
            RelayCounters::ProjectUpstreamCompleted => "project_upstream.completed",
            RelayCounters::ProjectUpstreamFailed => "project_upstream.failed",
            RelayCounters::ProjectCacheHit => "project_cache.hit",
            RelayCounters::ProjectCacheMiss => "project_cache.miss",
            RelayCounters::ServerStarting => "server.starting",
            #[cfg(feature = "processing")]
            RelayCounters::ProcessingMessageProduced => "processing.event.produced",
            RelayCounters::EventProtocol => "event.protocol",
            RelayCounters::EventTransaction => "event.transaction",
            RelayCounters::TransactionNameChanges => "event.transaction_name_changes",
            RelayCounters::Requests => "requests",
            RelayCounters::ResponsesStatusCodes => "responses.status_codes",
            RelayCounters::EvictingStaleProjectCaches => "project_cache.eviction",
            RelayCounters::MetricBucketsParsingFailed => "metrics.buckets.parsing_failed",
            RelayCounters::MetricMetaParsingFailed => "metrics.meta.parsing_failed",
            RelayCounters::MetricsTransactionNameExtracted => "metrics.transaction_name",
            RelayCounters::OpenTelemetryEvent => "event.opentelemetry",
            RelayCounters::GlobalConfigFetched => "global_config.fetch",
            RelayCounters::FeedbackAttachments => "processing.feedback_attachments",
            RelayCounters::CogsUsage => "cogs.usage",
            RelayCounters::ProjectStateFlushMetricsNoProject => "project_state.metrics.no_project",
            RelayCounters::BucketsDropped => "metrics.buckets.dropped",
            RelayCounters::ReplayExceededSegmentLimit => "replay.segment_limit_exceeded",
        }
    }
}

/// Low-cardinality platform that can be used as a statsd tag.
pub enum PlatformTag {
    Cocoa,
    Csharp,
    Edge,
    Go,
    Java,
    Javascript,
    Julia,
    Native,
    Node,
    Objc,
    Other,
    Perl,
    Php,
    Python,
    Ruby,
    Swift,
}

impl PlatformTag {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Cocoa => "cocoa",
            Self::Csharp => "csharp",
            Self::Edge => "edge",
            Self::Go => "go",
            Self::Java => "java",
            Self::Javascript => "javascript",
            Self::Julia => "julia",
            Self::Native => "native",
            Self::Node => "node",
            Self::Objc => "objc",
            Self::Other => "other",
            Self::Perl => "perl",
            Self::Php => "php",
            Self::Python => "python",
            Self::Ruby => "ruby",
            Self::Swift => "swift",
        }
    }
}

impl<S: AsRef<str>> From<S> for PlatformTag {
    fn from(value: S) -> Self {
        match value.as_ref() {
            "cocoa" => Self::Cocoa,
            "csharp" => Self::Csharp,
            "edge" => Self::Edge,
            "go" => Self::Go,
            "java" => Self::Java,
            "javascript" => Self::Javascript,
            "julia" => Self::Julia,
            "native" => Self::Native,
            "node" => Self::Node,
            "objc" => Self::Objc,
            "perl" => Self::Perl,
            "php" => Self::Php,
            "python" => Self::Python,
            "ruby" => Self::Ruby,
            "swift" => Self::Swift,
            _ => Self::Other,
        }
    }
}
