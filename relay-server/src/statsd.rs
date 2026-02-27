use relay_statsd::{CounterMetric, DistributionMetric, GaugeMetric, TimerMetric};
#[cfg(doc)]
use relay_system::RuntimeMetrics;

/// Gauge metrics used by Relay
pub enum RelayGauges {
    /// Tracks the number of futures waiting to be executed in the pool's queue.
    ///
    /// Useful for understanding the backlog of work and identifying potential bottlenecks.
    ///
    /// This metric is tagged with:
    /// - `pool`: the name of the pool.
    AsyncPoolQueueSize,
    /// Tracks the utilization of the async pool.
    ///
    /// The utilization is a value between 0.0 and 100.0 which determines how busy the pool is doing
    /// CPU-bound work.
    ///
    /// This metric is tagged with:
    /// - `pool`: the name of the pool.
    AsyncPoolUtilization,
    /// Tracks the activity of the async pool.
    ///
    /// The activity is a value between 0.0 and 100.0 which determines how busy is the pool
    /// w.r.t. to its provisioned capacity.
    ///
    /// This metric is tagged with:
    /// - `pool`: the name of the pool.
    AsyncPoolActivity,
    /// The state of Relay with respect to the upstream connection.
    /// Possible values are `0` for normal operations and `1` for a network outage.
    NetworkOutage,
    /// Number of elements in the envelope buffer across all the stacks.
    ///
    /// This metric is tagged with:
    /// - `storage_type`: The type of storage used in the envelope buffer.
    BufferEnvelopesCount,
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
    /// The maximum number of connections in the Redis pool.
    #[cfg(feature = "processing")]
    RedisPoolMaxConnections,
    /// The number of futures waiting to grab a connection.
    #[cfg(feature = "processing")]
    RedisPoolWaitingForConnection,
    /// The number of notifications in the broadcast channel of the project cache.
    ProjectCacheNotificationChannel,
    /// The number of scheduled and in progress fetches in the project cache.
    ProjectCacheScheduledFetches,
    /// Exposes the amount of currently open and handled connections by the server.
    ServerActiveConnections,
    /// Maximum delay of a metric bucket in seconds.
    ///
    /// The maximum is measured from initial creation of the bucket in an internal Relay
    /// until it is produced to Kafka.
    ///
    /// This metric is tagged with:
    /// - `namespace`: the metric namespace.
    #[cfg(feature = "processing")]
    MetricDelayMax,
    /// Estimated percentage [0-100] of how busy Relay's internal services are.
    ///
    /// This metric is tagged with:
    /// - `service`: the service name.
    /// - `instance_id`: a for the service name unique identifier for the running service
    ServiceUtilization,
    /// Number of attachment uploads currently in flight.
    #[cfg(feature = "processing")]
    ConcurrentAttachmentUploads,
}

impl GaugeMetric for RelayGauges {
    fn name(&self) -> &'static str {
        match self {
            Self::AsyncPoolQueueSize => "async_pool.queue_size",
            Self::AsyncPoolUtilization => "async_pool.utilization",
            Self::AsyncPoolActivity => "async_pool.activity",
            Self::NetworkOutage => "upstream.network_outage",
            Self::BufferEnvelopesCount => "buffer.envelopes_count",
            Self::BufferStackCount => "buffer.stack_count",
            Self::BufferDiskUsed => "buffer.disk_used",
            Self::SystemMemoryUsed => "health.system_memory.used",
            Self::SystemMemoryTotal => "health.system_memory.total",
            #[cfg(feature = "processing")]
            Self::RedisPoolConnections => "redis.pool.connections",
            #[cfg(feature = "processing")]
            Self::RedisPoolIdleConnections => "redis.pool.idle_connections",
            #[cfg(feature = "processing")]
            Self::RedisPoolMaxConnections => "redis.pool.max_connections",
            #[cfg(feature = "processing")]
            Self::RedisPoolWaitingForConnection => "redis.pool.waiting_for_connection",
            Self::ProjectCacheNotificationChannel => "project_cache.notification_channel.size",
            Self::ProjectCacheScheduledFetches => "project_cache.fetches.size",
            Self::ServerActiveConnections => "server.http.connections",
            #[cfg(feature = "processing")]
            Self::MetricDelayMax => "metrics.delay.max",
            Self::ServiceUtilization => "service.utilization",
            #[cfg(feature = "processing")]
            Self::ConcurrentAttachmentUploads => "attachment.upload.concurrent",
        }
    }
}

/// Gauge metrics collected from the Runtime.
pub enum RuntimeGauges {
    /// Exposes [`RuntimeMetrics::num_idle_threads`].
    NumIdleThreads,
    /// Exposes [`RuntimeMetrics::num_alive_tasks`].
    NumAliveTasks,
    /// Exposes [`RuntimeMetrics::blocking_queue_depth`].
    BlockingQueueDepth,
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
    /// Exposes [`RuntimeMetrics::worker_mean_poll_time`].
    ///
    /// This metric is tagged with:
    /// - `worker`: the worker id.
    WorkerMeanPollTime,
}

impl GaugeMetric for RuntimeGauges {
    fn name(&self) -> &'static str {
        match self {
            RuntimeGauges::NumIdleThreads => "runtime.idle_threads",
            RuntimeGauges::NumAliveTasks => "runtime.alive_tasks",
            RuntimeGauges::BlockingQueueDepth => "runtime.blocking_queue_depth",
            RuntimeGauges::NumBlockingThreads => "runtime.num_blocking_threads",
            RuntimeGauges::NumIdleBlockingThreads => "runtime.num_idle_blocking_threads",
            RuntimeGauges::NumWorkers => "runtime.num_workers",
            RuntimeGauges::WorkerLocalQueueDepth => "runtime.worker_local_queue_depth",
            RuntimeGauges::WorkerMeanPollTime => "runtime.worker_mean_poll_time",
        }
    }
}

/// Counter metrics collected from the Runtime.
pub enum RuntimeCounters {
    /// Exposes [`RuntimeMetrics::budget_forced_yield_count`].
    BudgetForcedYieldCount,
    /// Exposes [`RuntimeMetrics::worker_local_schedule_count`].
    ///
    /// This metric is tagged with:
    /// - `worker`: the worker id.
    WorkerLocalScheduleCount,
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

impl CounterMetric for RuntimeCounters {
    fn name(&self) -> &'static str {
        match self {
            RuntimeCounters::BudgetForcedYieldCount => "runtime.budget_forced_yield_count",
            RuntimeCounters::WorkerLocalScheduleCount => "runtime.worker_local_schedule_count",
            RuntimeCounters::WorkerNoopCount => "runtime.worker_noop_count",
            RuntimeCounters::WorkerOverflowCount => "runtime.worker_overflow_count",
            RuntimeCounters::WorkerParkCount => "runtime.worker_park_count",
            RuntimeCounters::WorkerPollCount => "runtime.worker_poll_count",
            RuntimeCounters::WorkerStealCount => "runtime.worker_steal_count",
            RuntimeCounters::WorkerStealOperations => "runtime.worker_steal_operations",
            RuntimeCounters::WorkerTotalBusyDuration => "runtime.worker_total_busy_duration",
        }
    }
}

/// Histogram metrics used by Relay.
pub enum RelayDistributions {
    /// The number of bytes received by Relay for each individual envelope item type.
    ///
    /// This metric is tagged with:
    ///  - `item_type`: The type of the items being counted.
    ///  - `is_container`: Whether this item is a container holding multiple items.
    EnvelopeItemSize,
    /// The amount of bytes in the item payloads of an envelope pushed to the envelope buffer.
    ///
    /// This is not quite the same as the actual size of a serialized envelope, because it ignores
    /// the envelope header and item headers.
    BufferEnvelopeBodySize,
    /// Size of a serialized envelope pushed to the envelope buffer.
    BufferEnvelopeSize,
    /// Size of a compressed envelope pushed to the envelope buffer.
    BufferEnvelopeSizeCompressed,
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
    /// Canonical size of a Trace Item.
    ///
    /// This is not the size in bytes, this is using the same algorithm we're using for the logs
    /// billing category.
    ///
    /// This metric is tagged with:
    ///  - `item`: the trace item type.
    ///  - `too_large`: `true` or `false`, whether the item is bigger than the allowed size limit.
    TraceItemCanonicalSize,
    /// The Content-Length of incoming HTTP requests in bytes.
    ///
    /// This metric is tagged with:
    ///  - `has_content_length`: Whether the Content-Length header was present ("true"/"false").
    ///  - `route`: The matched route pattern.
    ///  - `status_code`: The HTTP response status code.
    ContentLength,
}

impl DistributionMetric for RelayDistributions {
    fn name(&self) -> &'static str {
        match self {
            Self::EnvelopeItemSize => "event.item_size",
            Self::EventSpans => "event.spans",
            Self::BatchesPerPartition => "metrics.buckets.batches_per_partition",
            Self::BucketsPerBatch => "metrics.buckets.per_batch",
            Self::BufferEnvelopeBodySize => "buffer.envelope_body_size",
            Self::BufferEnvelopeSize => "buffer.envelope_size",
            Self::BufferEnvelopeSizeCompressed => "buffer.envelope_size.compressed",
            Self::ProjectStatePending => "project_state.pending",
            Self::ProjectStateAttempts => "project_state.attempts",
            Self::ProjectStateRequestBatchSize => "project_state.request.batch_size",
            Self::ProjectStateReceived => "project_state.received",
            Self::ProjectStateCacheSize => "project_cache.size",
            #[cfg(feature = "processing")]
            Self::ProjectStateSizeBytesCompressed => "project_state.size_bytes.compressed",
            #[cfg(feature = "processing")]
            Self::ProjectStateSizeBytesDecompressed => "project_state.size_bytes.decompressed",
            Self::UpstreamMessageQueueSize => "http_queue.size",
            Self::UpstreamRetries => "upstream.retries",
            Self::UpstreamQueryBodySize => "upstream.query.body_size",
            Self::UpstreamEnvelopeBodySize => "upstream.envelope.body_size",
            Self::UpstreamMetricsBodySize => "upstream.metrics.body_size",
            Self::PartitionKeys => "metrics.buckets.partition_keys",
            Self::PartitionSplits => "partition_splits",
            Self::TraceItemCanonicalSize => "trace_item.canonical_size",
            Self::ContentLength => "requests.content_length",
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
    ///
    /// This metric is tagged with:
    ///  - `type`: The type of limiter executed, `cached` or `consistent`.
    ///  - `unit`: The item/unit of work which is being rate limited, only available for new
    ///    processing pipelines.
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
    /// Latency of project config updates until they reach Relay.
    ///
    /// The metric is calculated by using the creation timestamp of the project config
    /// and when Relay updates its local cache with the new project config.
    ///
    /// No metric is emitted when Relay fetches a project config for the first time.
    ///
    /// This metric is tagged with:
    ///  - `delay`: Bucketed amount of seconds passed between fetches.
    ProjectCacheUpdateLatency,
    /// Total time spent from starting to fetch a project config update to completing the fetch.
    ProjectCacheFetchDuration,
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
    ///   was an error during scrubbing and finally "n/a" means scrubbing was successful
    ///   but no scurbbing rules applied.
    MinidumpScrubbing,
    /// Time spent on view hierarchy scrubbing.
    ///
    /// This is the total time spent on parsing and scrubbing the view hierarchy json file.
    ///
    /// This metric is tagged with:
    ///
    /// - `status`: "ok" means successful scrubbed, "error" means there was an error during
    ///   scrubbing
    ViewHierarchyScrubbing,
    /// Time spend on attachment scrubbing.
    ///
    /// This represents the total time spent on evaluating the scrubbing rules for an
    /// attachment and the attachment scrubbing itself, regardless of whether any rules were
    /// applied.  Note that minidumps which failed to be parsed (status="error" in
    /// scrubbing.minidumps.duration) will be scrubbed as plain attachments and count
    /// towards this.
    ///
    /// This metric is tagged with:
    ///
    ///   - `attachment_type`: The type of attachment, e.g. "minidump".
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
    /// Timing in milliseconds for processing a task in the project cache service.
    ///
    /// This metric is tagged with:
    /// - `task`: The type of the task the project cache does.
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
    /// Timing in milliseconds for processing a task in the aggregator service.
    ///
    /// This metric is tagged with:
    ///  - `task`: The task being executed by the aggregator.
    ///  - `aggregator`: The name of the aggregator.
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
    /// Timing in milliseconds for the time it takes for the buffer to pack & spool a batch.
    ///
    /// Contains the time it takes to pack multiple envelopes into a single memory blob.
    BufferSpool,
    /// Timing in milliseconds for the time it takes for the buffer to spool data to SQLite.
    BufferSqlWrite,
    /// Timing in milliseconds for the time it takes for the buffer to unspool data from disk.
    BufferUnspool,
    /// Timing in milliseconds for the time it takes for the buffer to push.
    BufferPush,
    /// Timing in milliseconds for the time it takes for the buffer to peek.
    BufferPeek,
    /// Timing in milliseconds for the time it takes for the buffer to pop.
    BufferPop,
    /// Timing in milliseconds for the time it takes for the buffer to drain its envelopes.
    BufferDrain,
    /// Timing in milliseconds for the time it takes for an envelope to be serialized.
    BufferEnvelopesSerialization,
    /// Timing in milliseconds for the time it takes for an envelope to be compressed.
    BufferEnvelopeCompression,
    /// Timing in milliseconds for the time it takes for an envelope to be decompressed.
    BufferEnvelopeDecompression,
    /// Timing in milliseconds to count spans in a serialized transaction payload.
    CheckNestedSpans,
    /// The time it needs to create a signature. Includes both the signature used for
    /// trusted relays and for register challenges.
    SignatureCreationDuration,
    /// Time needed to upload an attachment to objectstore.
    ///
    /// Tagged by:
    /// - `type`: "envelope" or "attachment_v2".
    #[cfg(feature = "processing")]
    AttachmentUploadDuration,
}

impl TimerMetric for RelayTimers {
    fn name(&self) -> &'static str {
        match self {
            RelayTimers::EventProcessingDeserialize => "event_processing.deserialize",
            RelayTimers::EventProcessingNormalization => "event_processing.normalization",
            RelayTimers::EventProcessingFiltering => "event_processing.filtering",
            RelayTimers::EventProcessingRateLimiting => "event_processing.rate_limiting",
            RelayTimers::EventProcessingPii => "event_processing.pii",
            RelayTimers::EventProcessingSpanMetricsExtraction => {
                "event_processing.span_metrics_extraction"
            }
            RelayTimers::EventProcessingSerialization => "event_processing.serialization",
            RelayTimers::EnvelopeWaitTime => "event.wait_time",
            RelayTimers::EnvelopeProcessingTime => "event.processing_time",
            RelayTimers::EnvelopeTotalTime => "event.total_time",
            RelayTimers::ProjectStateRequestDuration => "project_state.request.duration",
            #[cfg(feature = "processing")]
            RelayTimers::ProjectStateDecompression => "project_state.decompression",
            RelayTimers::ProjectCacheUpdateLatency => "project_cache.latency",
            RelayTimers::ProjectCacheFetchDuration => "project_cache.fetch.duration",
            RelayTimers::RequestsDuration => "requests.duration",
            RelayTimers::MinidumpScrubbing => "scrubbing.minidumps.duration",
            RelayTimers::ViewHierarchyScrubbing => "scrubbing.view_hierarchy_scrubbing.duration",
            RelayTimers::AttachmentScrubbing => "scrubbing.attachments.duration",
            RelayTimers::UpstreamRequestsDuration => "upstream.requests.duration",
            RelayTimers::TimestampDelay => "requests.timestamp_delay",
            RelayTimers::OutcomeAggregatorFlushTime => "outcomes.aggregator.flush_time",
            RelayTimers::ReplayRecordingProcessing => "replay.recording.process",
            RelayTimers::GlobalConfigRequestDuration => "global_config.requests.duration",
            RelayTimers::ProcessMessageDuration => "processor.message.duration",
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
            RelayTimers::BufferSqlWrite => "buffer.write.duration",
            RelayTimers::BufferUnspool => "buffer.unspool.duration",
            RelayTimers::BufferPush => "buffer.push.duration",
            RelayTimers::BufferPeek => "buffer.peek.duration",
            RelayTimers::BufferPop => "buffer.pop.duration",
            RelayTimers::BufferDrain => "buffer.drain.duration",
            RelayTimers::BufferEnvelopesSerialization => "buffer.envelopes_serialization",
            RelayTimers::BufferEnvelopeCompression => "buffer.envelopes_compression",
            RelayTimers::BufferEnvelopeDecompression => "buffer.envelopes_decompression",
            RelayTimers::CheckNestedSpans => "envelope.check_nested_spans",
            RelayTimers::SignatureCreationDuration => "signature.create.duration",
            #[cfg(feature = "processing")]
            RelayTimers::AttachmentUploadDuration => "attachment.upload.duration",
        }
    }
}

/// Counter metrics used by Relay
pub enum RelayCounters {
    /// Tracks the number of tasks driven to completion by the async pool.
    ///
    /// This metric is tagged with:
    /// - `pool`: the name of the pool.
    AsyncPoolFinishedTasks,
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
    /// Number of total envelope items we received.
    ///
    /// Note: This does not count raw items, it counts the logical amount of items,
    /// e.g. a single item container counts all its contained items.
    ///
    /// This metric is tagged with:
    ///  - `item_type`: The type of the items being counted.
    ///  - `is_container`: Whether this item is a container holding multiple items.
    ///  - `sdk`: The name of the Sentry SDK sending the envelope. This tag is only set for
    ///    Sentry's SDKs and defaults to "proprietary".
    EnvelopeItems,
    /// Number of bytes we processed per envelope item.
    ///
    /// This metric is tagged with:
    ///  - `item_type`: The type of the items being counted.
    ///  - `is_container`: Whether this item is a container holding multiple items.
    ///  - `sdk`: The name of the Sentry SDK sending the envelope. This tag is only set for
    ///    Sentry's SDKs and defaults to "proprietary".
    EnvelopeItemBytes,
    /// Number of times an envelope from the buffer is trying to be popped.
    BufferTryPop,
    /// Number of envelopes spool to disk.
    BufferSpooledEnvelopes,
    /// Number of envelopes unspooled from disk.
    BufferUnspooledEnvelopes,
    /// Number of project changed updates received by the buffer.
    BufferProjectChangedEvent,
    /// Number of times one or more projects of an envelope were pending when trying to pop
    /// their envelope.
    BufferProjectPending,
    /// Number of iterations of the envelope buffer service loop.
    BufferServiceLoopIteration,
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
    /// The number of individual outcomes including their quantity.
    ///
    /// While [`RelayCounters::Outcomes`] tracks the number of times aggregated outcomes
    /// have been emitted, this counter tracks the total quantity of individual outcomes.
    OutcomeQuantity,
    /// Number of project state HTTP requests.
    ///
    /// Relay updates projects in batches. Every update cycle, Relay requests
    /// `limits.max_concurrent_queries` batches of `cache.batch_size` projects from the upstream.
    /// The duration of these requests is reported via `project_state.request.duration`.
    ///
    /// Note that after an update loop has completed, there may be more projects pending updates.
    /// This is indicated by `project_state.pending`.
    ProjectStateRequest,
    /// Number of times a project state is requested from the central Redis cache.
    ///
    /// This metric is tagged with:
    ///  - `hit`: One of:
    ///     - `revision`: the cached version was validated to be up to date using its revision.
    ///     - `project_config`: the request was handled by the cache.
    ///     - `project_config_revision`: the request was handled by the cache and the revision did
    ///       not change.
    ///     - `false`: the request will be sent to the sentry endpoint.
    #[cfg(feature = "processing")]
    ProjectStateRedis,
    /// Number of times a project had a fetch scheduled.
    ProjectCacheSchedule,
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
    /// Number of spans produced in the new format.
    #[cfg(feature = "processing")]
    SpanV2Produced,
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
    /// Number of refreshes for stale projects in the cache.
    RefreshStaleProjectCaches,
    /// Number of times that parsing a metrics bucket item from an envelope failed.
    MetricBucketsParsingFailed,
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
    /// Incremented every time the server accepts a new connection.
    ServerSocketAccept,
    /// Incremented every time the server aborts a connection because of an idle timeout.
    ServerConnectionIdleTimeout,
    /// The total delay of metric buckets in seconds.
    ///
    /// The delay is measured from initial creation of the bucket in an internal Relay
    /// until it is produced to Kafka.
    ///
    /// Use [`Self::MetricDelayCount`] to calculate the average delay.
    ///
    /// This metric is tagged with:
    /// - `namespace`: the metric namespace.
    #[cfg(feature = "processing")]
    MetricDelaySum,
    /// The amount of buckets counted for the [`Self::MetricDelaySum`] metric.
    ///
    /// This metric is tagged with:
    /// - `namespace`: the metric namespace.
    #[cfg(feature = "processing")]
    MetricDelayCount,
    /// The amount of times PlayStation processing was attempted.
    #[cfg(all(sentry, feature = "processing"))]
    PlaystationProcessing,
    /// The number of times a sampling decision was made.
    ///
    /// This metric is tagged with:
    /// - `item`: what item the decision is taken for (transaction vs span).
    SamplingDecision,
    /// The number of times an upload of an attachment occurs.
    ///
    /// This metric is tagged with:
    /// - `result`: `success` or the failure reason.
    /// - `type`: `envelope` or `attachment_v2`
    #[cfg(feature = "processing")]
    AttachmentUpload,
    /// Whether a logs envelope has a trace context header or not
    ///
    /// This metric is tagged with:
    /// - `dsc`: yes or no
    /// - `sdk`: low-cardinality client name
    EnvelopeWithLogs,
    /// Amount of profile chunks without a platform item header.
    ///
    /// The metric is emitted when processing profile chunks, profile chunks which are fast path
    /// rate limited are not counted in this metric.
    ProfileChunksWithoutPlatform,
}

impl CounterMetric for RelayCounters {
    fn name(&self) -> &'static str {
        match self {
            RelayCounters::AsyncPoolFinishedTasks => "async_pool.finished_tasks",
            RelayCounters::EventCorrupted => "event.corrupted",
            RelayCounters::EnvelopeAccepted => "event.accepted",
            RelayCounters::EnvelopeRejected => "event.rejected",
            RelayCounters::EnvelopeItems => "event.items",
            RelayCounters::EnvelopeItemBytes => "event.item_bytes",
            RelayCounters::BufferTryPop => "buffer.try_pop",
            RelayCounters::BufferSpooledEnvelopes => "buffer.spooled_envelopes",
            RelayCounters::BufferUnspooledEnvelopes => "buffer.unspooled_envelopes",
            RelayCounters::BufferProjectChangedEvent => "buffer.project_changed_event",
            RelayCounters::BufferProjectPending => "buffer.project_pending",
            RelayCounters::BufferServiceLoopIteration => "buffer.service_loop_iteration",
            RelayCounters::Outcomes => "events.outcomes",
            RelayCounters::OutcomeQuantity => "events.outcome_quantity",
            RelayCounters::ProjectStateRequest => "project_state.request",
            #[cfg(feature = "processing")]
            RelayCounters::ProjectStateRedis => "project_state.redis.requests",
            RelayCounters::ProjectUpstreamCompleted => "project_upstream.completed",
            RelayCounters::ProjectUpstreamFailed => "project_upstream.failed",
            RelayCounters::ProjectCacheSchedule => "project_cache.schedule",
            RelayCounters::ServerStarting => "server.starting",
            #[cfg(feature = "processing")]
            RelayCounters::ProcessingMessageProduced => "processing.event.produced",
            #[cfg(feature = "processing")]
            RelayCounters::SpanV2Produced => "store.produced.span_v2",
            RelayCounters::EventProtocol => "event.protocol",
            RelayCounters::EventTransaction => "event.transaction",
            RelayCounters::TransactionNameChanges => "event.transaction_name_changes",
            RelayCounters::Requests => "requests",
            RelayCounters::ResponsesStatusCodes => "responses.status_codes",
            RelayCounters::EvictingStaleProjectCaches => "project_cache.eviction",
            RelayCounters::RefreshStaleProjectCaches => "project_cache.refresh",
            RelayCounters::MetricBucketsParsingFailed => "metrics.buckets.parsing_failed",
            RelayCounters::OpenTelemetryEvent => "event.opentelemetry",
            RelayCounters::GlobalConfigFetched => "global_config.fetch",
            RelayCounters::FeedbackAttachments => "processing.feedback_attachments",
            RelayCounters::CogsUsage => "cogs.usage",
            RelayCounters::ProjectStateFlushMetricsNoProject => "project_state.metrics.no_project",
            RelayCounters::BucketsDropped => "metrics.buckets.dropped",
            RelayCounters::ReplayExceededSegmentLimit => "replay.segment_limit_exceeded",
            RelayCounters::ServerSocketAccept => "server.http.accepted",
            RelayCounters::ServerConnectionIdleTimeout => "server.http.idle_timeout",
            #[cfg(feature = "processing")]
            RelayCounters::MetricDelaySum => "metrics.delay.sum",
            #[cfg(feature = "processing")]
            RelayCounters::MetricDelayCount => "metrics.delay.count",
            #[cfg(all(sentry, feature = "processing"))]
            RelayCounters::PlaystationProcessing => "processing.playstation",
            RelayCounters::SamplingDecision => "sampling.decision",
            #[cfg(feature = "processing")]
            RelayCounters::AttachmentUpload => "attachment.upload",
            RelayCounters::EnvelopeWithLogs => "logs.envelope",
            RelayCounters::ProfileChunksWithoutPlatform => "profile_chunk.no_platform",
        }
    }
}
