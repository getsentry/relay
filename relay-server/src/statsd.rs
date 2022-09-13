use relay_statsd::{CounterMetric, GaugeMetric, HistogramMetric, TimerMetric};

/// Gauge metrics used by Relay
pub enum RelayGauges {
    /// The state of Relay with respect to the upstream connection.
    /// Possible values are `0` for normal operations and `1` for a network outage.
    NetworkOutage,

    /// The number of items currently in the garbage disposal queue.
    ProjectCacheGarbageQueueSize,
}

impl GaugeMetric for RelayGauges {
    fn name(&self) -> &'static str {
        match self {
            RelayGauges::NetworkOutage => "upstream.network_outage",
            RelayGauges::ProjectCacheGarbageQueueSize => "project_cache.garbage.queue_size",
        }
    }
}

/// Histogram metrics used by Relay.
pub enum RelayHistograms {
    /// The number of envelopes in the queue as a percentage of the maximum number of envelopes that
    /// can be stored in the queue.
    ///
    /// The value ranges from `0` when the queue is empty to `1` when the queue is full and no
    /// additional events can be added. The queue size can be configured using `event.queue_size`.
    EnvelopeQueueSizePct,
    /// The number of envelopes in the queue.
    ///
    /// The queue holds all envelopes that are being processed at a particular time in Relay:
    ///
    /// - When Relay receives a Request, it ensures the submitted data is wrapped in a single
    ///   envelope.
    /// - The envelope receives some preliminary processing to determine if it can be processed or
    ///   if it must be rejected.
    /// - Once this determination has been made, the HTTP request that created the envelope
    ///   terminates and, if the request is to be further processed, the envelope enters a queue.
    /// - After the envelope finishes processing and is sent upstream, the envelope is considered
    ///   handled and it leaves the queue.
    ///
    /// The queue size can be configured with `cache.event_buffer_size`.
    EnvelopeQueueSize,
    /// The size of the HTTP request body as seen by Relay after it is extracted from a request in
    /// bytes.
    ///
    /// - For envelope requests, this is the full size of the envelope.
    /// - For JSON store requests, this is the size of the JSON body.
    /// - For multipart uploads of crash reports and attachments, this is the size of the multipart
    ///   body including boundaries.
    ///
    /// If this request contains a base64 zlib compressed payload without a proper
    /// `content-encoding` header, then this is the size before decompression.
    ///
    /// The maximum request body size can be configured with `limits.max_envelope_size`.
    RequestSizeBytesRaw,
    /// The size of the request body as seen by Relay after decompression and decoding in bytes.
    ///
    /// JSON store requests may contain a base64 zlib compressed payload without proper
    /// `content-encoding` header. In this case, this metric contains the size after decoding.
    /// Otherwise, it is always equal to `event.size_bytes.raw`.
    RequestSizeBytesUncompressed,
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

    /// Size of emitted kafka message in bytes, tagged by message type.
    #[cfg(feature = "processing")]
    KafkaMessageSize,

    /// Size of envelopes sent over HTTP in bytes.
    UpstreamQueryBodySize,

    /// Size of queries (projectconfig queries, i.e. the request payload, not the response) sent by
    /// Relay over HTTP in bytes.
    UpstreamEnvelopeBodySize,

    /// Counts how often a transaction name was changed before submitting the final transaction.
    ///
    /// A value of `0` indicates that the transaction was created with the final transaction name.
    /// In this case, the DSC will have the correct transaction name guaranteed. However, to
    /// determine how many traces had wrong transaction names propagated, check the
    /// `dynamic_sampling.propagations`.
    ///
    /// This metric is tagged with:
    ///
    ///  - `source`: The transaction source value in the final event payload.
    ///  - `platform`: The SDK platform value of the event payload.
    ///  - `sdk_name`: The name of the client SDK as reported in the event payload.
    ///  - `sdk_version`: The version of the client SDK as reported in the event payload.
    DynamicSamplingChanges,

    /// Counts the number of propagations before the final transaction name has been determined.
    ///
    /// A value of `0` indicates that the entire trace had identical transaction names in the
    /// Dynamic Sampling Context (DSC). This means that the entire trace is sampled consistently.
    ///
    /// Note that this differs from `dynamic_sampling.changes`, which indicates changes even in the
    /// absence of propagations.
    ///
    /// This metric is tagged with:
    ///
    ///  - `source`: The transaction source value in the final event payload.
    ///  - `platform`: The SDK platform value of the event payload.
    ///  - `sdk_name`: The name of the client SDK as reported in the event payload.
    ///  - `sdk_version`: The version of the client SDK as reported in the event payload.
    DynamicSamplingPropagationCount,

    /// The number of propagations before the final transaction name change relative to the total
    /// number of propagations.
    ///
    /// Tracks the same as `dynamic_sampling.propagations`, except that the value of this metric is
    /// a percentage between `0.0` and `1.0`. A value of `0` means that no propagations occurred,
    /// and `1` means that all propagations occurred with the wrong transaction name.
    ///
    /// This metric is tagged with:
    ///
    ///  - `source`: The transaction source value in the final event payload.
    ///  - `platform`: The SDK platform value of the event payload.
    ///  - `sdk_name`: The name of the client SDK as reported in the event payload.
    ///  - `sdk_version`: The version of the client SDK as reported in the event payload.
    DynamicSamplingPropagationPercentage,

    /// Time in milliseconds from the start of transaction until the final name is determined.
    ///
    /// If the transaction name changes multiple times, this records only the last instance. This
    /// metric is not logged if there were no changes to the transaction name.
    ///
    /// This metric is tagged with:
    ///
    ///  - `source`: The transaction source value in the final event payload.
    ///  - `platform`: The SDK platform value of the event payload.
    ///  - `sdk_name`: The name of the client SDK as reported in the event payload.
    ///  - `sdk_version`: The version of the client SDK as reported in the event payload.
    DynamicSamplingChangeDuration,

    /// Timing relative to the transaction duration until the final name is determined.
    ///
    /// This is a percentage between `0.0` and `1.0`.
    ///
    /// This metric is tagged with:
    ///
    ///  - `source`: The transaction source value in the final event payload.
    ///  - `platform`: The SDK platform value of the event payload.
    ///  - `sdk_name`: The name of the client SDK as reported in the event payload.
    ///  - `sdk_version`: The version of the client SDK as reported in the event payload.
    DynamicSamplingChangePercentage,
}

impl HistogramMetric for RelayHistograms {
    fn name(&self) -> &'static str {
        match self {
            RelayHistograms::EnvelopeQueueSizePct => "event.queue_size.pct",
            RelayHistograms::EnvelopeQueueSize => "event.queue_size",
            RelayHistograms::RequestSizeBytesRaw => "event.size_bytes.raw",
            RelayHistograms::RequestSizeBytesUncompressed => "event.size_bytes.uncompressed",
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
            #[cfg(feature = "processing")]
            RelayHistograms::KafkaMessageSize => "kafka.message_size",
            RelayHistograms::UpstreamQueryBodySize => "upstream.query.body_size",
            RelayHistograms::UpstreamEnvelopeBodySize => "upstream.envelope.body_size",
            RelayHistograms::DynamicSamplingChanges => "dynamic_sampling.changes",
            RelayHistograms::DynamicSamplingPropagationCount => "dynamic_sampling.propagations",
            RelayHistograms::DynamicSamplingPropagationPercentage => {
                "dynamic_sampling.propagation_pct"
            }
            RelayHistograms::DynamicSamplingChangeDuration => "dynamic_sampling.change_duration",
            RelayHistograms::DynamicSamplingChangePercentage => "dynamic_sampling.change_pct",
        }
    }
}

/// Timer metrics used by Relay
pub enum RelayTimers {
    /// Time in milliseconds spent deserializing an event from JSON bytes into the native data
    /// structure on which Relay operates.
    EventProcessingDeserialize,
    /// Time in milliseconds spent running light normalization on an event. Light normalization
    /// happens before envelope filtering and metrics extraction.
    EventProcessingLightNormalization,
    /// Time in milliseconds spent running event processors on an event for normalization. Event
    /// processing happens before filtering.
    #[cfg(feature = "processing")]
    EventProcessingProcess,
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

    /// Time in milliseconds spent on converting a transaction event into a metric.
    TransactionMetricsExtraction,
}

impl TimerMetric for RelayTimers {
    fn name(&self) -> &'static str {
        match self {
            RelayTimers::EventProcessingDeserialize => "event_processing.deserialize",
            RelayTimers::EventProcessingLightNormalization => {
                "event_processing.light_normalization"
            }
            #[cfg(feature = "processing")]
            RelayTimers::EventProcessingProcess => "event_processing.process",
            RelayTimers::EventProcessingFiltering => "event_processing.filtering",
            #[cfg(feature = "processing")]
            RelayTimers::EventProcessingRateLimiting => "event_processing.rate_limiting",
            RelayTimers::EventProcessingPii => "event_processing.pii",
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
            RelayTimers::TransactionMetricsExtraction => "metrics.extraction.transactions",
        }
    }
}

/// Counter metrics used by Relay
pub enum RelayCounters {
    /// Number of Events that had corrupted (unprintable) event attributes.
    ///
    /// This currently checks for `environment` and `release`, for which we know that
    /// some SDKs may send corrupted values.
    #[cfg(feature = "processing")]
    EventCorrupted,
    /// Number of envelopes accepted in the current time slot.
    ///
    /// This represents requests that have successfully passed rate limits and filters, and have
    /// been sent to the upstream.
    EnvelopeAccepted,
    /// Number of envelopes rejected in the current time slot.
    ///
    /// This includes envelopes being rejected because they are malformed or any other errors during
    /// processing (including filtered events, invalid payloads, and rate limits).
    ///
    /// To check the rejection reason, check `events.outcomes`, instead.
    EnvelopeRejected,
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
    /// This has a tag `hit` with values `true` or `false`.  If false the request will be
    /// sent to the sentry endpoint.
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
    /// Number of producer errors occurred after an envelope was already enqueued for sending to
    /// Kafka.
    ///
    /// These errors include, for example, _"MessageTooLarge"_ errors when the broker does not
    /// accept the requests over a certain size, which is usually due to invalid or inconsistent
    /// broker/producer configurations.
    #[cfg(feature = "processing")]
    ProcessingProduceError,
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
    ///  - `sdk`: The name of the Sentry SDK sending the transaction. This tag is only set for
    ///    Sentry's SDKs and defaults to "proprietary".
    ///  - `source`: The source of the transaction name on the client. See the [transaction source
    ///    documentation](https://develop.sentry.dev/sdk/event-payloads/properties/transaction_info/)
    ///    for all valid values.
    EventTransactionSource,
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
    /// Count extraction of transaction names. Tag with the decision to drop / replace / use original.
    MetricsTransactionNameExtracted,
}

impl CounterMetric for RelayCounters {
    fn name(&self) -> &'static str {
        match self {
            #[cfg(feature = "processing")]
            RelayCounters::EventCorrupted => "event.corrupted",
            RelayCounters::EnvelopeAccepted => "event.accepted",
            RelayCounters::EnvelopeRejected => "event.rejected",
            RelayCounters::Outcomes => "events.outcomes",
            RelayCounters::ProjectStateGet => "project_state.get",
            RelayCounters::ProjectStateRequest => "project_state.request",
            RelayCounters::ProjectStateNoCache => "project_state.no_cache",
            #[cfg(feature = "processing")]
            RelayCounters::ProjectStateRedis => "project_state.redis.requests",
            RelayCounters::ProjectUpstreamCompleted => "project_upstream.completed",
            RelayCounters::ProjectCacheHit => "project_cache.hit",
            RelayCounters::ProjectCacheMiss => "project_cache.miss",
            RelayCounters::ServerStarting => "server.starting",
            #[cfg(feature = "processing")]
            RelayCounters::ProcessingMessageProduced => "processing.event.produced",
            #[cfg(feature = "processing")]
            RelayCounters::ProcessingProduceError => "processing.produce.error",
            RelayCounters::EventProtocol => "event.protocol",
            RelayCounters::EventTransactionSource => "event.transaction_source",
            RelayCounters::Requests => "requests",
            RelayCounters::ResponsesStatusCodes => "responses.status_codes",
            RelayCounters::EvictingStaleProjectCaches => "project_cache.eviction",
            RelayCounters::MetricBucketsParsingFailed => "metrics.buckets.parsing_failed",
            RelayCounters::MetricsTransactionNameExtracted => "metrics.transaction_name",
        }
    }
}
