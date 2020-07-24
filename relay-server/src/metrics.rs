use relay_common::metrics::{CounterMetric, HistogramMetric, SetMetric, TimerMetric};

/// Set metrics used by Relay
pub enum RelaySets {
    /// Represents the number of active projects in the current slice of time
    UniqueProjects,
}

impl SetMetric for RelaySets {
    fn name(&self) -> &'static str {
        match self {
            RelaySets::UniqueProjects => "unique_projects",
        }
    }
}

/// Histogram metrics used by Relay.
pub enum RelayHistograms {
    /// The number of envelopes in the queue as a percentage of the maximum number of envelopes that
    /// can be stored in the queue.
    ///
    /// The value ranges from `0` (the queue is empty) to `1` (the queue is full and no additional
    /// events can be added).
    EnvelopeQueueSizePct,
    /// The number of envelopes in the queue.
    ///
    /// The event queue represents the envelopes that are being processed at a particular time in
    /// Relay. Once a request is received, the envelope receives some preliminary (quick) processing
    /// to determine if it can be processed or it is rejected. Once this determination has been
    /// done, the http request that created the envelope terminates and, if the request is to be
    /// further processed, the envelope enters a queue.
    ///
    /// Once the envelope finishes processing and is sent downstream, the envelope is considered
    /// handled and it leaves the queue.
    EnvelopeQueueSize,
    /// The size of the request body as seen by Relay after it is extracted from a request.
    ///
    /// For envelope requests, this is the full size of the envelope. For JSON store requests, this
    /// is the size of the JSON body.
    ///
    /// If this request contains a base64 zlib compressed payload without a proper
    /// `content-encoding` header, then this is the size before decompression.
    RequestSizeBytesRaw,
    /// The size of the request body as seen by Relay after it has been decompressed and decoded in
    /// case this request contains a base64 zlib compressed payload without a proper
    /// `content-encoding` header. Otherwise, this metric is always equal to `event.size_bytes.raw`.
    RequestSizeBytesUncompressed,
    /// Number of projects in the in-memory project cache that are waiting for their state to be
    /// updated.
    ProjectStatePending,
    /// Number of project states requested from the Upstream for the current batch request.
    ProjectStateRequestBatchSize,
    /// Number of project states received from the Upstream for the current batch request.
    ProjectStateReceived,
    /// Number of project states currently held in the in-memory project cache.
    ProjectStateCacheSize,
    /// The number of upstream requests queued up for a connection in the connection pool.
    ConnectorWaitQueue,
}

impl HistogramMetric for RelayHistograms {
    fn name(&self) -> &'static str {
        match self {
            RelayHistograms::EnvelopeQueueSizePct => "event.queue_size.pct",
            RelayHistograms::EnvelopeQueueSize => "event.queue_size",
            RelayHistograms::RequestSizeBytesRaw => "event.size_bytes.raw",
            RelayHistograms::RequestSizeBytesUncompressed => "event.size_bytes.uncompressed",
            RelayHistograms::ProjectStatePending => "project_state.pending",
            RelayHistograms::ProjectStateRequestBatchSize => "project_state.request.batch_size",
            RelayHistograms::ProjectStateReceived => "project_state.received",
            RelayHistograms::ProjectStateCacheSize => "project_cache.size",
            RelayHistograms::ConnectorWaitQueue => "connector.wait_queue",
        }
    }
}

/// Timer metrics used by Relay
pub enum RelayTimers {
    /// The time spent deserializing an event from a JSON byte array into the native data structure
    /// on which Relay operates.
    EventProcessingDeserialize,
    /// Time spent running event processors on an event. Event processing happens before filtering.
    #[cfg(feature = "processing")]
    EventProcessingProcess,
    /// Time spent running filtering on an event.
    #[cfg(feature = "processing")]
    EventProcessingFiltering,
    /// Time spent checking for rate limits in Redis.
    ///
    /// Note that not all events are checked against Redis. After an event is rate limited for the
    /// first time, the rate limit is cached. Events coming in during this period will be discarded
    /// earlier in the request queue and do not reach the processing queue.
    #[cfg(feature = "processing")]
    EventProcessingRateLimiting,
    /// Time spent in data scrubbing for the current event. Data scrubbing happens last before
    /// serializing the event back to JSON.
    EventProcessingPii,
    /// Time spent converting the event from its in-memory reprsentation into a JSON string.
    EventProcessingSerialization,
    /// Time spent between receiving a request in Relay (that is, beginning of request handling) and
    /// the start of synchronous processing in the EventProcessor. This metric primarily indicates
    /// backlog in event processing.
    EnvelopeWaitTime,
    /// The time spent in synchronous processing of envelopes.
    ///
    /// This timing covers the end-to-end processing in the CPU pool and comprises:
    ///
    ///  - `event_processing.deserialize`
    ///  - `event_processing.pii`
    ///  - `event_processing.serialization`
    ///
    /// With Relay in processing mode, this includes the following additional timings:
    ///
    ///  - `event_processing.process`
    ///  - `event_processing.filtering`
    ///  - `event_processing.rate_limiting`
    EnvelopeProcessingTime,
    /// The total time an envelope spends in Relay from the time it is received until it finishes
    /// processing and has been submitted.
    EnvelopeTotalTime,
    /// The total time spent during `ProjectCache.fetch_states` in which eviction of outdated
    /// projects happens.
    ProjectStateEvictionDuration,
    /// The total time spent during `ProjectCache.fetch_states` spent waiting for all ProjectState
    /// requests to resolve. During a fetch_states request, we pick up to max_num_requests *
    /// max_num_project_states_per_request projects that need their state updated and batch
    /// them into max_num_requests requests. This metric represents the time spent from issuing
    /// the first request until all requests are finished.
    ProjectStateRequestDuration,
    /// The total time spent getting the project id from upstream.
    /// **Note** that ProjectIdRequests happen only for the legacy
    /// endpoint that does not specify the project id in the url, for the new endpoints the
    /// project id is extracted from the url path. Only projects with the id not already fetched
    /// are counted.
    /// The project id is only fetched once and it is not refreshed.
    ProjectIdRequestDuration,
    /// The total duration of a request as seen from Relay from the moment the request is
    /// received until a http result is returned. Note that this does **not** represent the
    /// total duration for processing an event. Requests for events that are not immediately
    /// rejected ( because the project has hit a rate limit) are scheduled for processing at
    /// a latter time and an HTTP OK (200) is returned.
    RequestsDuration,
}

impl TimerMetric for RelayTimers {
    fn name(&self) -> &'static str {
        match self {
            RelayTimers::EventProcessingDeserialize => "event_processing.deserialize",
            #[cfg(feature = "processing")]
            RelayTimers::EventProcessingProcess => "event_processing.process",
            #[cfg(feature = "processing")]
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
            RelayTimers::ProjectIdRequestDuration => "project_id.request.duration",
            RelayTimers::RequestsDuration => "requests.duration",
        }
    }
}

/// Counter metrics used by Relay
pub enum RelayCounters {
    /// Number of envelopes accepted in the current time slot. This represents requests that have
    /// successfully passed rate limits, filters and have been successfully handled.
    EnvelopeAccepted,
    /// Number of envelopes rejected in the current time slot. This includes envelopes being
    /// rejected because they are malformed or any other errors during processing (including
    /// filtered events, invalid payloads and rate limits).
    EnvelopeRejected,
    /// Represents a group of counters incremented for every outcome emitted by Relay, implemented
    /// with tags. The following tags are present for each event outcome:
    ///
    /// - `outcome` which is an `Outcome` enumeration
    /// - `reason` which is the reason string for all outcomes that are not `Accepted`.
    #[cfg(feature = "processing")]
    Outcomes,
    /// Counts the number of times a project state lookup is done. This includes requests
    /// for projects that are cached and requests for projects that are not yet cached.
    /// All requests that return a  `EventAction::Accept` i.e. are not rate limited (on
    /// the fast path) or are discarded because we know the project is disabled or invalid
    /// will be counted.
    ProjectStateGet,
    /// Counts the number of project state http requests. Note that a project state HTTP request
    /// typically contains a number of projects (the project state requests are batched).
    ProjectStateRequest,
    /// Counts the number of times a request for a project is already present, this effectively
    /// represents the fraction of `project_state.get` that will **not** result in a ProjectState
    /// request.
    ProjectCacheHit,
    /// Counts the number of times a request for a project is not already present.
    /// `project_state.get` = `project_cache.miss` + `project_cache.hit`.
    /// Requests that are generating a cache hit will be queued and batched and eventually will
    /// generate a `project_state.request`.
    ProjectCacheMiss,
    /// Counts the number of requests for the  ProjectId (the timing is tracked
    /// by `project_id.request.duration`). Note that ProjectIdRequests happen only for the legacy
    /// endpoint that does not specify the project id in the url, for the new endpoints the
    /// project id is extracted from the url path. Only projects with the id not already fetched
    /// are counted. Once the ProjectId is successfully cached it will be retained indefinitely.
    ProjectIdRequest,
    /// Counts the number of times Relay started.
    /// This can be used to track unwanted restarts due to crashes or termination.
    ServerStarting,
    /// Counts the number of messages placed on the Kafka queue.
    ///
    /// When Relay operates with processing enabled and an item is successfully processed, each item
    /// will generate a message on the Kafka. The counter has an `event_type` tag which is set to
    /// either `event` or `attachment` representing the type of message produced on the Kafka queue.
    #[cfg(feature = "processing")]
    ProcessingMessageProduced,
    /// Counts the number of producer errors occurred after an event was already enqueued for
    /// sending to Kafka. These errors might include e.g. MessageTooLarge errors when the broker
    /// does not accept the requests over a certain size, which is usually due to invalic or
    /// inconsistent broker/producer configurations.
    #[cfg(feature = "processing")]
    ProcessingProduceError,
    /// Counts the number of events that hit any of the Store like endpoints (Store, Security,
    /// MiniDump, Unreal). The events are counted before they are rate limited , filtered or
    /// processed in any way. The counter has a `version` tag that tracks the message event
    /// protocol version.
    EventProtocol,
    /// Counts the number of requests reaching Relay.
    Requests,
    /// Counts the number of requests that have finished during the current interval.
    /// The counter has the following tags:
    ///
    /// - `status_code` The HTTP status code number.
    /// - `method` The HTTP method used in the request in uppercase.
    /// - `route` Unique dashed identifier of the endpoint.
    ResponsesStatusCodes,
    /// We are scanning our in-memory project cache for stale entries. This counter is incremented
    /// before doing the expensive operation.
    EvictingStaleProjectCaches,
    /// The number of requests that reused an already open upstream connection.
    ///
    /// Relay employs connection keep-alive whenever possible. Connections are kept open for 15
    /// seconds of inactivity, or 75 seconds of activity.
    ConnectorReused,
    /// The number of upstream connections opened.
    ConnectorOpened,
    /// The number of upstream connections closed due to connection timeouts.
    ///
    /// Relay employs connection keep-alive whenever possible. Connections are kept open for 15
    /// seconds of inactivity, or 75 seconds of activity.
    ConnectorClosed,
    /// The number of upstream connections that experienced errors.
    ConnectorErrors,
    /// The number of upstream connections that experienced a timeout.
    ConnectorTimeouts,
}

impl CounterMetric for RelayCounters {
    fn name(&self) -> &'static str {
        match self {
            RelayCounters::EnvelopeAccepted => "event.accepted",
            RelayCounters::EnvelopeRejected => "event.rejected",
            #[cfg(feature = "processing")]
            RelayCounters::Outcomes => "events.outcomes",
            RelayCounters::ProjectStateGet => "project_state.get",
            RelayCounters::ProjectStateRequest => "project_state.request",
            RelayCounters::ProjectCacheHit => "project_cache.hit",
            RelayCounters::ProjectCacheMiss => "project_cache.miss",
            RelayCounters::ProjectIdRequest => "project_id.request",
            RelayCounters::ServerStarting => "server.starting",
            #[cfg(feature = "processing")]
            RelayCounters::ProcessingMessageProduced => "processing.event.produced",
            #[cfg(feature = "processing")]
            RelayCounters::ProcessingProduceError => "processing.produce.error",
            RelayCounters::EventProtocol => "event.protocol",
            RelayCounters::Requests => "requests",
            RelayCounters::ResponsesStatusCodes => "responses.status_codes",
            RelayCounters::EvictingStaleProjectCaches => "project_cache.eviction",
            RelayCounters::ConnectorReused => "connector.reused",
            RelayCounters::ConnectorOpened => "connector.opened",
            RelayCounters::ConnectorClosed => "connector.closed",
            RelayCounters::ConnectorErrors => "connector.errors",
            RelayCounters::ConnectorTimeouts => "connector.timeouts",
        }
    }
}
