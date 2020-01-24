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
    /// The number of events in the queue as a percentage of the maximum number of events
    /// that can be stored in the queue ( 0 ... the queue is empty, 1 ... the queue is full
    /// and no additional events can be added).
    EventQueueSizePct,
    /// The number of events in the queue. The event queue represents the events that are being
    /// processed at a particular time in Relay. Once a request is received the event has
    /// some preliminary (quick) processing to determine if it can be processed or it is
    /// rejected. Once this determination has been done the http request that
    /// created the event terminates and, if the request is to be further processed,
    /// the event enters a queue ( a virtual queue, the event is kept in a future that
    /// will resolve at some point in time).
    /// Once the event finishes processing and is sent downstream (i.e. the future is
    /// resolved and the event leaves relay) the event is considered handled and it
    /// leaves the queue ( the queue size is decremented).
    EventQueueSize,
    /// The event size as seen by Relay after it is extracted from a request.
    EventSizeBytesRaw,
    /// The event size as seen by Relay after it has been decompressed and decoded (e.g. from Base64).
    EventSizeBytesUncompressed,
    /// Number of projects in the ProjectCache that are waiting for their state to be updated.
    ProjectStatePending,
    /// Number of project state requested from the Upstream for the current batch request.
    ProjectStateRequestBatchSize,
    /// Number of project states received from the Upstream for the current batch request.
    ProjectStateReceived,
    /// Number of project states currently held in the ProjectState cache.
    ProjectStateCacheSize,
}

impl HistogramMetric for RelayHistograms {
    fn name(&self) -> &'static str {
        match self {
            RelayHistograms::EventQueueSizePct => "event.queue_size.pct",
            RelayHistograms::EventQueueSize => "event.queue_size",
            RelayHistograms::EventSizeBytesRaw => "event.size_bytes.raw",
            RelayHistograms::EventSizeBytesUncompressed => "event.size_bytes.uncompressed",
            RelayHistograms::ProjectStatePending => "project_state.pending",
            RelayHistograms::ProjectStateRequestBatchSize => "project_state.request.batch_size",
            RelayHistograms::ProjectStateReceived => "project_state.received",
            RelayHistograms::ProjectStateCacheSize => "project_cache.size",
        }
    }
}

/// Timer metrics used by Relay
pub enum RelayTimers {
    /// The time spent deserializing an event from a JSON byte array into the native data structure
    /// on which Relay operates.
    EventProcessingDeserialize,
    /// Time spent running event processors on an event.
    /// Event processing happens before filtering.
    #[cfg(feature = "processing")]
    EventProcessingProcess,
    /// Time spent running filtering on an event.
    #[cfg(feature = "processing")]
    EventProcessingFiltering,
    /// Time spent checking for rate limits in Redis.
    /// Note that not all events are checked against Redis. After an event is rate limited
    /// for period A, any event using the same key coming during period A will be automatically
    /// rate limited without checking against Redis (the event will be simply discarded without
    /// being placed in the processing queue).
    #[cfg(feature = "processing")]
    EventProcessingRateLimiting,
    /// Time spent in data scrubbing for the current event.
    EventProcessingPii,
    /// Time spent converting the event from an Annotated<Event> into a String containing the JSON
    /// representation of the event.
    EventProcessingSerialization,
    /// Represents the time spent between receiving the event in Relay (i.e. beginning of the
    /// request handling) up to the time before starting synchronous processing in the EventProcessor.
    EventWaitTime,
    /// This is the time the event spends in the EventProcessor (i.e. the sync processing of the
    /// event).
    /// The time spent in synchronous event processing.
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
    EventProcessingTime,
    /// The total time an event spends in Relay from the time it is received until it finishes
    /// processing.
    EventTotalTime,
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
            RelayTimers::EventWaitTime => "event.wait_time",
            RelayTimers::EventProcessingTime => "event.processing_time",
            RelayTimers::EventTotalTime => "event.total_time",
            RelayTimers::ProjectStateEvictionDuration => "project_state.eviction.duration",
            RelayTimers::ProjectStateRequestDuration => "project_state.request.duration",
            RelayTimers::ProjectIdRequestDuration => "project_id.request.duration",
            RelayTimers::RequestsDuration => "requests.duration",
        }
    }
}

/// Counter metrics used by Relay
pub enum RelayCounters {
    /// Number of events accepted in the current time slot. This represents events that
    /// have successfully passed rate limits, filters and have been successfully handled.
    EventAccepted,
    /// Number of events rejected in the current time slot. This includes events being rejected
    /// because they are malformed or any other error during processing (including filtered
    /// events, discarded events and rate limited events).
    EventRejected,
    /// Represents a group of counters, implemented with using tags. The following tags are
    /// present for each event outcome:
    ///
    /// - `outcome` which is an `EventOutcome` enumeration
    /// - `reason` which is the reason string for all outcomes that are not `Accepted`.
    #[cfg(feature = "processing")]
    EventOutcomes,
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
    /// Counts the number of messages placed on the Kafka queue. When Relay operates with processing
    /// enabled and a message is successfully processed each message will generate an event on the
    /// Kafka queue and zero or more attachments. The counter has an  `event_type` tag which is set to
    /// either `event` or `attachment` representing the type of message produced on the Kafka queue.
    #[cfg(feature = "processing")]
    ProcessingEventProduced,
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
}

impl CounterMetric for RelayCounters {
    fn name(&self) -> &'static str {
        match self {
            RelayCounters::EventAccepted => "event.accepted",
            RelayCounters::EventRejected => "event.rejected",
            #[cfg(feature = "processing")]
            RelayCounters::EventOutcomes => "events.outcomes",
            RelayCounters::ProjectStateGet => "project_state.get",
            RelayCounters::ProjectStateRequest => "project_state.request",
            RelayCounters::ProjectCacheHit => "project_cache.hit",
            RelayCounters::ProjectCacheMiss => "project_cache.miss",
            RelayCounters::ProjectIdRequest => "project_id.request",
            RelayCounters::ServerStarting => "server.starting",
            #[cfg(feature = "processing")]
            RelayCounters::ProcessingEventProduced => "processing.event.produced",
            RelayCounters::EventProtocol => "event.protocol",
            RelayCounters::Requests => "requests",
            RelayCounters::ResponsesStatusCodes => "responses.status_codes",
        }
    }
}
