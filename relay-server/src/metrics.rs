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
    /// The number of events in the queue at the sampling moment.
    EventQueueSize,
    /// The event size as seen by Relay after it is extracted from a request
    EventSizeBytesRaw,
    /// The event size as seen by Relay after it is extracted Base64 decoded and unzipped.
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
    /// The time spent deserializing an event from a JSON byte array into a [relay_general::protocol::Event].
    EventProcessingDeserialize,
    /// Time spent running event processors on an event.
    /// Event processing happens before filtering.
    EventProcessingProcess,
    /// Time spent running filtering on an event.
    EventProcessingFiltering,
    /// Time spent checking for rate limits in Redis.
    /// Note that not all events are checked against Redis. After an event is rate limited
    /// for period A, any event using the same key coming during period A will be automatically
    /// rate limited without checking against Redis.
    EventProcessingRateLimiting,
    /// Time spent in PII for the current event.
    EventProcessingPii,
    /// Time spent converting the event from an Annotated<Event> into a String containing the JSON
    /// representation of the event.
    EventProcessingSerialization,
    /// Represents the time spent between receiving the event in Relay (i.e. beginning of the
    /// request handling) up to the time before starting synchronous processing in the EventProcessor.
    /// This is effectively the time between the event arriving in Relay and the event being ready
    /// to be processed.
    EventWaitTime,
    /// This is the time the event spends in the EventProcessor (i.e. the sync processing of the
    /// event).
    /// It includes decoding the event envelope and the times spent in EventProcessingProcess,
    /// EventProcessingFiltering, EventProcessingRateLimiting, EventProcessingPii and
    /// EventProcessingSerialization.
    EventProcessingTime,
    /// The total time an event spends in Relay from the time it is received until it finishes
    /// processing.
    EventTotalTime,

    ProjectStateEvictionDuration,
    ProjectStateRequestDuration,
    ProjectIdRequestDuration,
    RequestsDuration,
}

impl TimerMetric for RelayTimers {
    fn name(&self) -> &'static str {
        match self {
            RelayTimers::EventProcessingDeserialize => "event_processing.deserialize",
            RelayTimers::EventProcessingProcess => "event_processing.process",
            RelayTimers::EventProcessingFiltering => "event_processing.filtering",
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
    EventAccepted,
    EventRejected,
    EventOutcomes,
    ProjectStateGet,
    ProjectStateRequest,
    ProjectCacheHit,
    ProjectCacheMiss,
    ProjectIdRequest,
    ServerStarting,
    ProcessingEventProduced,
    EventProtocol,
    Requests,
    ResponsesStatusCodes,
}

impl CounterMetric for RelayCounters {
    fn name(&self) -> &'static str {
        match self {
            RelayCounters::EventAccepted => "event.accepted",
            RelayCounters::EventRejected => "event.rejected",
            RelayCounters::EventOutcomes => "events.outcomes",
            RelayCounters::ProjectStateGet => "project_state.get",
            RelayCounters::ProjectStateRequest => "project_state.request",
            RelayCounters::ProjectCacheHit => "project_cache.hit",
            RelayCounters::ProjectCacheMiss => "project_cache.miss",
            RelayCounters::ProjectIdRequest => "project_id.request",
            RelayCounters::ServerStarting => "server.starting",
            RelayCounters::ProcessingEventProduced => "processing.event.produced",
            RelayCounters::EventProtocol => "event.protocol",
            RelayCounters::Requests => "requests",
            RelayCounters::ResponsesStatusCodes => "responses.status_codes",
        }
    }
}
