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

/// Histogram metrics used by Relay
pub enum RelayHistograms {
    /// The number of events in the queue as a percentage of the maximum number of events
    /// that can be stored in the queue ( 0 ... the queue is empty, 1 ... the queue is full
    /// and no additional events can be added).
    EventQueueSizePct,
    /// The number of events in the queue at the sampling moment.
    EventQueueSize,

    ProjectStatePending,
    ProjectStateRequestBatchSize,
    ProjectStateReceived,
    ProjectStateCacheSize,
}

impl HistogramMetric for RelayHistograms {
    fn name(&self) -> &'static str {
        match self {
            RelayHistograms::EventQueueSizePct => "event.queue_size.pct",
            RelayHistograms::EventQueueSize => "event.queue_size",
            RelayHistograms::ProjectStatePending => "project_state.pending",
            RelayHistograms::ProjectStateRequestBatchSize => "project_state.request.batch_size",
            RelayHistograms::ProjectStateReceived => "project_state.received",
            RelayHistograms::ProjectStateCacheSize => "project_cache.size",
        }
    }
}

/// Timer metrics used by Relay
pub enum RelayTimers {
    EventProcessingDeserialize,
    EventProcessingProcess,
    EventProcessingFiltering,
    EventProcessingRateLimiting,
    EventProcessingPii,
    EventProcessingSerialization,
    EventWaitTime,
    EventProcessingTime,
    EventTotalTime,
    ProjectStateEvictionDuration,
    ProjectStateRequestDuration,
    ProjectIdRequestDuration,
    EventSizeBytesRaw,
    EventSizeBytesUncompressed,
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
            RelayTimers::EventSizeBytesRaw => "event.size_bytes.raw",
            RelayTimers::EventSizeBytesUncompressed => "event.size_bytes.uncompressed",
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
