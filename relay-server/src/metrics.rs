use relay_common::metrics::{CounterMetric, HistogramMetric, SetMetric, TimerMetric};
use std::borrow::Cow;

/// Set metrics used by Relay
pub enum RelaySets {
    /// Represents the number of active projects in the current slice of time
    UniqueProjects,
}

impl SetMetric for RelaySets {
    fn name(&self) -> Cow<'_, str> {
        match self {
            RelaySets::UniqueProjects => Cow::Borrowed("unique_projects"),
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
    fn name(&self) -> Cow<'_, str> {
        match self {
            RelayHistograms::EventQueueSizePct => Cow::Borrowed("event.queue_size.pct"),
            RelayHistograms::EventQueueSize => Cow::Borrowed("event.queue_size"),
            RelayHistograms::ProjectStatePending => Cow::Borrowed("project_state.pending"),
            RelayHistograms::ProjectStateRequestBatchSize => {
                Cow::Borrowed("project_state.request.batch_size")
            }
            RelayHistograms::ProjectStateReceived => Cow::Borrowed("project_state.received"),
            RelayHistograms::ProjectStateCacheSize => Cow::Borrowed("project_cache.size"),
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
    fn name(&self) -> Cow<'_, str> {
        match self {
            RelayTimers::EventProcessingDeserialize => {
                Cow::Borrowed("event_processing.deserialize")
            }
            RelayTimers::EventProcessingProcess => Cow::Borrowed("event_processing.process"),
            RelayTimers::EventProcessingFiltering => Cow::Borrowed("event_processing.filtering"),
            RelayTimers::EventProcessingRateLimiting => {
                Cow::Borrowed("event_processing.rate_limiting")
            }
            RelayTimers::EventProcessingPii => Cow::Borrowed("event_processing.pii"),
            RelayTimers::EventProcessingSerialization => {
                Cow::Borrowed("event_processing.serialization")
            }
            RelayTimers::EventWaitTime => Cow::Borrowed("event.wait_time"),
            RelayTimers::EventProcessingTime => Cow::Borrowed("event.processing_time"),
            RelayTimers::EventTotalTime => Cow::Borrowed("event.total_time"),
            RelayTimers::ProjectStateEvictionDuration => {
                Cow::Borrowed("project_state.eviction.duration")
            }
            RelayTimers::ProjectStateRequestDuration => {
                Cow::Borrowed("project_state.request.duration")
            }
            RelayTimers::ProjectIdRequestDuration => Cow::Borrowed("project_id.request.duration"),
            RelayTimers::EventSizeBytesRaw => Cow::Borrowed("event.size_bytes.raw"),
            RelayTimers::EventSizeBytesUncompressed => {
                Cow::Borrowed("event.size_bytes.uncompressed")
            }
            RelayTimers::RequestsDuration => Cow::Borrowed("requests.duration"),
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
    EventProtocol(u16),
    Requests,
    ResponsesStatusCodes,
}

impl CounterMetric for RelayCounters {
    fn name(&self) -> Cow<'_, str> {
        match self {
            RelayCounters::EventAccepted => Cow::Borrowed("event.accepted"),
            RelayCounters::EventRejected => Cow::Borrowed("event.rejected"),
            RelayCounters::EventOutcomes => Cow::Borrowed("events.outcomes"),
            RelayCounters::ProjectStateGet => Cow::Borrowed("project_state.get"),
            RelayCounters::ProjectStateRequest => Cow::Borrowed("project_state.request"),
            RelayCounters::ProjectCacheHit => Cow::Borrowed("project_cache.hit"),
            RelayCounters::ProjectCacheMiss => Cow::Borrowed("project_cache.miss"),
            RelayCounters::ProjectIdRequest => Cow::Borrowed("project_id.request"),
            RelayCounters::ServerStarting => Cow::Borrowed("server.starting"),
            RelayCounters::ProcessingEventProduced => Cow::Borrowed("processing.event.produced"),
            RelayCounters::EventProtocol(version) => {
                Cow::Owned(format!("event.protocol.v{}", version))
            }
            RelayCounters::Requests => Cow::Borrowed("requests"),
            RelayCounters::ResponsesStatusCodes => Cow::Borrowed("responses.status_codes"),
        }
    }
}
