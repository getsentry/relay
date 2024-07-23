use relay_statsd::{CounterMetric, GaugeMetric, TimerMetric};

/// Counter metrics for Relay system components.
pub enum SystemCounters {
    /// Number of runtime tasks created/spawned.
    ///
    /// Every call to [`spawn`](`crate::spawn`) increases this counter by one.
    ///
    /// This metric is tagged with:
    ///  - `id`: A unique identifier for the task, derived from its location in code.
    RuntimeTasksCreated,
}

impl CounterMetric for SystemCounters {
    fn name(&self) -> &'static str {
        match self {
            Self::RuntimeTasksCreated => "runtime.task.spawn.created",
        }
    }
}

/// Timer metrics for Relay system components.
pub enum SystemTimers {
    /// Duration how long a runtime task was alive for.
    ///
    /// This metric is emitted once for each task passed to [`spawn`](crate::spawn)
    /// with the time it took for the passed task to terminate.
    ///
    /// This metric is tagged with:
    ///  - `id`: A unique identifier for the task, derived from its location in code.
    RuntimeTasksFinished,
}

impl TimerMetric for SystemTimers {
    fn name(&self) -> &'static str {
        match self {
            Self::RuntimeTasksFinished => "runtime.task.spawn.finished",
        }
    }
}

/// Gauge metrics for Relay system components.
pub enum SystemGauges {
    /// A number of messages queued in a services inbound message channel.
    ///
    /// This metric is emitted once per second for every running service. Without backlogs, this
    /// number should be close to `0`. If this number is monotonically increasing, the service is
    /// not able to process the inbound message volume.
    ///
    /// This metric is tagged with:
    ///  - `service`: The fully qualified type name of the service implementation.
    ServiceBackPressure,
}

impl GaugeMetric for SystemGauges {
    fn name(&self) -> &'static str {
        match *self {
            Self::ServiceBackPressure => "service.back_pressure",
        }
    }
}
