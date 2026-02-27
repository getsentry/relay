use relay_statsd::GaugeMetric;

/// Gauge metrics for Relay system components.
pub enum SystemGauges {
    /// Number of active runtime tasks.
    ///
    /// Every call to [`spawn`](`crate::spawn()`) increases this counter by one,
    /// and decrements the counter by one on termination.
    ///
    /// This metric is tagged with:
    ///  - `id`: A unique identifier for the task, derived from its location in code.
    ///  - `file`: The source filename where the task is created.
    ///  - `line`: The source line where the task is created within the file.
    RuntimeTaskCount,
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
            Self::RuntimeTaskCount => "runtime.tasks",
            Self::ServiceBackPressure => "service.back_pressure",
        }
    }
}
