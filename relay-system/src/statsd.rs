use relay_statsd::{CounterMetric, GaugeMetric};

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
            SystemGauges::ServiceBackPressure => "service.back_pressure",
        }
    }
}

/// Counter metrics for Relay system components.
pub enum SystemCounters {
    /// The amount of time a service spends waiting for new messages.
    ///
    /// This is an indicator of how much more load a service can take on.
    ///
    /// Caveat: Some services circumvent the service framework by using custom incoming queues.
    /// For these, we cannot fully rely on this metric.
    ///
    /// This metric is tagged with:
    ///  - `service`: The fully qualified type name of the service implementation.
    ServiceIdleTime,
}

impl CounterMetric for SystemCounters {
    fn name(&self) -> &'static str {
        match self {
            Self::ServiceIdleTime => "service.idle_time_nanos",
        }
    }
}
