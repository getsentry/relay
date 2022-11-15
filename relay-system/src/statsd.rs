use relay_statsd::GaugeMetric;

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
