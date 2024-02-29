use crate::CogsMeasurement;

/// Cogs consumer, recording actual measurements.
pub trait CogsRecorder: Send + Sync {
    /// Record a single COGS measurement.
    fn record(&self, measurement: CogsMeasurement);
}

#[derive(Debug)]
pub struct NoopRecorder;

impl CogsRecorder for NoopRecorder {
    fn record(&self, _: CogsMeasurement) {}
}
