use crate::CogsMeasurement;

/// Cogs recorder, recording actual measurements.
pub trait CogsRecorder: Send + Sync {
    /// Record a single COGS measurement.
    fn record(&self, measurement: CogsMeasurement);
}

/// A recorder which discards all measurements.
#[derive(Debug)]
pub struct NoopRecorder;

impl CogsRecorder for NoopRecorder {
    fn record(&self, _: CogsMeasurement) {}
}
