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

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex, PoisonError};

    use super::*;

    #[derive(Clone, Default)]
    pub struct TestRecorder(Arc<Mutex<Vec<CogsMeasurement>>>);

    impl TestRecorder {
        pub fn measurements(&self) -> Vec<CogsMeasurement> {
            let inner = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            inner.clone()
        }
    }

    impl CogsRecorder for TestRecorder {
        fn record(&self, measurement: CogsMeasurement) {
            let mut inner = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            inner.push(measurement);
        }
    }
}

#[cfg(test)]
pub use test::*;
