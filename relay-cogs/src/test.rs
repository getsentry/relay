use std::sync::{Arc, Mutex, PoisonError};

use crate::{CogsMeasurement, CogsRecorder};

/// A [`CogsRecorder`] for testing which allows access to all recorded measurements.
#[derive(Clone, Default)]
pub struct TestRecorder(Arc<Mutex<Vec<CogsMeasurement>>>);

impl TestRecorder {
    /// Returns the recorded measurements.
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
