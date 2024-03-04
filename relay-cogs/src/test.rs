use std::sync::{Arc, Mutex, PoisonError};

use crate::{CogsMeasurement, CogsRecorder};

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
