use std::sync::{Arc, Mutex, PoisonError};

use metrics::{Counter, Gauge, Histogram, KeyName, SharedString, Unit};

#[derive(Debug, Default)]
pub struct MockRecorder {
    inner: Arc<Mutex<Vec<String>>>,
}

impl MockRecorder {
    pub fn consume(self) -> Vec<String> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.clone()
    }
}

impl metrics::Recorder for MockRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(MockFn {
            inner: Arc::clone(&self.inner),
            key: key.clone(),
        }))
    }

    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(MockFn {
            inner: Arc::clone(&self.inner),
            key: key.clone(),
        }))
    }

    fn register_histogram(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(MockFn {
            inner: Arc::clone(&self.inner),
            key: key.clone(),
        }))
    }
}

struct MockFn {
    inner: Arc<Mutex<Vec<String>>>,
    key: metrics::Key,
}

impl metrics::CounterFn for MockFn {
    fn increment(&self, value: u64) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.push(format_metric("c", &self.key, value));
    }

    fn absolute(&self, value: u64) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.push(format_metric("c", &self.key, format!("={value}")));
    }
}

impl metrics::GaugeFn for MockFn {
    fn increment(&self, value: f64) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.push(format_metric("g", &self.key, format!("+{value}")));
    }

    fn decrement(&self, value: f64) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.push(format_metric("g", &self.key, format!("-{value}")));
    }

    fn set(&self, value: f64) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.push(format_metric("g", &self.key, value));
    }
}

impl metrics::HistogramFn for MockFn {
    fn record(&self, value: f64) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.push(format_metric("d", &self.key, value));
    }
}

fn format_metric(t: &str, key: &metrics::Key, value: impl std::fmt::Display) -> String {
    let name = key.name();
    let tags = key
        .labels()
        .map(|label| {
            let key = label.key();
            let value = label.value();
            format!("{key}:{value}")
        })
        .collect::<Vec<_>>()
        .join(",");

    format!("{name}:{value}|{t}|#{tags}")
}
