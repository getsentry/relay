use crate::time::{Duration, Instant};

/// Simple collection of individual measurements.
///
/// Tracks the total time from starting the measurements as well as
/// individual categorized measurements.
pub struct Measurements {
    start: Instant,
    categorized: Vec<Measurement>,
}

impl Measurements {
    /// Starts recording the first measurement.
    pub fn start() -> Self {
        Measurements {
            start: Instant::now(),
            categorized: Vec::new(),
        }
    }

    /// Adds an individual categorized measurement.
    pub fn add(&mut self, duration: Duration, category: &'static str) {
        self.categorized.push(Measurement {
            duration,
            category: Some(category),
        });
    }

    /// Finishes the current measurements and returns all individual
    /// categorized measurements.
    pub fn finish(&self) -> impl Iterator<Item = Measurement> + '_ {
        let mut duration = self.start.elapsed();
        for c in &self.categorized {
            duration = duration.saturating_sub(c.duration);
        }

        std::iter::once(Measurement {
            duration,
            category: None,
        })
        .chain(self.categorized.iter().copied())
        .filter(|m| !m.duration.is_zero())
    }
}

/// A single, optionally, categorized measurement.
#[derive(Copy, Clone)]
pub struct Measurement {
    /// Length of the measurement.
    pub duration: Duration,
    /// Optional category, if the measurement was categorized.
    pub category: Option<&'static str>,
}
