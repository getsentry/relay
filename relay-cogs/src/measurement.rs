use crate::time::{Clock, Duration, Instant};

/// Simple collection of individual measurements.
///
/// Tracks the total time from starting the measurements as well as
/// individual categorized measurements.
pub struct Measurements<C> {
    start: Instant,
    categorized: Vec<Measurement>,
    clock: C,
}

impl<C> Measurements<C>
where
    C: Clock,
{
    /// Starts recording the first measurement.
    pub fn start(clock: C) -> Self {
        Measurements {
            start: clock.now(),
            categorized: Vec::new(),
            clock,
        }
    }

    /// Starts a categorized measurement, which can be finalized with [`Self::finish_category`].
    pub fn start_category(&self) -> CatgegoryMeasurement {
        CatgegoryMeasurement {
            start: self.clock.now(),
        }
    }

    /// Finishes an individual categorized measurement, started with [`Self::start_category`].
    pub fn finish_category(&mut self, measurement: CatgegoryMeasurement, category: &'static str) {
        self.categorized.push(Measurement {
            duration: self.clock.now().since(measurement.start),
            category: Some(category),
        });
    }

    /// Finishes the current measurements and returns all individual
    /// categorized measurements.
    pub fn finish(&self) -> impl Iterator<Item = Measurement> + '_ {
        let mut duration = self.clock.now().since(self.start);
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

/// An individual categorized measurement started with [`Measurements::start_category`].
#[derive(Clone, Copy)]
pub struct CatgegoryMeasurement {
    start: Instant,
}
