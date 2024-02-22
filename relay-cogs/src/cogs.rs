use core::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;
use std::usize;

use crate::{utils::EnumMap, AppFeature, ResourceId};
use crate::{CogsMeasurement, CogsRecorder, Value};

/// COGS measurements collector.
///
/// The collector is cheap to clone.
#[derive(Clone)]
pub struct Cogs {
    consumer: Arc<dyn CogsRecorder>,
}

impl Cogs {
    /// Creates a new [`Cogs`] from a [`recorder`](CogsRecorder).
    pub fn new<T>(recorder: T) -> Self
    where
        T: CogsRecorder + 'static,
    {
        Self {
            consumer: Arc::new(recorder),
        }
    }

    /// Shortcut for creating a [`Cogs`] from a [`crate::NoopRecorder`].
    ///
    /// All collected measurements will be dropped.
    pub fn noop() -> Self {
        Self::new(crate::NoopRecorder)
    }
}

impl Cogs {
    /// Starts a recording for a COGS measurement.
    ///
    /// When the returned token is dropped the measurement will be recorded
    /// with the configured [recorder](CogsRecorder).
    ///
    /// The recorded measurement can be attributed to multiple features by supplying a
    /// weighted [`AppFeatures`]. A single [`AppFeature`] attributes the entire measurement
    /// to the feature.
    pub fn record<F: Into<AppFeatures>>(&self, resource: ResourceId, features: F) -> CogsToken {
        CogsToken {
            resource,
            features: features.into(),
            start: Instant::now(),
            consumer: Arc::clone(&self.consumer),
        }
    }
}

/// An in progress COGS measurement.
///
/// The measurement is recorded when the token is dropped.
#[must_use]
pub struct CogsToken {
    resource: ResourceId,
    features: AppFeatures,
    start: Instant,
    consumer: Arc<dyn CogsRecorder>,
}

impl CogsToken {
    /// Cancels the COGS measurement.
    pub fn cancel(&mut self) {
        // No features -> nothing gets attributed.
        self.update(AppFeatures::none());
    }

    /// Updates the app features to whicht the active measurement is attributed to.
    pub fn update<T: Into<AppFeatures>>(&mut self, features: T) {
        self.features = features.into();
    }
}

impl Drop for CogsToken {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();

        for (feature, ratio) in self.features.weights() {
            let time = elapsed.div_f32(ratio);
            self.consumer.record(CogsMeasurement {
                resource: self.resource,
                feature,
                value: Value::Time(time),
            });
        }
    }
}

impl fmt::Debug for CogsToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CogsToken")
            .field("resource", &self.resource)
            .field("features", &self.features)
            .finish()
    }
}

/// A collection of weighted [app features](AppFeature).
///
/// Used to attribute a single COGS measurement to multiple features.
#[derive(Clone, Copy)]
pub struct AppFeatures(EnumMap<16, AppFeature, NonZeroUsize>);

impl AppFeatures {
    /// Attributes all measurements to a single [`AppFeature`].
    pub fn new(feature: AppFeature) -> Self {
        Self::builder().weight(feature, 1).build()
    }

    /// Attributes the measurement to nothing.
    pub fn none() -> Self {
        Self::builder().build()
    }

    /// Returns an [`AppFeatures`] builder.
    pub fn builder() -> AppFeaturesBuilder {
        AppFeaturesBuilder(Self(Default::default()))
    }

    /// Merges two instances of [`AppFeatures`] and sums the contained weights.
    pub fn merge(mut self, other: Self) -> Self {
        for (feature, weight) in other.0.into_iter() {
            if let Some(w) = self.0.get_mut(feature) {
                *w = w.saturating_add(weight.get());
            } else {
                self.0.insert(feature, weight);
            }
        }

        self
    }

    /// Returns an iterator yielding an app feature and it's associated weight
    /// normalized to the total stored weights in the range between `0.0` and `1.0`.
    ///
    /// Used to divide a measurement by the stored weights.
    pub fn weights(&self) -> impl Iterator<Item = (AppFeature, f32)> {
        let total_weight: usize = self.0.into_iter().map(|(_, weight)| weight.get()).sum();

        self.0.into_iter().filter_map(move |(feature, weight)| {
            if total_weight == 0 {
                return None;
            }

            let ratio = (weight.get() as f32 / total_weight as f32).clamp(0.0, 1.0);
            Some((feature, ratio))
        })
    }
}

impl fmt::Debug for AppFeatures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AppFeatures(")?;

        let mut first = true;
        for (feature, weight) in self.weights() {
            if !first {
                first = false;
                write!(f, ", ")?;
            }
            write!(f, "{feature:?}={weight:.2}")?;
        }
        write!(f, ")")
    }
}

impl From<AppFeature> for AppFeatures {
    fn from(value: AppFeature) -> Self {
        Self::new(value)
    }
}

/// A builder for [`AppFeatures`] which can be used to configure different weights per [`AppFeature`].
pub struct AppFeaturesBuilder(AppFeatures);

impl AppFeaturesBuilder {
    /// Increases the `weight` of an [`AppFeature`].
    pub fn add_weight(mut self, feature: AppFeature, weight: usize) -> Self {
        let Some(weight) = NonZeroUsize::new(weight) else {
            return self;
        };

        if let Some(previous) = self.0 .0.get_mut(feature) {
            *previous = previous.saturating_add(weight.get());
        } else {
            self.0 .0.insert(feature, weight);
        }

        self
    }

    /// Sets the specified `weight` for an [`AppFeature`].
    pub fn weight(mut self, feature: AppFeature, weight: usize) -> Self {
        if let Some(weight) = NonZeroUsize::new(weight) {
            self.0 .0.insert(feature, weight);
        } else {
            self.0 .0.remove(feature);
        }
        self
    }

    /// Builds and returns the [`AppFeatures`].
    pub fn build(self) -> AppFeatures {
        self.0
    }
}
