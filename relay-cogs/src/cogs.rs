use core::fmt;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;
use std::usize;

use crate::{AppFeature, ResourceId};
use crate::{CogsMeasurement, CogsRecorder, Value};

/// COGS measurements collector.
///
/// The collector is cheap to clone.
#[derive(Clone)]
pub struct Cogs {
    recorder: Arc<dyn CogsRecorder>,
}

impl Cogs {
    /// Creates a new [`Cogs`] from a [`recorder`](CogsRecorder).
    pub fn new<T>(recorder: T) -> Self
    where
        T: CogsRecorder + 'static,
    {
        Self {
            recorder: Arc::new(recorder),
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
    /// weighted [`FeatureWeights`]. A single [`AppFeature`] attributes the entire measurement
    /// to the feature.
    ///
    /// # Example:
    ///
    /// ```
    /// # use relay_cogs::{AppFeature, Cogs, ResourceId};
    /// # struct Span;
    /// # fn scrub_sql(_: &mut Span) {}
    /// # fn extract_tags(_: &mut Span) {};
    ///
    /// fn process_span(cogs: &Cogs, span: &mut Span) {
    ///     let _token = cogs.timed(ResourceId::Relay, AppFeature::Spans);
    ///
    ///     scrub_sql(span);
    ///     extract_tags(span);
    /// }
    ///
    /// ```
    pub fn timed<F: Into<FeatureWeights>>(&self, resource: ResourceId, weights: F) -> Token {
        Token {
            resource,
            features: weights.into(),
            start: Instant::now(),
            recorder: Arc::clone(&self.recorder),
        }
    }
}

/// An in progress COGS measurement.
///
/// The measurement is recorded when the token is dropped.
#[must_use]
pub struct Token {
    resource: ResourceId,
    features: FeatureWeights,
    start: Instant,
    recorder: Arc<dyn CogsRecorder>,
}

impl Token {
    /// Cancels the COGS measurement.
    pub fn cancel(&mut self) {
        // No features -> nothing gets attributed.
        self.update(FeatureWeights::none());
    }

    /// Updates the app features to which the active measurement is attributed to.
    ///
    /// # Example:
    ///
    /// ```
    /// # use relay_cogs::{AppFeature, Cogs, ResourceId};
    /// # struct Item;
    /// # fn do_something(_: &Item) -> bool { true };
    ///
    /// fn process(cogs: &Cogs, item: &Item) {
    ///     let mut token = cogs.timed(ResourceId::Relay, AppFeature::Unattributed);
    ///
    ///     // App feature is only known after some computations.
    ///     if do_something(item) {
    ///         token.update(AppFeature::Spans);
    ///     } else {
    ///         token.update(AppFeature::Transactions);
    ///     }
    /// }
    /// ```
    pub fn update<T: Into<FeatureWeights>>(&mut self, features: T) {
        self.features = features.into();
    }
}

impl Drop for Token {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();

        for (feature, ratio) in self.features.weights() {
            let time = elapsed.div_f32(ratio);
            self.recorder.record(CogsMeasurement {
                resource: self.resource,
                feature,
                value: Value::Time(time),
            });
        }
    }
}

impl fmt::Debug for Token {
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
#[derive(Clone)]
pub struct FeatureWeights(BTreeMap<AppFeature, NonZeroUsize>);

impl FeatureWeights {
    /// Attributes all measurements to a single [`AppFeature`].
    pub fn new(feature: AppFeature) -> Self {
        Self::builder().weight(feature, 1).build()
    }

    /// Attributes the measurement to nothing.
    pub fn none() -> Self {
        Self::builder().build()
    }

    /// Returns an [`FeatureWeights`] builder.
    pub fn builder() -> FeatureWeightsBuilder {
        FeatureWeightsBuilder(Self(Default::default()))
    }

    /// Merges two instances of [`FeatureWeights`] and sums the contained weights.
    pub fn merge(mut self, other: Self) -> Self {
        for (feature, weight) in other.0.into_iter() {
            if let Some(w) = self.0.get_mut(&feature) {
                *w = w.saturating_add(weight.get());
            } else {
                self.0.insert(feature, weight);
            }
        }

        self
    }

    /// Returns an iterator yielding an app feature and it's associated weight.
    ///
    /// Weights are normalized to the total stored weights in the range between `0.0` and `1.0`.
    /// Used to divide a measurement by the stored weights.
    ///
    /// # Examples
    ///
    /// ```
    /// use relay_cogs::{AppFeature, FeatureWeights};
    /// use std::collections::HashMap;
    ///
    /// let app_features = FeatureWeights::builder()
    ///     .weight(AppFeature::Transactions, 1)
    ///     .weight(AppFeature::Spans, 1)
    ///     .build();
    ///
    /// let weights: HashMap<AppFeature, f32> = app_features.weights().collect();
    /// assert_eq!(weights, HashMap::from([(AppFeature::Transactions, 0.5), (AppFeature::Spans, 0.5)]))
    /// ```
    pub fn weights(&self) -> impl Iterator<Item = (AppFeature, f32)> + '_ {
        let total_weight: usize = self.0.values().map(|weight| weight.get()).sum();

        self.0.iter().filter_map(move |(feature, weight)| {
            if total_weight == 0 {
                return None;
            }

            let ratio = (weight.get() as f32 / total_weight as f32).clamp(0.0, 1.0);
            Some((*feature, ratio))
        })
    }
}

impl fmt::Debug for FeatureWeights {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FeatureWeights(")?;

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

impl From<AppFeature> for FeatureWeights {
    fn from(value: AppFeature) -> Self {
        Self::new(value)
    }
}

/// A builder for [`FeatureWeights`] which can be used to configure different weights per [`AppFeature`].
pub struct FeatureWeightsBuilder(FeatureWeights);

impl FeatureWeightsBuilder {
    /// Increases the `weight` of an [`AppFeature`].
    pub fn add_weight(&mut self, feature: AppFeature, weight: usize) -> &mut Self {
        let Some(weight) = NonZeroUsize::new(weight) else {
            return self;
        };

        if let Some(previous) = self.0 .0.get_mut(&feature) {
            *previous = previous.saturating_add(weight.get());
        } else {
            self.0 .0.insert(feature, weight);
        }

        self
    }

    /// Sets the specified `weight` for an [`AppFeature`].
    pub fn weight(&mut self, feature: AppFeature, weight: usize) -> &mut Self {
        if let Some(weight) = NonZeroUsize::new(weight) {
            self.0 .0.insert(feature, weight);
        } else {
            self.0 .0.remove(&feature);
        }
        self
    }

    /// Builds and returns the [`FeatureWeights`].
    pub fn build(&mut self) -> FeatureWeights {
        std::mem::replace(self, FeatureWeights::builder()).0
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use super::*;
    use crate::test::TestRecorder;

    #[test]
    fn test_cogs_simple() {
        let recorder = TestRecorder::default();
        let cogs = Cogs::new(recorder.clone());

        drop(cogs.timed(ResourceId::Relay, AppFeature::Spans));

        let measurements = recorder.measurements();
        assert_eq!(measurements.len(), 1);
        assert_eq!(measurements[0].resource, ResourceId::Relay);
        assert_eq!(measurements[0].feature, AppFeature::Spans);
    }

    #[test]
    fn test_cogs_multiple_weights() {
        let recorder = TestRecorder::default();
        let cogs = Cogs::new(recorder.clone());

        let f = FeatureWeights::builder()
            .weight(AppFeature::Spans, 1)
            .weight(AppFeature::Transactions, 1)
            .weight(AppFeature::MetricsSpans, 0) // Noop
            .add_weight(AppFeature::MetricsSpans, 1)
            .weight(AppFeature::Transactions, 0) // Reset
            .build();
        {
            let _token = cogs.timed(ResourceId::Relay, f);
            std::thread::sleep(Duration::from_millis(50));
        }

        let measurements = recorder.measurements();
        assert_eq!(measurements.len(), 2);
        assert_eq!(measurements[0].resource, ResourceId::Relay);
        assert_eq!(measurements[0].feature, AppFeature::Spans);
        assert_eq!(measurements[1].resource, ResourceId::Relay);
        assert_eq!(measurements[1].feature, AppFeature::MetricsSpans);
        assert_eq!(measurements[0].value, measurements[1].value);
    }

    #[test]
    fn test_app_features_none() {
        let a = FeatureWeights::none();
        assert_eq!(a.weights().count(), 0);
    }

    #[test]
    fn test_app_features_new() {
        let a = FeatureWeights::new(AppFeature::Spans);
        assert_eq!(
            a.weights().collect::<Vec<_>>(),
            vec![(AppFeature::Spans, 1.0)]
        );
    }

    #[test]
    fn test_app_features_merge() {
        let a = FeatureWeights::builder()
            .weight(AppFeature::Spans, 1)
            .weight(AppFeature::Transactions, 2)
            .build();

        let b = FeatureWeights::builder()
            .weight(AppFeature::Spans, 2)
            .weight(AppFeature::Unattributed, 5)
            .build();

        let c = FeatureWeights::merge(FeatureWeights::none(), FeatureWeights::merge(a, b));

        let weights: HashMap<_, _> = c.weights().collect();
        assert_eq!(
            weights,
            HashMap::from([
                (AppFeature::Spans, 0.3),
                (AppFeature::Transactions, 0.2),
                (AppFeature::Unattributed, 0.5),
            ])
        )
    }
}
