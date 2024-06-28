#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Array, Empty, FromValue, IntoValue, Object};
use std::collections::BTreeMap;

use crate::processor::ProcessValue;

pub type MetricSummaryMapping = Object<Array<MetricSummary>>;

/// A collection of [`MetricSummary`] items keyed by the metric.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct MetricsSummary(pub MetricSummaryMapping);

impl MetricsSummary {
    /// Returns an empty [`MetricsSummary`].
    pub fn empty() -> MetricsSummary {
        MetricsSummary(BTreeMap::new())
    }

    /// Combinator to modify the contained metric summaries.
    pub fn update_value<F: FnOnce(MetricSummaryMapping) -> MetricSummaryMapping>(&mut self, f: F) {
        self.0 = f(std::mem::take(&mut self.0));
    }

    /// Merges a [`MetricsSummary`] into itself.
    ///
    /// The merge operation concatenates tags of the same metric even if they are identical for the
    /// sake of simplicity. This doesn't cause issues for us, under the assumption that the two
    /// summaries do not have overlapping metric names (which is the case for us since summaries
    /// computed by the SDKs have metric names that are different from the ones we generate in
    /// Relay).
    ///
    /// Do not use this function to merge any arbitrary [`MetricsSummary`]s.
    pub fn merge(&mut self, metrics_summary: MetricsSummary) {
        for (metric_name, metrics) in metrics_summary.0 {
            let original_metrics = self.0.entry(metric_name).or_insert(Annotated::new(vec![]));
            match (original_metrics.value_mut(), metrics.0) {
                (Some(original_metrics), Some(metrics)) => {
                    original_metrics.extend(metrics);
                }
                (None, Some(metrics)) => {
                    original_metrics.set_value(Some(metrics));
                }
                _ => {}
            }
        }
    }
}

/// The metric summary of a single metric that is emitted by the SDK.
///
/// The summary contains specific aggregate values that the metric had during the span's lifetime. A single span can
/// have the same metric emitted multiple times, which is the reason for aggregates being computed in each summary.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct MetricSummary {
    /// Minimum value of the metric.
    pub min: Annotated<f64>,

    /// Maximum value of the metric.
    pub max: Annotated<f64>,

    /// Sum of all metric values.
    pub sum: Annotated<f64>,

    /// Count of all metric values.
    pub count: Annotated<u64>,

    /// Tags of the metric.
    pub tags: Annotated<Object<String>>,
}
