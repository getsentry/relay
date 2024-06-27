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
    /// The merge operation purposefully concatenates the metrics of the same name even if they have the
    /// same tags in order to make the implementation simpler. This is not a big deal since ClickHouse
    /// will correctly merge data on its own, so it's mostly going to cause bigger payloads.
    ///
    /// We do not expect this merge to happen very frequently because SDKs should stop sending summaries.
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
