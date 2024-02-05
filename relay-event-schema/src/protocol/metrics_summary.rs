#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Array, Empty, FromValue, IntoValue, Object};

use crate::processor::ProcessValue;

pub type MetricSummaryMapping = Object<Array<MetricSummary>>;

/// A collection of [`MetricSummary`] items keyed by the metric.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct MetricsSummary(pub MetricSummaryMapping);

impl MetricsSummary {
    /// Combinator to modify the contained metric summaries.
    pub fn update_value<F: FnOnce(MetricSummaryMapping) -> MetricSummaryMapping>(&mut self, f: F) {
        self.0 = f(std::mem::take(&mut self.0));
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
