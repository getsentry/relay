use relay_protocol::{
    Annotated, Empty, FromValue, Getter, IntoValue, Object, SkipSerialization, Value,
};
use std::fmt::{self, Display};

#[allow(unused_imports)]
use relay_base_schema::metrics::{DurationUnit, FractionUnit, InformationUnit, MetricUnit};
use serde::{Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::{Attributes, SpanId, Timestamp, TraceId};

/// Strict version of [`MetricUnit`] that does not allow custom units.
///
/// This enum mirrors the structure of [`MetricUnit`] but excludes the `Custom` variant.
/// When converting from [`MetricUnit`], any custom units are mapped to [`StrictMetricUnit::None`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Default)]
pub enum StrictMetricUnit {
    /// A time duration, defaulting to `"millisecond"`.
    Duration(DurationUnit),
    /// Size of information derived from bytes, defaulting to `"byte"`.
    Information(InformationUnit),
    /// Fractions such as percentages, defaulting to `"ratio"`.
    Fraction(FractionUnit),
    /// Untyped value without a unit (`""`).
    #[default]
    None,
}

impl StrictMetricUnit {
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    pub fn as_str(&self) -> &str {
        match self {
            StrictMetricUnit::Duration(u) => u.as_str(),
            StrictMetricUnit::Information(u) => u.as_str(),
            StrictMetricUnit::Fraction(u) => u.as_str(),
            StrictMetricUnit::None => "none",
        }
    }
}

impl From<MetricUnit> for StrictMetricUnit {
    fn from(unit: MetricUnit) -> Self {
        match unit {
            MetricUnit::Duration(u) => StrictMetricUnit::Duration(u),
            MetricUnit::Information(u) => StrictMetricUnit::Information(u),
            MetricUnit::Fraction(u) => StrictMetricUnit::Fraction(u),
            MetricUnit::Custom(_) => StrictMetricUnit::None,
            MetricUnit::None => StrictMetricUnit::None,
        }
    }
}

impl Display for StrictMetricUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StrictMetricUnit::Duration(u) => u.fmt(f),
            StrictMetricUnit::Information(u) => u.fmt(f),
            StrictMetricUnit::Fraction(u) => u.fmt(f),
            StrictMetricUnit::None => f.write_str("none"),
        }
    }
}

impl Empty for StrictMetricUnit {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

impl FromValue for StrictMetricUnit {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match MetricUnit::from_value(value) {
            Annotated(Some(unit), meta) => Annotated(Some(unit.into()), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for StrictMetricUnit {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(self.as_str(), s)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[metastructure(process_func = "process_trace_metric", value_type = "TraceMetric")]
pub struct TraceMetric {
    /// Timestamp when the metric was created.
    #[metastructure(required = true)]
    pub timestamp: Annotated<Timestamp>,

    /// The ID of the trace the metric belongs to.
    #[metastructure(required = true, trim = false)]
    pub trace_id: Annotated<TraceId>,

    /// The Span this metric belongs to.
    #[metastructure(required = false, trim = false)]
    pub span_id: Annotated<SpanId>,

    /// The metric name.
    #[metastructure(required = true, trim = false)]
    pub name: Annotated<String>,

    /// The metric type.
    #[metastructure(required = true, field = "type")]
    pub ty: Annotated<MetricType>,

    /// The metric unit.
    #[metastructure(required = false)]
    pub unit: Annotated<StrictMetricUnit>,

    /// The metric value.
    ///
    /// Should be constrained to a number.
    #[metastructure(pii = "maybe", required = true, trim = false)]
    pub value: Annotated<Value>,

    /// Arbitrary attributes on a metric.
    #[metastructure(pii = "maybe", trim = false)]
    pub attributes: Annotated<Attributes>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = false)]
    pub other: Object<Value>,
}

impl Getter for TraceMetric {
    fn get_value(&self, path: &str) -> Option<relay_protocol::Val<'_>> {
        Some(match path.strip_prefix("trace_metric.")? {
            "name" => self.name.as_str()?.into(),
            path => {
                if let Some(key) = path.strip_prefix("attributes.") {
                    let key = key.strip_suffix(".value")?;
                    self.attributes.value()?.get_value(key)?.into()
                } else {
                    return None;
                }
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetricType {
    /// A gauge metric represents a single numerical value that can arbitrarily go up and down.
    Gauge,
    /// A distribution metric represents a collection of values that can be aggregated.
    Distribution,
    /// A counter metric represents a single numerical value.
    Counter,
    /// Unknown type, for forward compatibility.
    Unknown(String),
}

impl MetricType {
    fn as_str(&self) -> &str {
        match self {
            MetricType::Gauge => "gauge",
            MetricType::Distribution => "distribution",
            MetricType::Counter => "counter",
            MetricType::Unknown(s) => s.as_str(),
        }
    }
}

impl Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for MetricType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "gauge" => MetricType::Gauge,
            "distribution" => MetricType::Distribution,
            "counter" => MetricType::Counter,
            _ => MetricType::Unknown(value),
        }
    }
}

impl FromValue for MetricType {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), meta) => Annotated(Some(value.into()), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for MetricType {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(self.as_str(), s)
    }
}

impl ProcessValue for MetricType {}

impl Empty for MetricType {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::SerializableAnnotated;

    #[test]
    fn test_trace_metric_serialization() {
        let json = r#"{
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "name": "http.request.duration",
            "type": "distribution",
            "value": 123.45,
            "attributes": {
                "http.method": {
                    "value": "GET",
                    "type": "string"
                },
                "http.status_code": {
                    "value": "200",
                    "type": "integer"
                }
            }
        }"#;

        let data = Annotated::<TraceMetric>::from_json(json).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "name": "http.request.duration",
          "type": "distribution",
          "value": 123.45,
          "attributes": {
            "http.method": {
              "type": "string",
              "value": "GET"
            },
            "http.status_code": {
              "type": "integer",
              "value": "200"
            }
          }
        }
        "###);
    }

    #[test]
    fn test_strict_metric_unit_serialization() {
        let unit = StrictMetricUnit::Duration(DurationUnit::Second);
        assert_eq!(unit.to_string(), "second");

        let unit = StrictMetricUnit::Information(InformationUnit::Byte);
        assert_eq!(unit.to_string(), "byte");

        let unit = StrictMetricUnit::Fraction(FractionUnit::Percent);
        assert_eq!(unit.to_string(), "percent");

        let unit = StrictMetricUnit::None;
        assert_eq!(unit.to_string(), "none");
    }

    #[test]
    fn test_trace_metric_with_custom_unit() {
        let json = r#"{
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "name": "custom.metric",
            "type": "counter",
            "value": 42,
            "unit": "customunit"
        }"#;

        let data = Annotated::<TraceMetric>::from_json(json).unwrap();
        let trace_metric = data.value().unwrap();

        assert_eq!(trace_metric.unit.value(), Some(&StrictMetricUnit::None));
    }
}
