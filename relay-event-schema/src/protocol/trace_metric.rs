use relay_protocol::{
    Annotated, Empty, FromValue, Getter, IntoValue, Object, SkipSerialization, Value,
};
use std::fmt::{self, Display};

use relay_base_schema::metrics::MetricUnit;
use serde::{Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::{Attributes, SpanId, Timestamp, TraceId};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[metastructure(process_func = "process_trace_metric", value_type = "TraceMetric")]
pub struct TraceMetric {
    /// Timestamp when the metric was created.
    #[metastructure(required = true)]
    pub timestamp: Annotated<Timestamp>,

    /// The ID of the trace the metric belongs to.
    #[metastructure(pii = "false", required = true, trim = false)]
    pub trace_id: Annotated<TraceId>,

    /// The Span this metric belongs to.
    #[metastructure(pii = "false", required = false, trim = false)]
    pub span_id: Annotated<SpanId>,

    /// The metric name.
    #[metastructure(pii = "false", required = true, trim = false)]
    pub name: Annotated<String>,

    /// The metric type.
    #[metastructure(pii = "false", required = true, field = "type")]
    pub r#type: Annotated<MetricType>,

    #[metastructure(pii = "false", required = false)]
    pub unit: Annotated<MetricUnit>,

    /// The metric value. Should be constrained to a number.
    #[metastructure(pii = "maybe", required = true, trim = false)]
    pub value: Annotated<Value>,

    /// Arbitrary attributes on a metric.
    #[metastructure(pii = "maybe", trim = false)]
    pub attributes: Annotated<Attributes>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

impl Getter for TraceMetric {
    fn get_value(&self, path: &str) -> Option<relay_protocol::Val<'_>> {
        Some(match path.strip_prefix("metric.")? {
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
    // Set has been omitted as it's not currently supported.
    Gauge,
    Distribution,
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
}
