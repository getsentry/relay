//! OpenTelemetry metrics to Sentry TraceMetric conversion.
//!
//! This module handles the conversion of OTLP MetricsData into Sentry TraceMetric format.

use chrono::{DateTime, TimeZone, Utc};
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    ExponentialHistogramDataPoint, HistogramDataPoint, Metric, MetricsData, NumberDataPoint,
    SummaryDataPoint,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message as _;
use relay_base_schema::metrics::MetricUnit;
use relay_event_schema::protocol::{
    Attributes, MetricType, SpanId, Timestamp, TraceId, TraceMetric,
};
use relay_protocol::{Annotated, Value};

use crate::integrations::OtelFormat;
use crate::processing::trace_metrics::{Error, Result};
use crate::services::outcome::DiscardReason;

/// Expands OTeL metrics into the [`TraceMetric`] format.
pub fn expand<F>(format: OtelFormat, payload: &[u8], mut produce: F) -> Result<()>
where
    F: FnMut(TraceMetric),
{
    let metrics_data = parse_metrics_data(format, payload)?;

    for resource_metrics in metrics_data.resource_metrics {
        let resource = resource_metrics.resource.as_ref();
        for scope_metrics in resource_metrics.scope_metrics {
            let scope = scope_metrics.scope.as_ref();
            for metric in scope_metrics.metrics {
                for trace_metric in otel_metric_to_trace_metrics(metric, resource, scope) {
                    produce(trace_metric);
                }
            }
        }
    }

    Ok(())
}

fn parse_metrics_data(format: OtelFormat, payload: &[u8]) -> Result<MetricsData> {
    match format {
        OtelFormat::Json => serde_json::from_slice(payload).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse metrics data as JSON"
            );
            Error::Invalid(DiscardReason::InvalidJson)
        }),
        OtelFormat::Protobuf => MetricsData::decode(payload).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse metrics data as protobuf"
            );
            Error::Invalid(DiscardReason::InvalidProtobuf)
        }),
    }
}

/// Converts an OTLP metric to one or more TraceMetric items.
///
/// OTLP metrics have complex types that map to TraceMetric as follows:
/// - Gauge -> Gauge (direct mapping)
/// - Sum (monotonic) -> Counter
/// - Sum (non-monotonic) -> Gauge
/// - Histogram -> Distribution (emits sum and count as separate metrics)
/// - ExponentialHistogram -> Distribution (emits sum and count as separate metrics)
/// - Summary -> Distribution (emits sum, count, and quantiles as separate metrics)
fn otel_metric_to_trace_metrics(
    metric: Metric,
    resource: Option<&Resource>,
    scope: Option<&InstrumentationScope>,
) -> Vec<TraceMetric> {
    let mut result = Vec::new();

    let unit = parse_unit(&metric.unit);

    let Some(data) = metric.data else {
        return result;
    };

    match data {
        Data::Gauge(gauge) => {
            for data_point in gauge.data_points {
                if let Some(trace_metric) = convert_number_data_point(
                    &metric.name,
                    MetricType::Gauge,
                    unit,
                    data_point,
                    resource,
                    scope,
                ) {
                    result.push(trace_metric);
                }
            }
        }
        Data::Sum(sum) => {
            // Monotonic sums are counters, non-monotonic are gauges
            let metric_type = if sum.is_monotonic {
                MetricType::Counter
            } else {
                MetricType::Gauge
            };
            for data_point in sum.data_points {
                if let Some(trace_metric) = convert_number_data_point(
                    &metric.name,
                    metric_type.clone(),
                    unit,
                    data_point,
                    resource,
                    scope,
                ) {
                    result.push(trace_metric);
                }
            }
        }
        Data::Histogram(histogram) => {
            for data_point in histogram.data_points {
                result.extend(convert_histogram_data_point(
                    &metric.name,
                    unit,
                    data_point,
                    resource,
                    scope,
                ));
            }
        }
        Data::ExponentialHistogram(exp_histogram) => {
            for data_point in exp_histogram.data_points {
                result.extend(convert_exponential_histogram_data_point(
                    &metric.name,
                    unit,
                    data_point,
                    resource,
                    scope,
                ));
            }
        }
        Data::Summary(summary) => {
            for data_point in summary.data_points {
                result.extend(convert_summary_data_point(
                    &metric.name,
                    unit,
                    data_point,
                    resource,
                    scope,
                ));
            }
        }
    }

    result
}

fn convert_number_data_point(
    name: &str,
    metric_type: MetricType,
    unit: Option<MetricUnit>,
    data_point: NumberDataPoint,
    resource: Option<&Resource>,
    scope: Option<&InstrumentationScope>,
) -> Option<TraceMetric> {
    let value = match data_point.value? {
        NumberValue::AsDouble(v) => Value::F64(v),
        NumberValue::AsInt(v) => Value::I64(v),
    };

    let timestamp = nanos_to_timestamp(data_point.time_unix_nano);
    let (trace_id, span_id) = extract_trace_context(&data_point.exemplars);
    let attributes = build_attributes(&data_point.attributes, resource, scope);

    Some(TraceMetric {
        timestamp: Annotated::new(timestamp),
        trace_id: Annotated::new(trace_id),
        span_id: span_id.map(Annotated::new).unwrap_or_default(),
        name: Annotated::new(name.to_owned()),
        ty: Annotated::new(metric_type),
        unit: unit.map(Annotated::new).unwrap_or_default(),
        value: Annotated::new(value),
        attributes: Annotated::new(attributes),
        other: Default::default(),
    })
}

fn convert_histogram_data_point(
    name: &str,
    unit: Option<MetricUnit>,
    data_point: HistogramDataPoint,
    resource: Option<&Resource>,
    scope: Option<&InstrumentationScope>,
) -> Vec<TraceMetric> {
    let mut result = Vec::new();
    let timestamp = nanos_to_timestamp(data_point.time_unix_nano);
    let (trace_id, span_id) = extract_trace_context(&data_point.exemplars);
    let attributes = build_attributes(&data_point.attributes, resource, scope);

    // Emit sum metric
    if let Some(sum) = data_point.sum {
        result.push(TraceMetric {
            timestamp: Annotated::new(timestamp),
            trace_id: Annotated::new(trace_id),
            span_id: span_id.map(Annotated::new).unwrap_or_default(),
            name: Annotated::new(format!("{name}.sum")),
            ty: Annotated::new(MetricType::Distribution),
            unit: unit.map(Annotated::new).unwrap_or_default(),
            value: Annotated::new(Value::F64(sum)),
            attributes: Annotated::new(attributes.clone()),
            other: Default::default(),
        });
    }

    // Emit count metric
    result.push(TraceMetric {
        timestamp: Annotated::new(timestamp),
        trace_id: Annotated::new(trace_id),
        span_id: span_id.map(Annotated::new).unwrap_or_default(),
        name: Annotated::new(format!("{name}.count")),
        ty: Annotated::new(MetricType::Distribution),
        unit: None.into(),
        value: Annotated::new(Value::U64(data_point.count)),
        attributes: Annotated::new(attributes),
        other: Default::default(),
    });

    result
}

fn convert_exponential_histogram_data_point(
    name: &str,
    unit: Option<MetricUnit>,
    data_point: ExponentialHistogramDataPoint,
    resource: Option<&Resource>,
    scope: Option<&InstrumentationScope>,
) -> Vec<TraceMetric> {
    let mut result = Vec::new();
    let timestamp = nanos_to_timestamp(data_point.time_unix_nano);
    let (trace_id, span_id) = extract_trace_context(&data_point.exemplars);
    let attributes = build_attributes(&data_point.attributes, resource, scope);

    // Emit sum metric
    if let Some(sum) = data_point.sum {
        result.push(TraceMetric {
            timestamp: Annotated::new(timestamp),
            trace_id: Annotated::new(trace_id),
            span_id: span_id.map(Annotated::new).unwrap_or_default(),
            name: Annotated::new(format!("{name}.sum")),
            ty: Annotated::new(MetricType::Distribution),
            unit: unit.map(Annotated::new).unwrap_or_default(),
            value: Annotated::new(Value::F64(sum)),
            attributes: Annotated::new(attributes.clone()),
            other: Default::default(),
        });
    }

    // Emit count metric
    result.push(TraceMetric {
        timestamp: Annotated::new(timestamp),
        trace_id: Annotated::new(trace_id),
        span_id: span_id.map(Annotated::new).unwrap_or_default(),
        name: Annotated::new(format!("{name}.count")),
        ty: Annotated::new(MetricType::Distribution),
        unit: None.into(),
        value: Annotated::new(Value::U64(data_point.count)),
        attributes: Annotated::new(attributes),
        other: Default::default(),
    });

    result
}

fn convert_summary_data_point(
    name: &str,
    unit: Option<MetricUnit>,
    data_point: SummaryDataPoint,
    resource: Option<&Resource>,
    scope: Option<&InstrumentationScope>,
) -> Vec<TraceMetric> {
    let mut result = Vec::new();
    let timestamp = nanos_to_timestamp(data_point.time_unix_nano);
    let (trace_id, span_id) = extract_trace_context_empty();
    let attributes = build_attributes(&data_point.attributes, resource, scope);

    // Emit sum metric
    result.push(TraceMetric {
        timestamp: Annotated::new(timestamp),
        trace_id: Annotated::new(trace_id),
        span_id: span_id.map(Annotated::new).unwrap_or_default(),
        name: Annotated::new(format!("{name}.sum")),
        ty: Annotated::new(MetricType::Distribution),
        unit: unit.map(Annotated::new).unwrap_or_default(),
        value: Annotated::new(Value::F64(data_point.sum)),
        attributes: Annotated::new(attributes.clone()),
        other: Default::default(),
    });

    // Emit count metric
    result.push(TraceMetric {
        timestamp: Annotated::new(timestamp),
        trace_id: Annotated::new(trace_id),
        span_id: span_id.map(Annotated::new).unwrap_or_default(),
        name: Annotated::new(format!("{name}.count")),
        ty: Annotated::new(MetricType::Distribution),
        unit: None.into(),
        value: Annotated::new(Value::U64(data_point.count)),
        attributes: Annotated::new(attributes.clone()),
        other: Default::default(),
    });

    // Emit quantile metrics
    for quantile_value in data_point.quantile_values {
        result.push(TraceMetric {
            timestamp: Annotated::new(timestamp),
            trace_id: Annotated::new(trace_id),
            span_id: span_id.map(Annotated::new).unwrap_or_default(),
            name: Annotated::new(format!("{name}.quantile.{}", quantile_value.quantile)),
            ty: Annotated::new(MetricType::Distribution),
            unit: unit.map(Annotated::new).unwrap_or_default(),
            value: Annotated::new(Value::F64(quantile_value.value)),
            attributes: Annotated::new(attributes.clone()),
            other: Default::default(),
        });
    }

    result
}

fn nanos_to_timestamp(nanos: u64) -> Timestamp {
    let secs = (nanos / 1_000_000_000) as i64;
    let subsec_nanos = (nanos % 1_000_000_000) as u32;
    let dt: DateTime<Utc> = Utc.timestamp_opt(secs, subsec_nanos).unwrap();
    Timestamp::from(dt)
}

/// Extract trace_id and span_id from exemplars if available, otherwise generate a random trace_id.
fn extract_trace_context(
    exemplars: &[opentelemetry_proto::tonic::metrics::v1::Exemplar],
) -> (TraceId, Option<SpanId>) {
    // Try to get trace context from exemplars
    for exemplar in exemplars {
        if !exemplar.trace_id.is_empty() {
            // Try to parse as hex string from bytes
            if let Ok(trace_id) = std::str::from_utf8(&exemplar.trace_id)
                .map_err(|_| ())
                .and_then(|s| s.parse::<TraceId>().map_err(|_| ()))
            {
                let span_id = if !exemplar.span_id.is_empty() {
                    std::str::from_utf8(&exemplar.span_id)
                        .ok()
                        .and_then(|s| s.parse::<SpanId>().ok())
                } else {
                    None
                };
                return (trace_id, span_id);
            }
            // Try to convert raw bytes to TraceId (16 bytes -> hex string)
            if exemplar.trace_id.len() == 16 {
                let hex = data_encoding::HEXLOWER.encode(&exemplar.trace_id);
                if let Ok(trace_id) = hex.parse::<TraceId>() {
                    let span_id = if exemplar.span_id.len() == 8 {
                        let hex = data_encoding::HEXLOWER.encode(&exemplar.span_id);
                        hex.parse::<SpanId>().ok()
                    } else {
                        None
                    };
                    return (trace_id, span_id);
                }
            }
        }
    }

    // Generate a random trace_id if none found
    (TraceId::random(), None)
}

/// Generate a random trace_id when no exemplars are available.
fn extract_trace_context_empty() -> (TraceId, Option<SpanId>) {
    (TraceId::random(), None)
}

fn build_attributes(
    otel_attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    resource: Option<&Resource>,
    scope: Option<&InstrumentationScope>,
) -> Attributes {
    let mut attributes = Attributes::new();

    // Add resource attributes with "resource." prefix
    if let Some(resource) = resource {
        for attr in &resource.attributes {
            if let Some(attribute) = attr
                .value
                .as_ref()
                .and_then(|v| v.value.as_ref())
                .and_then(|v| relay_otel::otel_value_to_attribute(v.clone()))
            {
                let key = format!("resource.{}", attr.key);
                attributes.0.insert(key, Annotated::new(attribute));
            }
        }
    }

    // Add scope attributes with "instrumentation." prefix
    if let Some(scope) = scope {
        for attr in &scope.attributes {
            if let Some(attribute) = attr
                .value
                .as_ref()
                .and_then(|v| v.value.as_ref())
                .and_then(|v| relay_otel::otel_value_to_attribute(v.clone()))
            {
                let key = format!("instrumentation.{}", attr.key);
                attributes.0.insert(key, Annotated::new(attribute));
            }
        }

        // Add scope name and version
        if !scope.name.is_empty() {
            attributes.insert("instrumentation.name".to_owned(), scope.name.clone());
        }
        if !scope.version.is_empty() {
            attributes.insert("instrumentation.version".to_owned(), scope.version.clone());
        }
    }

    // Add data point attributes
    for attr in otel_attributes {
        if let Some(attribute) = attr
            .value
            .as_ref()
            .and_then(|v| v.value.as_ref())
            .and_then(|v| relay_otel::otel_value_to_attribute(v.clone()))
        {
            attributes
                .0
                .insert(attr.key.clone(), Annotated::new(attribute));
        }
    }

    attributes
}

fn parse_unit(unit: &str) -> Option<MetricUnit> {
    if unit.is_empty() {
        return None;
    }

    // Try to parse the unit, returning None if invalid
    unit.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_metrics_json() {
        let json = r#"{
            "resourceMetrics": [{
                "resource": {
                    "attributes": [{
                        "key": "service.name",
                        "value": {"stringValue": "test-service"}
                    }],
                    "droppedAttributesCount": 0
                },
                "scopeMetrics": [{
                    "scope": {
                        "name": "my.library",
                        "version": "1.0.0",
                        "attributes": [],
                        "droppedAttributesCount": 0
                    },
                    "metrics": [{
                        "name": "my.gauge",
                        "description": "A gauge metric",
                        "unit": "1",
                        "metadata": [],
                        "gauge": {
                            "dataPoints": [{
                                "startTimeUnixNano": "0",
                                "timeUnixNano": "1544712660300000000",
                                "asDouble": 42.5,
                                "attributes": [{
                                    "key": "env",
                                    "value": {"stringValue": "production"}
                                }],
                                "exemplars": [],
                                "flags": 0
                            }]
                        }
                    }],
                    "schemaUrl": ""
                }],
                "schemaUrl": ""
            }]
        }"#;

        let mut metrics = Vec::new();
        let result = expand(OtelFormat::Json, json.as_bytes(), |m| metrics.push(m));

        assert!(result.is_ok());
        assert_eq!(metrics.len(), 1);

        let metric = &metrics[0];
        assert_eq!(metric.name.as_str(), Some("my.gauge"));
        assert_eq!(metric.ty.value(), Some(&MetricType::Gauge));
        assert_eq!(metric.value.value(), Some(&Value::F64(42.5)));
    }

    #[test]
    fn test_parse_counter_sum() {
        let json = r#"{
            "resourceMetrics": [{
                "scopeMetrics": [{
                    "metrics": [{
                        "name": "my.counter",
                        "sum": {
                            "isMonotonic": true,
                            "dataPoints": [{
                                "startTimeUnixNano": "1544712660300000000",
                                "timeUnixNano": "1544712660300000000",
                                "asDouble": 100.0,
                                "attributes": [],
                                "exemplars": [],
                                "flags": 0
                            }],
                            "aggregationTemporality": 1
                        }
                    }]
                }]
            }]
        }"#;

        let mut metrics = Vec::new();
        let result = expand(OtelFormat::Json, json.as_bytes(), |m| metrics.push(m));

        assert!(result.is_ok());
        assert_eq!(metrics.len(), 1);

        let metric = &metrics[0];
        assert_eq!(metric.name.as_str(), Some("my.counter"));
        assert_eq!(metric.ty.value(), Some(&MetricType::Counter));
        assert_eq!(metric.value.value(), Some(&Value::F64(100.0)));
    }

    #[test]
    fn test_parse_non_monotonic_sum() {
        let json = r#"{
            "resourceMetrics": [{
                "scopeMetrics": [{
                    "metrics": [{
                        "name": "my.sum",
                        "sum": {
                            "isMonotonic": false,
                            "dataPoints": [{
                                "startTimeUnixNano": "1544712660300000000",
                                "timeUnixNano": "1544712660300000000",
                                "asDouble": 50.0,
                                "attributes": [],
                                "exemplars": [],
                                "flags": 0
                            }],
                            "aggregationTemporality": 1
                        }
                    }]
                }]
            }]
        }"#;

        let mut metrics = Vec::new();
        let result = expand(OtelFormat::Json, json.as_bytes(), |m| metrics.push(m));

        assert!(result.is_ok());
        assert_eq!(metrics.len(), 1);

        let metric = &metrics[0];
        assert_eq!(metric.name.as_str(), Some("my.sum"));
        assert_eq!(metric.ty.value(), Some(&MetricType::Gauge));
    }

    #[test]
    fn test_parse_histogram() {
        let json = r#"{
            "resourceMetrics": [{
                "scopeMetrics": [{
                    "metrics": [{
                        "name": "my.histogram",
                        "histogram": {
                            "dataPoints": [{
                                "startTimeUnixNano": "1544712660300000000",
                                "timeUnixNano": "1544712660300000000",
                                "count": 10,
                                "sum": 100.5,
                                "bucketCounts": [2, 3, 5],
                                "explicitBounds": [1.0, 5.0],
                                "attributes": [],
                                "exemplars": [],
                                "flags": 0,
                                "min": 0.0,
                                "max": 10.0
                            }],
                            "aggregationTemporality": 1
                        }
                    }]
                }]
            }]
        }"#;

        let mut metrics = Vec::new();
        let result = expand(OtelFormat::Json, json.as_bytes(), |m| metrics.push(m));

        assert!(result.is_ok());
        // Histogram expands to sum and count
        assert_eq!(metrics.len(), 2);

        let sum_metric = metrics
            .iter()
            .find(|m| m.name.as_str() == Some("my.histogram.sum"));
        let count_metric = metrics
            .iter()
            .find(|m| m.name.as_str() == Some("my.histogram.count"));

        assert!(sum_metric.is_some());
        assert!(count_metric.is_some());

        assert_eq!(sum_metric.unwrap().value.value(), Some(&Value::F64(100.5)));
        assert_eq!(count_metric.unwrap().value.value(), Some(&Value::U64(10)));
    }

    #[test]
    fn test_parse_summary_conversion() {
        // Test the summary conversion logic directly rather than JSON parsing
        // since the opentelemetry-proto crate's JSON serde for Summary may have issues.
        use opentelemetry_proto::tonic::metrics::v1::{
            SummaryDataPoint, summary_data_point::ValueAtQuantile,
        };

        let data_point = SummaryDataPoint {
            time_unix_nano: 1544712660300000000,
            start_time_unix_nano: 1544712660300000000,
            count: 100,
            sum: 500.0,
            quantile_values: vec![
                ValueAtQuantile {
                    quantile: 0.5,
                    value: 5.0,
                },
                ValueAtQuantile {
                    quantile: 0.99,
                    value: 9.0,
                },
            ],
            attributes: vec![],
            flags: 0,
        };

        let metrics = convert_summary_data_point("my.summary", None, data_point, None, None);

        // Summary expands to sum, count, and 2 quantiles
        assert_eq!(metrics.len(), 4);

        assert!(
            metrics
                .iter()
                .any(|m| m.name.as_str() == Some("my.summary.sum"))
        );
        assert!(
            metrics
                .iter()
                .any(|m| m.name.as_str() == Some("my.summary.count"))
        );
        assert!(
            metrics
                .iter()
                .any(|m| m.name.as_str() == Some("my.summary.quantile.0.5"))
        );
        assert!(
            metrics
                .iter()
                .any(|m| m.name.as_str() == Some("my.summary.quantile.0.99"))
        );
    }

    #[test]
    fn test_invalid_json() {
        let result = expand(OtelFormat::Json, b"invalid json", |_| {});
        assert!(result.is_err());
    }

    #[test]
    fn test_resource_and_scope_attributes() {
        let json = r#"{
            "resourceMetrics": [{
                "resource": {
                    "attributes": [{
                        "key": "service.name",
                        "value": {"stringValue": "test-service"}
                    }],
                    "droppedAttributesCount": 0
                },
                "scopeMetrics": [{
                    "scope": {
                        "name": "my.library",
                        "version": "1.0.0",
                        "attributes": [{
                            "key": "scope.attr",
                            "value": {"intValue": "42"}
                        }],
                        "droppedAttributesCount": 0
                    },
                    "metrics": [{
                        "name": "my.metric",
                        "metadata": [],
                        "gauge": {
                            "dataPoints": [{
                                "startTimeUnixNano": "0",
                                "timeUnixNano": "1544712660300000000",
                                "asDouble": 1.0,
                                "attributes": [],
                                "exemplars": [],
                                "flags": 0
                            }]
                        }
                    }],
                    "schemaUrl": ""
                }],
                "schemaUrl": ""
            }]
        }"#;

        let mut metrics = Vec::new();
        let result = expand(OtelFormat::Json, json.as_bytes(), |m| metrics.push(m));

        assert!(result.is_ok());
        assert_eq!(metrics.len(), 1);

        let attrs = metrics[0].attributes.value().unwrap();
        assert!(attrs.0.contains_key("resource.service.name"));
        assert!(attrs.0.contains_key("instrumentation.scope.attr"));
        assert!(attrs.0.contains_key("instrumentation.name"));
        assert!(attrs.0.contains_key("instrumentation.version"));
    }
}
