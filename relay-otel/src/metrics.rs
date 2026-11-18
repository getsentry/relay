//! Transforms OpenTelemetry metrics to Sentry trace metrics.
//!
//! Every OpenTelemetry data point is converted into one or more trace metrics, which all share
//! the same `sentry._internal.point_id`:
//!
//!  - Gauge and delta Sum data points are converted into a single `point` trace metric.
//!  - Delta Histogram data points are converted into one `bucket` trace metric for each
//!    non-empty bucket.
//!  - Every exemplar is converted into an additional `exemplar` trace metric.
//!
//! Cumulative temporality, exponential histograms and summaries are not supported.

use chrono::{DateTime, Utc};
use opentelemetry_proto::tonic::common::v1::{InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Exemplar, HistogramDataPoint, Metric as OtelMetric, NumberDataPoint,
    exemplar, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use relay_base_schema::metrics::{DurationUnit, InformationUnit, MetricUnit};
use relay_conventions::attributes::{SENTRY__ORIGIN, SENTRY__PLATFORM};
use relay_event_schema::protocol::{
    Attributes, MetricType, SpanId, Timestamp, TraceId, TraceMetric,
};
use relay_protocol::{Annotated, Value};

use crate::logs::nanos_to_utc;
use crate::{otel_resource_to_platform, otel_scope_into_attributes, otel_value_to_attribute};

const ROW_KIND: &str = "sentry._internal.row_kind";
const POINT_ID: &str = "sentry._internal.point_id";
const BUCKET_LOWER: &str = "sentry._internal.bucket_lower";
const BUCKET_UPPER: &str = "sentry._internal.bucket_upper";
const BUCKET_COUNT: &str = "sentry._internal.bucket_count";
const SUM_SHARE: &str = "sentry._internal.sum_share";
const HISTOGRAM_COUNT: &str = "sentry._internal.histogram_count";
const HISTOGRAM_SUM: &str = "sentry._internal.histogram_sum";
const HISTOGRAM_MIN: &str = "sentry._internal.histogram_min";
const HISTOGRAM_MAX: &str = "sentry._internal.histogram_max";

/// Transforms an OpenTelemetry metric into Sentry trace metrics.
///
/// Returns the amount of data points and exemplars which could not be converted.
pub fn otel_to_sentry_trace_metrics<F>(
    otel_metric: OtelMetric,
    resource: Option<&Resource>,
    scope: Option<&InstrumentationScope>,
    mut produce: F,
) -> usize
where
    F: FnMut(TraceMetric),
{
    let OtelMetric {
        name,
        description: _,
        unit,
        metadata: _,
        data,
    } = otel_metric;

    let mut attributes = Attributes::default();
    attributes.insert(SENTRY__ORIGIN, "auto.otlp.metrics".to_owned());
    if let Some(platform) = resource.and_then(otel_resource_to_platform) {
        attributes.insert(SENTRY__PLATFORM, platform.to_owned());
    }
    otel_scope_into_attributes(&mut attributes, resource, scope);

    let metric = Metric {
        name,
        unit: otel_unit_to_sentry_unit(&unit),
        attributes,
    };

    let is_delta = |temporality| temporality == AggregationTemporality::Delta as i32;

    match data {
        None => 0,
        Some(metric::Data::Gauge(gauge)) => gauge
            .data_points
            .into_iter()
            .map(|point| number_point(&metric, MetricType::Gauge, point, &mut produce))
            .sum(),
        Some(metric::Data::Sum(sum)) if is_delta(sum.aggregation_temporality) => sum
            .data_points
            .into_iter()
            .map(|point| number_point(&metric, MetricType::Counter, point, &mut produce))
            .sum(),
        Some(metric::Data::Histogram(histogram)) if is_delta(histogram.aggregation_temporality) => {
            histogram
                .data_points
                .into_iter()
                .map(|point| histogram_point(&metric, point, &mut produce))
                .sum()
        }
        Some(metric::Data::Sum(sum)) => sum.data_points.len(),
        Some(metric::Data::Histogram(histogram)) => histogram.data_points.len(),
        Some(metric::Data::ExponentialHistogram(histogram)) => histogram.data_points.len(),
        Some(metric::Data::Summary(summary)) => summary.data_points.len(),
    }
}

/// Maps the OpenTelemetry (UCUM) units, which are not natively understood by Sentry.
fn otel_unit_to_sentry_unit(unit: &str) -> Option<MetricUnit> {
    match unit {
        "us" => Some(MetricUnit::Duration(DurationUnit::MicroSecond)),
        "By" => Some(MetricUnit::Information(InformationUnit::Byte)),
        unit => unit.parse().ok(),
    }
}

/// Information shared by all data points of a metric.
struct Metric {
    name: String,
    unit: Option<MetricUnit>,
    attributes: Attributes,
}

/// Information shared by all trace metrics created from the same data point.
struct Point<'a> {
    metric: &'a Metric,
    ty: MetricType,
    trace_id: TraceId,
    timestamp: DateTime<Utc>,
    attributes: Attributes,
}

impl<'a> Point<'a> {
    fn new(
        metric: &'a Metric,
        ty: MetricType,
        time_unix_nano: u64,
        otel_attributes: Vec<KeyValue>,
    ) -> Self {
        let trace_id = TraceId::random();

        let mut attributes = metric.attributes.clone();
        otel_attributes_into(&mut attributes, otel_attributes);
        attributes.insert(POINT_ID, trace_id.to_string());

        Self {
            metric,
            ty,
            trace_id,
            timestamp: nanos_to_utc(time_unix_nano).unwrap_or_else(Utc::now),
            attributes,
        }
    }

    fn trace_metric(&self, row_kind: &str, value: f64, mut attributes: Attributes) -> TraceMetric {
        attributes.insert(ROW_KIND, row_kind);

        TraceMetric {
            timestamp: Annotated::new(Timestamp(self.timestamp)),
            trace_id: Annotated::new(self.trace_id),
            span_id: Annotated::empty(),
            name: Annotated::new(self.metric.name.clone()),
            ty: Annotated::new(self.ty.clone()),
            unit: Annotated::from(self.metric.unit),
            value: Annotated::new(Value::F64(value)),
            attributes: Annotated::new(attributes),
            other: Default::default(),
        }
    }

    /// Converts all exemplars and returns the amount of invalid exemplars.
    fn exemplars<F>(&self, exemplars: Vec<Exemplar>, produce: &mut F) -> usize
    where
        F: FnMut(TraceMetric),
    {
        let mut dropped = 0;
        for exemplar in exemplars {
            match self.exemplar(exemplar) {
                Some(metric) => produce(metric),
                None => dropped += 1,
            }
        }
        dropped
    }

    fn exemplar(&self, exemplar: Exemplar) -> Option<TraceMetric> {
        let Exemplar {
            filtered_attributes,
            time_unix_nano,
            span_id,
            trace_id,
            value,
        } = exemplar;

        let value = match value? {
            exemplar::Value::AsDouble(value) => value,
            exemplar::Value::AsInt(value) => value as f64,
        };
        if !value.is_finite() {
            return None;
        }

        let mut attributes = self.attributes.clone();
        otel_attributes_into(&mut attributes, filtered_attributes);

        let mut metric = self.trace_metric("exemplar", value, attributes);
        if let Some(timestamp) = nanos_to_utc(time_unix_nano) {
            metric.timestamp = Annotated::new(Timestamp(timestamp));
        }
        if let Ok(trace_id) = TraceId::try_from(trace_id.as_slice()) {
            metric.trace_id = Annotated::new(trace_id);
        }
        metric.span_id = SpanId::try_from(span_id.as_slice()).ok().into();

        Some(metric)
    }
}

/// Converts a gauge or sum data point and returns the amount of dropped data.
fn number_point<F>(
    metric: &Metric,
    ty: MetricType,
    point: NumberDataPoint,
    produce: &mut F,
) -> usize
where
    F: FnMut(TraceMetric),
{
    let NumberDataPoint {
        attributes,
        start_time_unix_nano: _,
        time_unix_nano,
        exemplars,
        flags: _,
        value,
    } = point;

    let value = match value {
        Some(number_data_point::Value::AsDouble(value)) => value,
        Some(number_data_point::Value::AsInt(value)) => value as f64,
        None => return 1,
    };
    if !value.is_finite() {
        return 1;
    }

    let point = Point::new(metric, ty, time_unix_nano, attributes);
    produce(point.trace_metric("point", value, point.attributes.clone()));
    point.exemplars(exemplars, produce)
}

/// A histogram bucket with inclusive bounds.
struct Bucket {
    lower: f64,
    upper: f64,
    count: u64,
}

impl Bucket {
    fn mid(&self) -> f64 {
        (self.lower + self.upper) / 2.0
    }
}

/// Converts a histogram data point and returns the amount of dropped data.
fn histogram_point<F>(metric: &Metric, point: HistogramDataPoint, produce: &mut F) -> usize
where
    F: FnMut(TraceMetric),
{
    let HistogramDataPoint {
        attributes,
        start_time_unix_nano: _,
        time_unix_nano,
        count,
        sum,
        bucket_counts,
        explicit_bounds,
        exemplars,
        flags: _,
        min,
        max,
    } = point;

    // A histogram without observations has nothing to record.
    if count == 0 {
        return 0;
    }

    let Some(buckets) = histogram_buckets(count, sum, min, max, &bucket_counts, &explicit_bounds)
    else {
        return 1;
    };

    let point = Point::new(metric, MetricType::Histogram, time_unix_nano, attributes);

    let mut attributes = point.attributes.clone();
    attributes.insert(HISTOGRAM_COUNT, i64::try_from(count).unwrap_or(i64::MAX));
    if let Some(sum) = sum {
        attributes.insert(HISTOGRAM_SUM, sum);
    }
    if let Some(min) = min {
        attributes.insert(HISTOGRAM_MIN, min);
    }
    if let Some(max) = max {
        attributes.insert(HISTOGRAM_MAX, max);
    }

    // The sum is distributed across buckets weighted by their estimated sum (count * mid).
    let total: u64 = buckets.iter().map(|b| b.count).sum();
    let weighted: f64 = buckets.iter().map(|b| b.count as f64 * b.mid()).sum();

    for bucket in buckets {
        let estimate = bucket.count as f64 * bucket.mid();
        let sum_share = match sum {
            Some(sum) if weighted != 0.0 && weighted.is_finite() => estimate * (sum / weighted),
            Some(sum) => sum * bucket.count as f64 / total as f64,
            None => estimate,
        };

        let mut attributes = attributes.clone();
        attributes.insert(BUCKET_LOWER, bucket.lower);
        attributes.insert(BUCKET_UPPER, bucket.upper);
        attributes.insert(
            BUCKET_COUNT,
            i64::try_from(bucket.count).unwrap_or(i64::MAX),
        );
        attributes.insert(SUM_SHARE, sum_share);

        produce(point.trace_metric("bucket", bucket.mid(), attributes));
    }

    point.exemplars(exemplars, produce)
}

/// Returns all non-empty buckets of a histogram, `None` if the histogram is invalid.
///
/// The unbounded first and last bucket are bounded by the minimum and maximum of the histogram,
/// if they are within the bucket, otherwise the bucket collapses onto its only finite bound.
///
/// A histogram without explicit buckets is a single bucket from minimum to maximum.
fn histogram_buckets(
    count: u64,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
    counts: &[u64],
    bounds: &[f64],
) -> Option<Vec<Bucket>> {
    if [sum, min, max]
        .into_iter()
        .flatten()
        .any(|v| !v.is_finite())
    {
        return None;
    }

    let buckets: Vec<_> = match (counts.len(), bounds.len()) {
        (0 | 1, 0) => {
            let avg = sum.map(|sum| sum / count as f64);
            vec![Bucket {
                lower: min.or(avg)?,
                upper: max.or(avg)?,
                count,
            }]
        }
        (len, bounds_len) if len == bounds_len + 1 => {
            let first = bounds[0];
            let last = bounds[bounds_len - 1];

            counts
                .iter()
                .enumerate()
                .filter(|(_, count)| **count > 0)
                .map(|(i, &count)| Bucket {
                    lower: match i {
                        0 => min.filter(|&min| min <= first).unwrap_or(first),
                        i => bounds[i - 1],
                    },
                    upper: match i {
                        i if i == bounds_len => max.filter(|&max| max >= last).unwrap_or(last),
                        i => bounds[i],
                    },
                    count,
                })
                .collect()
        }
        _ => return None,
    };

    buckets
        .iter()
        .all(|bucket| bucket.mid().is_finite())
        .then_some(buckets)
}

fn otel_attributes_into(attributes: &mut Attributes, otel_attributes: Vec<KeyValue>) {
    for attribute in otel_attributes {
        if let Some(attr) = attribute
            .value
            .and_then(|v| v.value)
            .and_then(otel_value_to_attribute)
        {
            attributes.0.insert(attribute.key, Annotated::new(attr));
        }
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::SerializableAnnotated;

    use super::*;

    fn convert(json: &str) -> (Vec<Annotated<TraceMetric>>, usize) {
        let otel_metric: OtelMetric = serde_json::from_str(json).unwrap();

        let mut metrics = Vec::new();
        let dropped = otel_to_sentry_trace_metrics(otel_metric, None, None, |metric| {
            metrics.push(Annotated::new(metric))
        });

        (metrics, dropped)
    }

    #[test]
    fn test_sum_and_gauge() {
        let (metrics, dropped) = convert(
            r#"{
              "name": "http.requests",
              "unit": "1",
              "sum": {
                "aggregationTemporality": 1,
                "isMonotonic": true,
                "dataPoints": [
                  {
                    "timeUnixNano": "1700000000000000000",
                    "asInt": "3",
                    "attributes": [{"key": "http.route", "value": {"stringValue": "/checkout"}}]
                  },
                  {"timeUnixNano": "1700000000000000000"}
                ]
              }
            }"#,
        );

        assert_eq!(dropped, 1);
        insta::assert_json_snapshot!(metrics.iter().map(SerializableAnnotated).collect::<Vec<_>>(), {
            "[].trace_id" => "[trace_id]",
            "[].attributes[\"sentry._internal.point_id\"].value" => "[point_id]",
        }, @r#"
        [
          {
            "timestamp": 1700000000.0,
            "trace_id": "[trace_id]",
            "name": "http.requests",
            "type": "counter",
            "unit": "1",
            "value": 3.0,
            "attributes": {
              "http.route": {
                "type": "string",
                "value": "/checkout"
              },
              "sentry._internal.point_id": {
                "type": "string",
                "value": "[point_id]"
              },
              "sentry._internal.row_kind": {
                "type": "string",
                "value": "point"
              },
              "sentry.origin": {
                "type": "string",
                "value": "auto.otlp.metrics"
              }
            }
          }
        ]
        "#);

        let (metrics, dropped) = convert(
            r#"{
              "name": "cpu.utilization",
              "gauge": {"dataPoints": [{"timeUnixNano": "1700000000000000000", "asDouble": 0.75}]}
            }"#,
        );
        assert_eq!(dropped, 0);
        assert_eq!(metrics.len(), 1);
        assert_eq!(
            metrics[0].value().unwrap().ty.value(),
            Some(&MetricType::Gauge)
        );
    }

    #[test]
    fn test_unsupported() {
        let (metrics, dropped) = convert(
            r#"{
              "name": "http.requests",
              "sum": {
                "aggregationTemporality": 2,
                "dataPoints": [{"asInt": "3"}, {"asInt": "4"}]
              }
            }"#,
        );
        assert!(metrics.is_empty());
        assert_eq!(dropped, 2);

        let (metrics, dropped) = convert(
            r#"{
              "name": "http.duration",
              "summary": {"dataPoints": [{"count": "1", "sum": 1.0}]}
            }"#,
        );
        assert!(metrics.is_empty());
        assert_eq!(dropped, 1);
    }

    #[test]
    fn test_histogram() {
        let (metrics, dropped) = convert(
            r#"{
              "name": "http.server.duration",
              "unit": "ms",
              "histogram": {
                "aggregationTemporality": 1,
                "dataPoints": [{
                  "timeUnixNano": "1700000000000000000",
                  "count": "10000",
                  "sum": 1569400.0,
                  "bucketCounts": ["9400", "0", "600", "0"],
                  "explicitBounds": [5.0, 2500.0, 5000.0],
                  "min": 0.4,
                  "max": 4870.0,
                  "exemplars": [{
                    "timeUnixNano": "1699999999000000000",
                    "asDouble": 3.0,
                    "traceId": "5b8efff798038103d269b633813fc60c",
                    "spanId": "eee19b7ec3c1b174",
                    "filteredAttributes": [{"key": "user", "value": {"stringValue": "foo"}}]
                  }]
                }]
              }
            }"#,
        );

        assert_eq!(dropped, 0);
        insta::assert_json_snapshot!(metrics.iter().map(SerializableAnnotated).collect::<Vec<_>>(), {
            "[0:2].trace_id" => "[trace_id]",
            "[].attributes[\"sentry._internal.point_id\"].value" => "[point_id]",
        }, @r#"
        [
          {
            "timestamp": 1700000000.0,
            "trace_id": "[trace_id]",
            "name": "http.server.duration",
            "type": "histogram",
            "unit": "millisecond",
            "value": 2.7,
            "attributes": {
              "sentry._internal.bucket_count": {
                "type": "integer",
                "value": 9400
              },
              "sentry._internal.bucket_lower": {
                "type": "double",
                "value": 0.4
              },
              "sentry._internal.bucket_upper": {
                "type": "double",
                "value": 5.0
              },
              "sentry._internal.histogram_count": {
                "type": "integer",
                "value": 10000
              },
              "sentry._internal.histogram_max": {
                "type": "double",
                "value": 4870.0
              },
              "sentry._internal.histogram_min": {
                "type": "double",
                "value": 0.4
              },
              "sentry._internal.histogram_sum": {
                "type": "double",
                "value": 1569400.0
              },
              "sentry._internal.point_id": {
                "type": "string",
                "value": "[point_id]"
              },
              "sentry._internal.row_kind": {
                "type": "string",
                "value": "bucket"
              },
              "sentry._internal.sum_share": {
                "type": "double",
                "value": 17505.371410489675
              },
              "sentry.origin": {
                "type": "string",
                "value": "auto.otlp.metrics"
              }
            }
          },
          {
            "timestamp": 1700000000.0,
            "trace_id": "[trace_id]",
            "name": "http.server.duration",
            "type": "histogram",
            "unit": "millisecond",
            "value": 3750.0,
            "attributes": {
              "sentry._internal.bucket_count": {
                "type": "integer",
                "value": 600
              },
              "sentry._internal.bucket_lower": {
                "type": "double",
                "value": 2500.0
              },
              "sentry._internal.bucket_upper": {
                "type": "double",
                "value": 5000.0
              },
              "sentry._internal.histogram_count": {
                "type": "integer",
                "value": 10000
              },
              "sentry._internal.histogram_max": {
                "type": "double",
                "value": 4870.0
              },
              "sentry._internal.histogram_min": {
                "type": "double",
                "value": 0.4
              },
              "sentry._internal.histogram_sum": {
                "type": "double",
                "value": 1569400.0
              },
              "sentry._internal.point_id": {
                "type": "string",
                "value": "[point_id]"
              },
              "sentry._internal.row_kind": {
                "type": "string",
                "value": "bucket"
              },
              "sentry._internal.sum_share": {
                "type": "double",
                "value": 1551894.6285895102
              },
              "sentry.origin": {
                "type": "string",
                "value": "auto.otlp.metrics"
              }
            }
          },
          {
            "timestamp": 1699999999.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "name": "http.server.duration",
            "type": "histogram",
            "unit": "millisecond",
            "value": 3.0,
            "attributes": {
              "sentry._internal.point_id": {
                "type": "string",
                "value": "[point_id]"
              },
              "sentry._internal.row_kind": {
                "type": "string",
                "value": "exemplar"
              },
              "sentry.origin": {
                "type": "string",
                "value": "auto.otlp.metrics"
              },
              "user": {
                "type": "string",
                "value": "foo"
              }
            }
          }
        ]
        "#);

        let point_ids: Vec<_> = metrics
            .iter()
            .map(|m| m.value().unwrap().attributes.value().unwrap())
            .map(|a| a.get_value(POINT_ID).unwrap())
            .collect();
        assert!(point_ids.iter().all(|id| *id == point_ids[0]));
    }

    #[test]
    fn test_histogram_without_buckets() {
        let (metrics, dropped) = convert(
            r#"{
              "name": "queue.size",
              "histogram": {
                "aggregationTemporality": 1,
                "dataPoints": [
                  {"count": "4", "sum": 20.0},
                  {"count": "4", "min": 1.0},
                  {"count": "2", "bucketCounts": ["1", "1"], "explicitBounds": []},
                  {"count": "0"}
                ]
              }
            }"#,
        );

        assert_eq!(dropped, 2);
        let metric = metrics[0].value().unwrap();
        let attributes = metric.attributes.value().unwrap();
        assert_eq!(metric.value.value(), Some(&Value::F64(5.0)));
        assert_eq!(attributes.get_value(BUCKET_LOWER), Some(&Value::F64(5.0)));
        assert_eq!(attributes.get_value(BUCKET_UPPER), Some(&Value::F64(5.0)));
        assert_eq!(attributes.get_value(BUCKET_COUNT), Some(&Value::I64(4)));
        assert_eq!(attributes.get_value(SUM_SHARE), Some(&Value::F64(20.0)));
        assert_eq!(metrics.len(), 1);
    }
}
