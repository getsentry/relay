//! Transforms OpenTelemetry metrics to Sentry trace metrics.
//!
//! Every OpenTelemetry data point is converted into one or more trace metrics, which all share
//! the same `sentry._internal.point_id`:
//!
//!  - Gauge and monotonic delta Sum data points are converted into a single `point` trace metric.
//!  - Delta Histogram data points are converted into a single `point` trace metric, which carries
//!    the non-empty buckets as arrays.
//!  - Every exemplar of a histogram is converted into an additional `exemplar` trace metric.
//!    Exemplars of gauges and sums are ignored.
//!
//! Cumulative temporality, non-monotonic sums, exponential histograms and summaries are not
//! supported.

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
    AttributeType, AttributeValue, Attributes, MetricType, SpanId, Timestamp, TraceId, TraceMetric,
};
use relay_protocol::{Annotated, Value};

use crate::logs::nanos_to_utc;
use crate::{otel_resource_to_platform, otel_scope_into_attributes, otel_value_to_attribute};

const ROW_KIND: &str = "sentry._internal.row_kind";
const POINT_ID: &str = "sentry._internal.point_id";
const BUCKET_LOWERS: &str = "sentry._internal.bucket_lowers";
const BUCKET_UPPERS: &str = "sentry._internal.bucket_uppers";
const BUCKET_COUNTS: &str = "sentry._internal.bucket_counts";
const BUCKET_SUM_SHARES: &str = "sentry._internal.bucket_sum_shares";
const HISTOGRAM_COUNT: &str = "sentry._internal.histogram_count";
const HISTOGRAM_SUM: &str = "sentry._internal.histogram_sum";
const HISTOGRAM_MIN: &str = "sentry._internal.histogram_min";
const HISTOGRAM_MAX: &str = "sentry._internal.histogram_max";

/// The maximum amount of buckets of a histogram data point.
///
/// Every non-empty bucket is stored on the trace metric, data points with more buckets are
/// rejected.
const MAX_HISTOGRAM_BUCKETS: usize = 160;

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
        Some(metric::Data::Sum(sum))
            if sum.is_monotonic && is_delta(sum.aggregation_temporality) =>
        {
            sum.data_points
                .into_iter()
                .map(|point| number_point(&metric, MetricType::Counter, point, &mut produce))
                .sum()
        }
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
        exemplars: _,
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
    0
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

    fn half_width(&self) -> f64 {
        (self.upper - self.lower) / 2.0
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

    let total = buckets
        .iter()
        .fold(0u64, |sum, b| sum.saturating_add(b.count));

    // Every bucket is estimated at its midpoint, the difference to the sum is spread across buckets
    // proportionally to their width. The spread is limited to the bounds of the buckets, the shares
    // only add up to the sum if the buckets can hold it.
    let estimate: f64 = buckets.iter().map(|b| b.count as f64 * b.mid()).sum();
    let room: f64 = buckets
        .iter()
        .map(|b| b.count as f64 * b.half_width())
        .sum();
    let spread = match sum {
        Some(sum) if room > 0.0 => ((sum - estimate) / room).clamp(-1.0, 1.0),
        _ => 0.0,
    };
    let sum_shares: Vec<f64> = buckets
        .iter()
        .map(|b| b.count as f64 * (b.mid() + spread * b.half_width()))
        .collect();

    // The value of the point is the mean of all its buckets.
    let value = sum_shares.iter().sum::<f64>() / total as f64;
    if !value.is_finite() {
        return 1;
    }

    attributes.insert(
        BUCKET_LOWERS,
        array(buckets.iter().map(|b| Value::F64(b.lower))),
    );
    attributes.insert(
        BUCKET_UPPERS,
        array(buckets.iter().map(|b| Value::F64(b.upper))),
    );
    attributes.insert(
        BUCKET_COUNTS,
        array(
            buckets
                .iter()
                .map(|b| Value::I64(i64::try_from(b.count).unwrap_or(i64::MAX))),
        ),
    );
    attributes.insert(
        BUCKET_SUM_SHARES,
        array(sum_shares.into_iter().map(Value::F64)),
    );

    produce(point.trace_metric("point", value, attributes));
    point.exemplars(exemplars, produce)
}

/// Creates an array attribute from homogeneous values.
fn array(values: impl Iterator<Item = Value>) -> AttributeValue {
    AttributeValue {
        ty: Annotated::new(AttributeType::Array),
        value: Annotated::new(Value::Array(values.map(Annotated::new).collect())),
    }
}

/// Returns all non-empty buckets of a histogram, `None` if the histogram is invalid or has no
/// non-empty buckets.
///
/// The lowest and highest non-empty bucket are bounded by the minimum and maximum of the histogram,
/// if they are within the bucket. The unbounded first and last bucket otherwise collapse onto their
/// only finite bound.
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
    // A single bound can not be used to bound the open-ended buckets.
    let (min, max) = min.zip(max).unzip();

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
        (len, bounds_len) if len == bounds_len + 1 && len <= MAX_HISTOGRAM_BUCKETS => {
            let lowest = counts.iter().position(|&count| count > 0);
            let highest = counts.iter().rposition(|&count| count > 0);

            counts
                .iter()
                .enumerate()
                .filter(|(_, count)| **count > 0)
                .map(|(i, &count)| {
                    let lower = i.checked_sub(1).map_or(f64::NEG_INFINITY, |i| bounds[i]);
                    let upper = bounds.get(i).copied().unwrap_or(f64::INFINITY);
                    let within = |value: f64| lower <= value && value <= upper;

                    Bucket {
                        lower: match min.filter(|&min| Some(i) == lowest && within(min)) {
                            Some(min) => min,
                            None if i == 0 => upper,
                            None => lower,
                        },
                        upper: match max.filter(|&max| Some(i) == highest && within(max)) {
                            Some(max) => max,
                            None if i == bounds_len => lower,
                            None => upper,
                        },
                        count,
                    }
                })
                .collect()
        }
        _ => return None,
    };

    (!buckets.is_empty() && buckets.iter().all(|bucket| bucket.mid().is_finite()))
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
                    "attributes": [{"key": "http.route", "value": {"stringValue": "/checkout"}}],
                    "exemplars": [{"asInt": "2"}]
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
              "name": "queue.size",
              "sum": {
                "aggregationTemporality": 1,
                "isMonotonic": false,
                "dataPoints": [{"asInt": "3"}]
              }
            }"#,
        );
        assert!(metrics.is_empty());
        assert_eq!(dropped, 1);

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
            "[0].trace_id" => "[trace_id]",
            "[].attributes[\"sentry._internal.point_id\"].value" => "[point_id]",
        }, @r#"
        [
          {
            "timestamp": 1700000000.0,
            "trace_id": "[trace_id]",
            "name": "http.server.duration",
            "type": "histogram",
            "unit": "millisecond",
            "value": 156.93999999999997,
            "attributes": {
              "sentry._internal.bucket_counts": {
                "type": "array",
                "value": [
                  9400,
                  600
                ]
              },
              "sentry._internal.bucket_lowers": {
                "type": "array",
                "value": [
                  0.4,
                  2500.0
                ]
              },
              "sentry._internal.bucket_sum_shares": {
                "type": "array",
                "value": [
                  5697.070787038303,
                  1563702.9292129616
                ]
              },
              "sentry._internal.bucket_uppers": {
                "type": "array",
                "value": [
                  5.0,
                  4870.0
                ]
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
                "value": "point"
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
        let array = |v| Some(Value::Array(vec![Annotated::new(v)]));
        assert_eq!(
            attributes.get_value(BUCKET_LOWERS),
            array(Value::F64(5.0)).as_ref()
        );
        assert_eq!(
            attributes.get_value(BUCKET_UPPERS),
            array(Value::F64(5.0)).as_ref()
        );
        assert_eq!(
            attributes.get_value(BUCKET_COUNTS),
            array(Value::I64(4)).as_ref()
        );
        assert_eq!(
            attributes.get_value(BUCKET_SUM_SHARES),
            array(Value::F64(20.0)).as_ref()
        );
        assert_eq!(metrics.len(), 1);
    }

    #[test]
    fn test_histogram_too_many_buckets() {
        let point = |buckets: usize| {
            format!(
                r#"{{"count": "1", "bucketCounts": {:?}, "explicitBounds": {:?}}}"#,
                (0..buckets).map(|i| (i == 0) as u64).collect::<Vec<_>>(),
                (1..buckets).map(|i| i as f64).collect::<Vec<_>>(),
            )
        };

        let (metrics, dropped) = convert(&format!(
            r#"{{
              "name": "http.server.duration",
              "histogram": {{
                "aggregationTemporality": 1,
                "dataPoints": [{}, {}]
              }}
            }}"#,
            point(MAX_HISTOGRAM_BUCKETS),
            point(MAX_HISTOGRAM_BUCKETS + 1),
        ));

        assert_eq!(dropped, 1);
        assert_eq!(metrics.len(), 1);
    }

    #[test]
    fn test_histogram_sum_shares_within_buckets() {
        let (metrics, dropped) = convert(
            r#"{
              "name": "http.server.duration",
              "histogram": {
                "aggregationTemporality": 1,
                "dataPoints": [
                  {"count": "2", "sum": 12004.5, "min": 4.0, "max": 12000.0, "bucketCounts": ["1", "1"], "explicitBounds": [5.0]},
                  {"count": "2", "sum": 5.0, "min": -10.0, "max": 10.0, "bucketCounts": ["1", "1"], "explicitBounds": [0.0]},
                  {"count": "2", "sum": 1005.0, "bucketCounts": ["0", "1", "1"], "explicitBounds": [0.0, 10.0]}
                ]
              }
            }"#,
        );

        assert_eq!(dropped, 0);
        // The last point has more than its buckets can hold, its shares can not add up to the sum.
        for (metric, sum) in metrics.iter().zip([Some(12004.5), Some(5.0), None]) {
            let attributes = metric.value().unwrap().attributes.value().unwrap();
            let floats = |name| match attributes.get_value(name) {
                Some(Value::Array(values)) => values
                    .iter()
                    .map(|v| match v.value() {
                        Some(Value::F64(v)) => *v,
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>(),
                _ => unreachable!(),
            };

            let shares = floats(BUCKET_SUM_SHARES);
            let bounds = floats(BUCKET_LOWERS).into_iter().zip(floats(BUCKET_UPPERS));
            assert!(
                shares
                    .iter()
                    .zip(bounds)
                    .all(|(s, (l, u))| l <= *s && *s <= u)
            );
            if let Some(sum) = sum {
                assert!((shares.iter().sum::<f64>() - sum).abs() < 1e-9);
            }
        }
    }

    #[test]
    fn test_histogram_single_bound() {
        let buckets = histogram_buckets(1, None, None, Some(7.0), &[1, 0], &[10.0]).unwrap();
        assert_eq!((buckets[0].lower, buckets[0].upper), (10.0, 10.0));

        let buckets = histogram_buckets(1, None, Some(12.0), None, &[0, 1], &[10.0]).unwrap();
        assert_eq!((buckets[0].lower, buckets[0].upper), (10.0, 10.0));
    }
}
