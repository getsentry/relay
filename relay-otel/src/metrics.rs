//! Transforms OpenTelemetry metrics to Sentry trace metrics.
//!
//! Every data point becomes one trace metric, and every exemplar of a histogram an additional
//! `histogram_exemplar` trace metric with the same `sentry._internal.point_id`:
//!
//!  - Gauges and monotonic delta sums become a `gauge` or `counter`.
//!  - Delta histograms and exponential histograms become a `histogram` with the non-empty
//!    buckets as arrays.
//!
//! Cumulative temporality, non-monotonic sums and summaries are not supported.

use chrono::{DateTime, Utc};
use opentelemetry_proto::tonic::common::v1::{InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Exemplar, ExponentialHistogramDataPoint, HistogramDataPoint,
    Metric as OtelMetric, NumberDataPoint, exemplar, exponential_histogram_data_point, metric,
    number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use relay_base_schema::metrics::{DurationUnit, InformationUnit, MetricUnit};
use relay_conventions::attributes::{SENTRY__ORIGIN, SENTRY__PLATFORM};
use relay_event_schema::protocol::{
    AttributeType, AttributeValue, Attributes, MetricType, SpanId, Timestamp, TraceId, TraceMetric,
    attribute_size, value_size,
};
use relay_protocol::{Annotated, Value};

use crate::logs::nanos_to_utc;
use crate::{otel_resource_to_platform, otel_scope_into_attributes, otel_value_to_attribute};

const POINT_ID: &str = "sentry._internal.point_id";
const BUCKET_LOWERS: &str = "sentry._internal.bucket_lowers";
const BUCKET_UPPERS: &str = "sentry._internal.bucket_uppers";
const BUCKET_COUNTS: &str = "sentry._internal.bucket_counts";
const BUCKET_SUM_SHARES: &str = "sentry._internal.bucket_sum_shares";
const HISTOGRAM_COUNT: &str = "sentry._internal.histogram_count";
const HISTOGRAM_SUM: &str = "sentry._internal.histogram_sum";
const HISTOGRAM_MIN: &str = "sentry._internal.histogram_min";
const HISTOGRAM_MAX: &str = "sentry._internal.histogram_max";

/// The maximum amount of buckets of a histogram data point, larger data points are rejected.
const MAX_HISTOGRAM_BUCKETS: usize = 160;

/// The maximum amount of non-empty buckets on each side of a downscaled exponential histogram.
const MAX_EXPONENTIAL_BUCKETS: usize = 64;

/// Calculates the byte size of a trace metric converted from OpenTelemetry.
///
/// Like the size of trace metrics sent by SDKs, but only the data sent by the user is counted.
/// Internal attributes carrying user data only count their values, attributes added by Relay are
/// not counted.
pub fn calculate_trace_metric_size(metric: &TraceMetric) -> u64 {
    let mut total_size = 0;

    total_size += metric.name.value().map_or(0, |s| s.len());
    total_size += value_size(&metric.value);

    if let Some(attributes) = metric.attributes.value() {
        for (key, value) in &attributes.0 {
            total_size += match key.as_str() {
                SENTRY__ORIGIN | POINT_ID | BUCKET_SUM_SHARES => 0,
                BUCKET_LOWERS | BUCKET_UPPERS | BUCKET_COUNTS | HISTOGRAM_COUNT | HISTOGRAM_SUM
                | HISTOGRAM_MIN | HISTOGRAM_MAX => attribute_size(value),
                key => key.len() + attribute_size(value),
            };
        }
    }

    u64::try_from(total_size).unwrap_or(u64::MAX).max(1)
}

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
        Some(metric::Data::ExponentialHistogram(histogram))
            if is_delta(histogram.aggregation_temporality) =>
        {
            histogram
                .data_points
                .into_iter()
                .map(|point| exponential_histogram_point(&metric, point, &mut produce))
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
    trace_id: TraceId,
    timestamp: DateTime<Utc>,
    attributes: Attributes,
}

impl<'a> Point<'a> {
    fn new(metric: &'a Metric, time_unix_nano: u64, otel_attributes: Vec<KeyValue>) -> Self {
        let trace_id = TraceId::random();

        let mut attributes = metric.attributes.clone();
        otel_attributes_into(&mut attributes, otel_attributes);
        attributes.insert(POINT_ID, trace_id.to_string());

        Self {
            metric,
            trace_id,
            timestamp: nanos_to_utc(time_unix_nano).unwrap_or_else(Utc::now),
            attributes,
        }
    }

    fn trace_metric(&self, ty: MetricType, value: f64, attributes: Attributes) -> TraceMetric {
        TraceMetric {
            timestamp: Annotated::new(Timestamp(self.timestamp)),
            trace_id: Annotated::new(self.trace_id),
            span_id: Annotated::empty(),
            name: Annotated::new(self.metric.name.clone()),
            ty: Annotated::new(ty),
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

        let mut metric = self.trace_metric(MetricType::HistogramExemplar, value, attributes);
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

    let point = Point::new(metric, time_unix_nano, attributes);
    produce(point.trace_metric(ty, value, point.attributes.clone()));
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

    let histogram = Histogram {
        attributes,
        time_unix_nano,
        count,
        sum,
        min,
        max,
        exemplars,
    };
    let buckets = histogram.bounds().and_then(|(min, max)| {
        histogram_buckets(count, sum, min, max, &bucket_counts, &explicit_bounds)
    });
    histogram.convert(metric, buckets, produce)
}

/// Converts an exponential histogram data point and returns the amount of dropped data.
fn exponential_histogram_point<F>(
    metric: &Metric,
    point: ExponentialHistogramDataPoint,
    produce: &mut F,
) -> usize
where
    F: FnMut(TraceMetric),
{
    let ExponentialHistogramDataPoint {
        attributes,
        start_time_unix_nano: _,
        time_unix_nano,
        count,
        sum,
        scale,
        zero_count,
        positive,
        negative,
        flags: _,
        exemplars,
        min,
        max,
        zero_threshold,
    } = point;

    let histogram = Histogram {
        attributes,
        time_unix_nano,
        count,
        sum,
        min,
        max,
        exemplars,
    };
    let buckets = histogram
        .bounds()
        .filter(|_| zero_threshold.is_finite())
        .map(|(min, max)| {
            exponential_buckets(
                min,
                max,
                scale,
                (zero_count, zero_threshold),
                positive.unwrap_or_default(),
                negative.unwrap_or_default(),
            )
        });
    histogram.convert(metric, buckets, produce)
}

/// The bucket independent information of a histogram data point.
struct Histogram {
    attributes: Vec<KeyValue>,
    time_unix_nano: u64,
    count: u64,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
    exemplars: Vec<Exemplar>,
}

impl Histogram {
    /// Returns the minimum and maximum to bound the buckets with, `None` if they are invalid.
    ///
    /// A single bound can not be used to bound the open-ended buckets, so both bounds are only
    /// used if both are known.
    fn bounds(&self) -> Option<(Option<f64>, Option<f64>)> {
        let (min, max) = self.min.zip(self.max).unzip();
        let finite = [self.sum, min, max]
            .into_iter()
            .flatten()
            .all(f64::is_finite);
        finite.then_some((min, max))
    }

    /// Converts the histogram with its non-empty buckets and returns the amount of dropped data.
    fn convert<F>(self, metric: &Metric, buckets: Option<Vec<Bucket>>, produce: &mut F) -> usize
    where
        F: FnMut(TraceMetric),
    {
        let Self {
            attributes,
            time_unix_nano,
            count,
            sum,
            min,
            max,
            exemplars,
        } = self;

        // A histogram without observations has nothing to record.
        if count == 0 {
            return 0;
        }
        let Some(buckets) = buckets.filter(|buckets| {
            !buckets.is_empty() && buckets.iter().all(|bucket| bucket.mid().is_finite())
        }) else {
            return 1;
        };

        let point = Point::new(metric, time_unix_nano, attributes);

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

        // Every bucket is estimated at its midpoint, the difference to the sum is spread across
        // buckets proportionally to their width, but not beyond their bounds.
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

        let counts = buckets
            .iter()
            .map(|b| i64::try_from(b.count).unwrap_or(i64::MAX));
        attributes.insert(BUCKET_COUNTS, array(counts.map(Value::I64)));
        let lowers = buckets.iter().map(|b| Value::F64(b.lower));
        attributes.insert(BUCKET_LOWERS, array(lowers));
        let uppers = buckets.iter().map(|b| Value::F64(b.upper));
        attributes.insert(BUCKET_UPPERS, array(uppers));
        let sum_shares = sum_shares.into_iter().map(Value::F64);
        attributes.insert(BUCKET_SUM_SHARES, array(sum_shares));

        produce(point.trace_metric(MetricType::Histogram, value, attributes));
        point.exemplars(exemplars, produce)
    }
}

/// Creates an array attribute from homogeneous values.
fn array(values: impl Iterator<Item = Value>) -> AttributeValue {
    AttributeValue {
        ty: Annotated::new(AttributeType::Array),
        value: Annotated::new(Value::Array(values.map(Annotated::new).collect())),
    }
}

/// Returns all non-empty buckets of a histogram, `None` if the buckets are invalid.
///
/// The lowest and highest non-empty bucket are bounded by the minimum and maximum, if they are
/// within the bucket. The unbounded first and last bucket otherwise collapse onto their only
/// finite bound. A histogram without explicit buckets is a single bucket from minimum to maximum.
fn histogram_buckets(
    count: u64,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
    counts: &[u64],
    bounds: &[f64],
) -> Option<Vec<Bucket>> {
    match (counts.len(), bounds.len()) {
        (0 | 1, 0) => {
            let avg = sum.map(|sum| sum / count as f64);
            Some(vec![Bucket {
                lower: min.or(avg)?,
                upper: max.or(avg)?,
                count,
            }])
        }
        (len, bounds_len) if len == bounds_len + 1 && len <= MAX_HISTOGRAM_BUCKETS => {
            let lowest = counts.iter().position(|&count| count > 0);
            let highest = counts.iter().rposition(|&count| count > 0);

            let buckets = counts
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
                .collect();
            Some(buckets)
        }
        _ => None,
    }
}

/// Returns all non-empty buckets of an exponential histogram.
///
/// Negative buckets are mirrored onto the positive bucket boundaries and the zero bucket spans
/// from the negative to the positive zero threshold. The lowest and highest non-empty bucket are
/// bounded by the minimum and maximum, if they are within the bucket.
fn exponential_buckets(
    min: Option<f64>,
    max: Option<f64>,
    scale: i32,
    (zero_count, zero_threshold): (u64, f64),
    positive: exponential_histogram_data_point::Buckets,
    negative: exponential_histogram_data_point::Buckets,
) -> Vec<Bucket> {
    let mut buckets: Vec<_> = exponential_side(scale, negative)
        .into_iter()
        .rev()
        .map(|bucket| Bucket {
            lower: -bucket.upper,
            upper: -bucket.lower,
            count: bucket.count,
        })
        .collect();
    if zero_count > 0 {
        buckets.push(Bucket {
            lower: -zero_threshold.abs(),
            upper: zero_threshold.abs(),
            count: zero_count,
        });
    }
    buckets.extend(exponential_side(scale, positive));

    if let Some(first) = buckets.first_mut()
        && let Some(min) = min.filter(|&min| first.lower <= min && min <= first.upper)
    {
        first.lower = min;
    }
    if let Some(last) = buckets.last_mut()
        && let Some(max) = max.filter(|&max| last.lower <= max && max <= last.upper)
    {
        last.upper = max;
    }

    buckets
}

/// Returns the non-empty buckets of one side of an exponential histogram with positive bounds.
///
/// The buckets are downscaled until there are at most [`MAX_EXPONENTIAL_BUCKETS`], every step
/// merges two neighbouring buckets.
fn exponential_side(scale: i32, buckets: exponential_histogram_data_point::Buckets) -> Vec<Bucket> {
    let exponential_histogram_data_point::Buckets {
        offset,
        bucket_counts,
    } = buckets;

    let buckets: Vec<_> = bucket_counts
        .into_iter()
        .enumerate()
        .filter(|(_, count)| *count > 0)
        .map(|(i, count)| (i64::from(offset) + i as i64, count))
        .collect();

    let downscaled = |shift: u32| buckets.chunk_by(move |a, b| a.0 >> shift == b.0 >> shift);
    let shift = (0..63)
        .find(|&shift| downscaled(shift).count() <= MAX_EXPONENTIAL_BUCKETS)
        .unwrap_or(63);

    // Bucket `index` covers `(base^index, base^(index + 1)]` with `base = 2^(2^-scale)`.
    let factor = 2f64.powf(f64::from(shift) - f64::from(scale));
    downscaled(shift)
        .map(|chunk| {
            let index = (chunk[0].0 >> shift) as f64;
            Bucket {
                lower: 2f64.powf(index * factor),
                upper: 2f64.powf((index + 1.0) * factor),
                count: chunk
                    .iter()
                    .fold(0, |sum, (_, count)| sum.saturating_add(*count)),
            }
        })
        .collect()
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

    /// Returns the numbers of an array attribute.
    fn numbers(metric: &Annotated<TraceMetric>, key: &str) -> Vec<f64> {
        let attributes = metric.value().unwrap().attributes.value().unwrap();
        let Some(Value::Array(values)) = attributes.get_value(key) else {
            panic!("{key} is not an array");
        };
        let number = |value: &Annotated<Value>| match value.value() {
            Some(&Value::F64(value)) => value,
            Some(&Value::I64(value)) => value as f64,
            value => panic!("{value:?} is not a number"),
        };
        values.iter().map(number).collect()
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
    fn test_cumulative_sum() {
        let (metrics, dropped) = convert(
            r#"{"sum": {"aggregationTemporality": 2, "dataPoints": [{"asInt": "3"}, {"asInt": "4"}]}}"#,
        );
        assert!(metrics.is_empty());
        assert_eq!(dropped, 2);
    }

    #[test]
    fn test_non_monotonic_sum() {
        let (metrics, dropped) = convert(
            r#"{"sum": {"aggregationTemporality": 1, "isMonotonic": false, "dataPoints": [{"asInt": "3"}]}}"#,
        );
        assert!(metrics.is_empty());
        assert_eq!(dropped, 1);
    }

    #[test]
    fn test_summary() {
        let (metrics, dropped) =
            convert(r#"{"summary": {"dataPoints": [{"count": "1", "sum": 1.0}]}}"#);
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
            "type": "histogram_exemplar",
            "unit": "millisecond",
            "value": 3.0,
            "attributes": {
              "sentry._internal.point_id": {
                "type": "string",
                "value": "[point_id]"
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
        assert_eq!(metrics.len(), 1);
        let value = metrics[0].value().unwrap().value.value();
        assert_eq!(value, Some(&Value::F64(5.0)));
        assert_eq!(numbers(&metrics[0], BUCKET_LOWERS), [5.0]);
        assert_eq!(numbers(&metrics[0], BUCKET_UPPERS), [5.0]);
        assert_eq!(numbers(&metrics[0], BUCKET_COUNTS), [4.0]);
        assert_eq!(numbers(&metrics[0], BUCKET_SUM_SHARES), [20.0]);
    }

    #[test]
    fn test_histogram_single_bound() {
        let (metrics, dropped) = convert(
            r#"{
              "name": "queue.size",
              "histogram": {
                "aggregationTemporality": 1,
                "dataPoints": [
                  {"count": "1", "max": 7.0, "bucketCounts": ["1", "0"], "explicitBounds": [10.0]},
                  {"count": "1", "min": 12.0, "bucketCounts": ["0", "1"], "explicitBounds": [10.0]}
                ]
              }
            }"#,
        );

        // A single bound does not bound the open-ended buckets, they collapse onto their bound.
        assert_eq!(dropped, 0);
        assert_eq!(numbers(&metrics[0], BUCKET_LOWERS), [10.0]);
        assert_eq!(numbers(&metrics[0], BUCKET_UPPERS), [10.0]);
        assert_eq!(numbers(&metrics[1], BUCKET_LOWERS), [10.0]);
        assert_eq!(numbers(&metrics[1], BUCKET_UPPERS), [10.0]);
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
        // The shares add up to the sum, but never leave their bucket.
        assert_eq!(
            numbers(&metrics[0], BUCKET_SUM_SHARES),
            [4.999958319439814, 11999.50004168056]
        );
        assert_eq!(numbers(&metrics[1], BUCKET_SUM_SHARES), [-2.5, 7.5]);
        // The buckets can not hold the sum, the shares stop at the upper bounds.
        assert_eq!(numbers(&metrics[2], BUCKET_SUM_SHARES), [10.0, 10.0]);
    }

    #[test]
    fn test_exponential_histogram() {
        let (metrics, dropped) = convert(
            r#"{
              "name": "http.server.duration",
              "exponentialHistogram": {
                "aggregationTemporality": 1,
                "dataPoints": [{
                  "count": "7",
                  "sum": 5.0,
                  "scale": 0,
                  "zeroCount": "1",
                  "zeroThreshold": 0.5,
                  "positive": {"offset": 0, "bucketCounts": ["1", "2"]},
                  "negative": {"offset": 1, "bucketCounts": ["3"]},
                  "min": -3.0,
                  "max": 3.5
                }]
              }
            }"#,
        );

        assert_eq!(dropped, 0);
        assert_eq!(metrics.len(), 1);
        assert_eq!(numbers(&metrics[0], BUCKET_LOWERS), [-3.0, -0.5, 1.0, 2.0]);
        assert_eq!(numbers(&metrics[0], BUCKET_UPPERS), [-2.0, 0.5, 2.0, 3.5]);
    }

    #[test]
    fn test_exponential_histogram_downscale() {
        let buckets = exponential_side(
            0,
            exponential_histogram_data_point::Buckets {
                offset: -10,
                bucket_counts: vec![1; 100],
            },
        );

        // Downscaled once to scale -1, which merges every two buckets.
        assert_eq!(buckets.len(), 50);
        assert!(buckets.iter().all(|bucket| bucket.count == 2));
        assert_eq!(
            (buckets[0].lower, buckets[0].upper),
            (2f64.powi(-10), 2f64.powi(-8))
        );
    }

    #[test]
    fn test_calculate_trace_metric_size() {
        let (metrics, dropped) = convert(
            r#"{
              "name": "http.server.duration",
              "histogram": {
                "aggregationTemporality": 1,
                "dataPoints": [{
                  "attributes": [{"key": "route", "value": {"stringValue": "/api"}}],
                  "count": "3",
                  "sum": 30.0,
                  "bucketCounts": ["1", "2"],
                  "explicitBounds": [10.0],
                  "min": 5.0,
                  "max": 15.0,
                  "exemplars": [{
                    "asDouble": 12.0,
                    "filteredAttributes": [{"key": "user", "value": {"stringValue": "foo"}}]
                  }]
                }]
              }
            }"#,
        );

        assert_eq!(dropped, 0);
        let sizes: Vec<_> = metrics
            .iter()
            .map(|metric| calculate_trace_metric_size(metric.value().unwrap()))
            .collect();

        // Name, value and `route`, the point additionally counts 3 arrays of 2 numbers and 4
        // numbers, the exemplar its `user` attribute.
        assert_eq!(sizes, [20 + 8 + 9 + 3 * 16 + 4 * 8, 20 + 8 + 9 + 7]);
    }
}
