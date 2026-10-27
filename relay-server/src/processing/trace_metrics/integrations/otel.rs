//! Converts OTLP metrics into [`TraceMetric`] rows.
//!
//! Every OTLP data point produces one or more rows which all share a `sentry._internal.point_id`:
//!
//!  - Sum (delta) and Gauge points produce a single `point` row.
//!  - Histogram and ExponentialHistogram (delta) points produce one `bucket` row per non-empty
//!    bucket.
//!  - Every exemplar on any point produces an additional `exemplar` row.
//!
//! Only delta temporality is supported for sums and histograms, cumulative and unspecified points
//! are dropped. Summary metrics are not supported.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use opentelemetry_proto::tonic::common::v1::{InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Exemplar, ExponentialHistogramDataPoint, HistogramDataPoint, Metric,
    MetricsData, NumberDataPoint, exemplar, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message as _;
use relay_base_schema::metrics::MetricUnit;
use relay_conventions::attributes::{SENTRY__ORIGIN, SENTRY__PLATFORM};
use relay_event_schema::protocol::{
    Attributes, MetricType, SpanId, Timestamp, TraceId, TraceMetric,
};
use relay_protocol::{Annotated, Value};
use uuid::Uuid;

use crate::integrations::OtelFormat;
use crate::processing::trace_metrics::{Error, Result, Settings};
use crate::services::outcome::DiscardReason;

/// Maximum amount of non-empty buckets on one side of an exponential histogram.
///
/// Exponential histograms with more buckets are downscaled until they fit.
const MAX_EXPONENTIAL_BUCKETS: usize = 64;

const POINT_ID: &str = "sentry._internal.point_id";
const ROW_KIND: &str = "sentry._internal.row_kind";
const SOURCE: &str = "sentry._internal.source";
const START_TIMESTAMP_PRECISE: &str = "sentry._internal.start_timestamp_precise";
const BUCKET_LOWER: &str = "sentry._internal.bucket_lower";
const BUCKET_UPPER: &str = "sentry._internal.bucket_upper";
const BUCKET_COUNT: &str = "sentry._internal.bucket_count";
const SUM_SHARE: &str = "sentry._internal.sum_share";
const HISTOGRAM_SUM_MISSING: &str = "sentry._internal.histogram_sum_missing";
const HISTOGRAM_COUNT: &str = "sentry._internal.histogram_count";
const HISTOGRAM_SUM: &str = "sentry._internal.histogram_sum";
const HISTOGRAM_MIN: &str = "sentry._internal.histogram_min";
const HISTOGRAM_MAX: &str = "sentry._internal.histogram_max";

/// Expands OTeL metrics into the [`TraceMetric`] format.
pub fn expand<F>(format: OtelFormat, payload: &[u8], mut produce: F) -> Result<Settings>
where
    F: FnMut(TraceMetric),
{
    let metrics = parse_metrics_data(format, payload)?;
    convert_metrics_data(metrics, &mut produce);
    Ok(Settings::default())
}

fn parse_metrics_data(format: OtelFormat, payload: &[u8]) -> Result<MetricsData> {
    match format {
        OtelFormat::Json => {
            let parsed = serde_json::from_slice(payload).and_then(|mut value| {
                json::normalize_metrics_data(&mut value);
                serde_json::from_value(value)
            });

            parsed.map_err(|e| {
                relay_log::debug!(
                    error = &e as &dyn std::error::Error,
                    "Failed to parse metrics data as JSON"
                );
                Error::Invalid(DiscardReason::InvalidJson)
            })
        }
        OtelFormat::Protobuf => MetricsData::decode(payload).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse metrics data as protobuf"
            );
            Error::Invalid(DiscardReason::InvalidProtobuf)
        }),
    }
}

/// Converts all metrics contained in [`MetricsData`] into trace metric rows.
fn convert_metrics_data<F>(data: MetricsData, produce: &mut F)
where
    F: FnMut(TraceMetric),
{
    for resource_metrics in data.resource_metrics {
        let resource = resource_metrics.resource.as_ref();
        for scope_metrics in resource_metrics.scope_metrics {
            let scope = scope_metrics.scope.as_ref();
            let base = base_attributes(resource, scope);

            for metric in scope_metrics.metrics {
                convert_metric(metric, &base, produce);
            }
        }
    }
}

/// Attributes shared by all metrics of a scope, mirroring the OTLP logs conversion.
fn base_attributes(
    resource: Option<&Resource>,
    scope: Option<&InstrumentationScope>,
) -> Attributes {
    let mut attributes = Attributes::default();
    attributes.insert(SENTRY__ORIGIN, "auto.otlp.metrics".to_owned());
    if let Some(platform) = resource.and_then(relay_otel::otel_resource_to_platform) {
        attributes.insert(SENTRY__PLATFORM, platform.to_owned());
    }
    relay_otel::otel_scope_into_attributes(&mut attributes, resource, scope);
    attributes
}

/// Metric level information shared by all data points of a metric.
struct MetricInfo<'a> {
    name: String,
    unit: Option<MetricUnit>,
    base: &'a Attributes,
}

fn convert_metric<F>(metric: Metric, base: &Attributes, produce: &mut F)
where
    F: FnMut(TraceMetric),
{
    let Metric {
        name, unit, data, ..
    } = metric;

    let Some(data) = data else {
        relay_log::debug!(metric = name, "dropping OTLP metric without data");
        return;
    };

    let info = MetricInfo {
        unit: map_unit(&unit),
        name,
        base,
    };

    match data {
        metric::Data::Gauge(gauge) => {
            for point in gauge.data_points {
                convert_number_point(&info, MetricType::Gauge, point, produce);
            }
        }
        metric::Data::Sum(sum) => {
            if !is_delta(&info, sum.aggregation_temporality) {
                return;
            }
            for point in sum.data_points {
                convert_number_point(&info, MetricType::Counter, point, produce);
            }
        }
        metric::Data::Histogram(histogram) => {
            if !is_delta(&info, histogram.aggregation_temporality) {
                return;
            }
            for point in histogram.data_points {
                convert_histogram_point(&info, point, produce);
            }
        }
        metric::Data::ExponentialHistogram(histogram) => {
            if !is_delta(&info, histogram.aggregation_temporality) {
                return;
            }
            for point in histogram.data_points {
                convert_exponential_histogram_point(&info, point, produce);
            }
        }
        metric::Data::Summary(_) => {
            relay_log::debug!(
                metric = info.name,
                "dropping unsupported OTLP summary metric"
            );
        }
    }
}

fn is_delta(info: &MetricInfo<'_>, temporality: i32) -> bool {
    let is_delta = temporality == AggregationTemporality::Delta as i32;
    if !is_delta {
        relay_log::debug!(
            metric = info.name,
            temporality,
            "dropping OTLP metric with unsupported aggregation temporality"
        );
    }
    is_delta
}

/// Maps an OTLP (UCUM) unit to a Sentry [`MetricUnit`].
fn map_unit(unit: &str) -> Option<MetricUnit> {
    let unit = match unit {
        "" | "1" => return Some(MetricUnit::None),
        "ms" => "millisecond",
        "s" => "second",
        "us" => "microsecond",
        "ns" => "nanosecond",
        "By" => "byte",
        unit => unit,
    };

    unit.parse().ok().or(Some(MetricUnit::None))
}

/// State shared by all rows produced from a single OTLP data point.
struct Point<'a> {
    info: &'a MetricInfo<'a>,
    ty: MetricType,
    point_id: String,
    trace_id: TraceId,
    timestamp: DateTime<Utc>,
    start_time_unix_nano: u64,
    /// Base attributes and data point attributes.
    attributes: Attributes,
}

impl<'a> Point<'a> {
    fn new(
        info: &'a MetricInfo<'a>,
        ty: MetricType,
        point_attributes: Vec<KeyValue>,
        time_unix_nano: u64,
        start_time_unix_nano: u64,
    ) -> Self {
        let uuid = Uuid::new_v4();
        let trace_id = TraceId::try_from(uuid).unwrap_or_else(|_| TraceId::random());

        let mut attributes = info.base.clone();
        insert_otel_attributes(&mut attributes, point_attributes);

        Self {
            info,
            ty,
            point_id: uuid.simple().to_string(),
            trace_id,
            timestamp: nanos_to_utc(time_unix_nano).unwrap_or_else(Utc::now),
            start_time_unix_nano,
            attributes,
        }
    }

    /// Creates a row with all attributes shared by rows of this point.
    fn row(
        &self,
        row_kind: &str,
        value: f64,
        mut attributes: Attributes,
        timestamp: DateTime<Utc>,
        trace_id: TraceId,
        span_id: Option<SpanId>,
    ) -> TraceMetric {
        attributes.insert(POINT_ID, self.point_id.clone());
        attributes.insert(ROW_KIND, row_kind);
        attributes.insert(SOURCE, "otlp");
        if self.start_time_unix_nano > 0
            && let Ok(start) = i64::try_from(self.start_time_unix_nano)
        {
            attributes.insert(START_TIMESTAMP_PRECISE, start);
        }

        TraceMetric {
            timestamp: Annotated::new(Timestamp(timestamp)),
            trace_id: Annotated::new(trace_id),
            span_id: Annotated::from(span_id),
            name: Annotated::new(self.info.name.clone()),
            ty: Annotated::new(self.ty.clone()),
            unit: Annotated::from(self.info.unit),
            value: Annotated::new(Value::F64(value)),
            attributes: Annotated::new(attributes),
            other: Default::default(),
        }
    }

    /// Creates a `point` row for counters and gauges.
    fn point_row(&self, value: f64) -> TraceMetric {
        self.row(
            "point",
            value,
            self.attributes.clone(),
            self.timestamp,
            self.trace_id,
            None,
        )
    }

    /// Creates an `exemplar` row, returns `None` if the exemplar has no valid value.
    fn exemplar_row(&self, exemplar: Exemplar) -> Option<TraceMetric> {
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

        let trace_id = match trace_id.is_empty() {
            true => self.trace_id,
            false => TraceId::try_from(trace_id.as_slice()).unwrap_or(self.trace_id),
        };
        let span_id = match span_id.is_empty() {
            true => None,
            false => SpanId::try_from(span_id.as_slice()).ok(),
        };

        let mut attributes = self.attributes.clone();
        insert_otel_attributes(&mut attributes, filtered_attributes);

        Some(self.row(
            "exemplar",
            value,
            attributes,
            nanos_to_utc(time_unix_nano).unwrap_or(self.timestamp),
            trace_id,
            span_id,
        ))
    }

    fn emit_exemplars<F>(&self, exemplars: Vec<Exemplar>, produce: &mut F)
    where
        F: FnMut(TraceMetric),
    {
        for exemplar in exemplars {
            if let Some(row) = self.exemplar_row(exemplar) {
                produce(row);
            }
        }
    }
}

fn convert_number_point<F>(
    info: &MetricInfo<'_>,
    ty: MetricType,
    point: NumberDataPoint,
    produce: &mut F,
) where
    F: FnMut(TraceMetric),
{
    let NumberDataPoint {
        attributes,
        start_time_unix_nano,
        time_unix_nano,
        exemplars,
        flags: _,
        value,
    } = point;

    let value = match value {
        Some(number_data_point::Value::AsDouble(value)) => value,
        Some(number_data_point::Value::AsInt(value)) => value as f64,
        None => {
            relay_log::debug!(metric = info.name, "dropping OTLP data point without value");
            return;
        }
    };
    if !value.is_finite() {
        relay_log::debug!(metric = info.name, "dropping non-finite OTLP data point");
        return;
    }

    let point = Point::new(info, ty, attributes, time_unix_nano, start_time_unix_nano);
    produce(point.point_row(value));
    point.emit_exemplars(exemplars, produce);
}

/// A single histogram bucket, bounds are inclusive.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Bucket {
    lower: f64,
    upper: f64,
    count: u64,
}

/// Summary statistics of a histogram data point.
#[derive(Debug, Clone, Copy)]
struct HistogramStats {
    count: u64,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
}

impl HistogramStats {
    /// Returns `true` if the statistics are usable.
    fn is_valid(&self) -> bool {
        self.count > 0
            && [self.sum, self.min, self.max]
                .into_iter()
                .flatten()
                .all(f64::is_finite)
    }

    /// Clamps the lower bound of the lowest bucket to the min of the histogram.
    ///
    /// The bound is only changed if min lies within the bucket.
    fn clamp_lower(&self, bucket: &mut Bucket) {
        if let Some(min) = self.min
            && min > bucket.lower
            && min <= bucket.upper
        {
            bucket.lower = min;
        }
    }

    /// Clamps the upper bound of the highest bucket to the max of the histogram.
    ///
    /// The bound is only changed if max lies within the bucket.
    fn clamp_upper(&self, bucket: &mut Bucket) {
        if let Some(max) = self.max
            && max < bucket.upper
            && max >= bucket.lower
        {
            bucket.upper = max;
        }
    }

    /// A single bucket for a histogram without any bucket information.
    fn fallback_bucket(&self) -> Option<Bucket> {
        let avg = self.sum.map(|sum| sum / self.count as f64);
        Some(Bucket {
            lower: self.min.or(avg)?,
            upper: self.max.or(avg)?,
            count: self.count,
        })
    }
}

fn convert_histogram_point<F>(info: &MetricInfo<'_>, point: HistogramDataPoint, produce: &mut F)
where
    F: FnMut(TraceMetric),
{
    let HistogramDataPoint {
        attributes,
        start_time_unix_nano,
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

    let stats = HistogramStats {
        count,
        sum,
        min,
        max,
    };
    if !stats.is_valid() {
        relay_log::debug!(metric = info.name, "dropping invalid OTLP histogram point");
        return;
    }

    let Some(buckets) = explicit_buckets(&stats, &bucket_counts, &explicit_bounds) else {
        relay_log::debug!(
            metric = info.name,
            "dropping OTLP histogram point without buckets"
        );
        return;
    };

    let point = Point::new(
        info,
        MetricType::Histogram,
        attributes,
        time_unix_nano,
        start_time_unix_nano,
    );
    emit_buckets(&point, &stats, buckets, produce);
    point.emit_exemplars(exemplars, produce);
}

/// Converts explicit histogram buckets into non-empty [`Bucket`]s.
fn explicit_buckets(stats: &HistogramStats, counts: &[u64], bounds: &[f64]) -> Option<Vec<Bucket>> {
    if bounds.is_empty() || counts.len() != bounds.len() + 1 {
        return stats.fallback_bucket().map(|bucket| vec![bucket]);
    }

    let last = counts.len() - 1;
    let buckets = counts
        .iter()
        .enumerate()
        .filter(|(_, count)| **count > 0)
        .map(|(i, &count)| {
            let lower = if i == 0 {
                f64::NEG_INFINITY
            } else {
                bounds[i - 1]
            };
            let upper = if i == last { f64::INFINITY } else { bounds[i] };
            let mut bucket = Bucket {
                lower,
                upper,
                count,
            };
            if i == 0 {
                stats.clamp_lower(&mut bucket);
            }
            if i == last {
                stats.clamp_upper(&mut bucket);
            }

            // Without (valid) min and max, the open ended buckets collapse onto their only bound.
            if bucket.lower == f64::NEG_INFINITY {
                bucket.lower = bounds[0];
            }
            if bucket.upper == f64::INFINITY {
                bucket.upper = bounds[last - 1];
            }
            bucket
        })
        .collect();

    Some(buckets)
}

fn convert_exponential_histogram_point<F>(
    info: &MetricInfo<'_>,
    point: ExponentialHistogramDataPoint,
    produce: &mut F,
) where
    F: FnMut(TraceMetric),
{
    let ExponentialHistogramDataPoint {
        attributes,
        start_time_unix_nano,
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

    let stats = HistogramStats {
        count,
        sum,
        min,
        max,
    };
    if !stats.is_valid() {
        relay_log::debug!(metric = info.name, "dropping invalid OTLP histogram point");
        return;
    }

    let mut buckets = exponential_buckets(
        &stats,
        scale,
        zero_count,
        zero_threshold,
        positive.as_ref(),
        negative.as_ref(),
    );
    if buckets.is_empty() {
        buckets.extend(stats.fallback_bucket());
    }
    if buckets.is_empty() {
        relay_log::debug!(
            metric = info.name,
            "dropping OTLP histogram point without buckets"
        );
        return;
    }

    let point = Point::new(
        info,
        MetricType::Histogram,
        attributes,
        time_unix_nano,
        start_time_unix_nano,
    );
    emit_buckets(&point, &stats, buckets, produce);
    point.emit_exemplars(exemplars, produce);
}

/// Converts exponential histogram buckets into non-empty [`Bucket`]s, ordered by their bounds.
fn exponential_buckets(
    stats: &HistogramStats,
    scale: i32,
    zero_count: u64,
    zero_threshold: f64,
    positive: Option<&Buckets>,
    negative: Option<&Buckets>,
) -> Vec<Bucket> {
    let mut buckets = Vec::new();

    if let Some(negative) = negative {
        let (scale, counts) = downscale(scale, collect_exponential_buckets(negative));
        // Highest index first, the highest index is the most negative bucket.
        for (&index, &count) in counts.iter().rev() {
            buckets.push(Bucket {
                lower: -exponential_bound(scale, index + 1),
                upper: -exponential_bound(scale, index),
                count,
            });
        }
    }

    if zero_count > 0 {
        let zero_threshold = match zero_threshold.is_finite() {
            true => zero_threshold.abs(),
            false => 0.0,
        };
        buckets.push(Bucket {
            lower: -zero_threshold,
            upper: zero_threshold,
            count: zero_count,
        });
    }

    if let Some(positive) = positive {
        let (scale, counts) = downscale(scale, collect_exponential_buckets(positive));
        for (&index, &count) in counts.iter() {
            buckets.push(Bucket {
                lower: exponential_bound(scale, index),
                upper: exponential_bound(scale, index + 1),
                count,
            });
        }
    }

    if let Some(first) = buckets.first_mut() {
        stats.clamp_lower(first);
    }
    if let Some(last) = buckets.last_mut() {
        stats.clamp_upper(last);
    }

    buckets
}

/// Collects all non-empty exponential buckets by their absolute index.
fn collect_exponential_buckets(buckets: &Buckets) -> BTreeMap<i64, u64> {
    let mut result = BTreeMap::new();
    for (i, &count) in buckets.bucket_counts.iter().enumerate() {
        if count > 0 {
            let index = i64::from(buckets.offset) + i as i64;
            *result.entry(index).or_default() += count;
        }
    }
    result
}

/// Reduces the scale until there are at most [`MAX_EXPONENTIAL_BUCKETS`] buckets.
fn downscale(mut scale: i32, mut counts: BTreeMap<i64, u64>) -> (i32, BTreeMap<i64, u64>) {
    while counts.len() > MAX_EXPONENTIAL_BUCKETS {
        scale -= 1;
        let mut downscaled = BTreeMap::new();
        for (index, count) in counts {
            *downscaled.entry(index >> 1).or_default() += count;
        }
        counts = downscaled;
    }
    (scale, counts)
}

/// Returns `base^index` with `base = 2^(2^-scale)`.
fn exponential_bound(scale: i32, index: i64) -> f64 {
    (index as f64 * (-f64::from(scale)).exp2()).exp2()
}

/// Emits one `bucket` row for every bucket of a histogram point.
fn emit_buckets<F>(point: &Point<'_>, stats: &HistogramStats, buckets: Vec<Bucket>, produce: &mut F)
where
    F: FnMut(TraceMetric),
{
    let mids: Vec<f64> = buckets.iter().map(|b| (b.lower + b.upper) / 2.0).collect();
    if buckets
        .iter()
        .zip(&mids)
        .any(|(b, mid)| !b.lower.is_finite() || !b.upper.is_finite() || !mid.is_finite())
    {
        relay_log::debug!(
            metric = point.info.name,
            "dropping OTLP histogram point with non-finite buckets"
        );
        return;
    }

    let total_count: u64 = buckets.iter().map(|b| b.count).sum();
    let weighted_mids: f64 = buckets
        .iter()
        .zip(&mids)
        .map(|(b, mid)| b.count as f64 * mid)
        .sum();

    for (bucket, mid) in buckets.into_iter().zip(mids) {
        let count = bucket.count as f64;
        let sum_share = match stats.sum {
            Some(sum) if weighted_mids != 0.0 && weighted_mids.is_finite() => {
                count * mid * (sum / weighted_mids)
            }
            Some(sum) => sum * count / total_count as f64,
            None => count * mid,
        };

        let mut attributes = point.attributes.clone();
        attributes.insert(BUCKET_LOWER, bucket.lower);
        attributes.insert(BUCKET_UPPER, bucket.upper);
        attributes.insert(BUCKET_COUNT, saturating_i64(bucket.count));
        attributes.insert(SUM_SHARE, sum_share);
        attributes.insert(HISTOGRAM_COUNT, saturating_i64(stats.count));
        match stats.sum {
            Some(sum) => attributes.insert(HISTOGRAM_SUM, sum),
            None => attributes.insert(HISTOGRAM_SUM_MISSING, true),
        }
        if let Some(min) = stats.min {
            attributes.insert(HISTOGRAM_MIN, min);
        }
        if let Some(max) = stats.max {
            attributes.insert(HISTOGRAM_MAX, max);
        }

        produce(point.row(
            "bucket",
            mid,
            attributes,
            point.timestamp,
            point.trace_id,
            None,
        ));
    }
}

fn insert_otel_attributes(attributes: &mut Attributes, otel_attributes: Vec<KeyValue>) {
    for attribute in otel_attributes {
        if let Some(attr) = attribute
            .value
            .and_then(|v| v.value)
            .and_then(relay_otel::otel_value_to_attribute)
        {
            attributes.0.insert(attribute.key, Annotated::new(attr));
        }
    }
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn nanos_to_utc(ts: u64) -> Option<DateTime<Utc>> {
    i64::try_from(ts)
        .ok()
        .filter(|&ts| ts > 0)
        .map(DateTime::from_timestamp_nanos)
}

/// Normalization of OTLP/JSON metrics payloads.
///
/// The JSON (de-)serialization of the `opentelemetry-proto` crate is inconsistent for metrics:
/// some 64 bit integer fields require the (spec compliant) string encoding, some require a number
/// and some messages do not tolerate missing fields. This module rewrites a JSON payload into the
/// shape the crate expects, accepting both strings and numbers for all 64 bit integers.
mod json {
    use serde_json::{Map, Value};

    #[derive(Clone, Copy)]
    enum PointKind {
        Number,
        Histogram,
        Exponential,
    }

    pub fn normalize_metrics_data(value: &mut Value) {
        let Some(data) = value.as_object_mut() else {
            return;
        };
        default(data, "resourceMetrics", Value::Array(Vec::new()));

        for resource_metrics in array_mut(value, "resourceMetrics") {
            for scope_metrics in array_mut(resource_metrics, "scopeMetrics") {
                for metric in array_mut(scope_metrics, "metrics") {
                    normalize_metric(metric);
                }
            }
        }
    }

    fn normalize_metric(metric: &mut Value) {
        let Some(metric) = metric.as_object_mut() else {
            return;
        };

        // Summaries are not supported, removing them avoids parsing issues.
        metric.remove("summary");

        for (key, kind) in [
            ("gauge", PointKind::Number),
            ("sum", PointKind::Number),
            ("histogram", PointKind::Histogram),
            ("exponentialHistogram", PointKind::Exponential),
        ] {
            let Some(data) = metric.get_mut(key) else {
                continue;
            };

            if let Some(temporality) = data.get_mut("aggregationTemporality")
                && let Some(s) = temporality.as_str()
            {
                *temporality = Value::from(match s {
                    "AGGREGATION_TEMPORALITY_DELTA" => 1,
                    "AGGREGATION_TEMPORALITY_CUMULATIVE" => 2,
                    s => s.parse::<i32>().unwrap_or(0),
                });
            }

            for point in array_mut(data, "dataPoints") {
                normalize_point(point, kind);
            }
        }
    }

    fn normalize_point(point: &mut Value, kind: PointKind) {
        let Some(obj) = point.as_object_mut() else {
            return;
        };

        match kind {
            PointKind::Number | PointKind::Histogram => {
                to_string(obj, "timeUnixNano");
                to_string(obj, "startTimeUnixNano");
            }
            PointKind::Exponential => {
                for key in [
                    "timeUnixNano",
                    "startTimeUnixNano",
                    "count",
                    "zeroCount",
                    "scale",
                    "flags",
                    "zeroThreshold",
                ] {
                    default(obj, key, 0.into());
                }
                default(obj, "attributes", Value::Array(Vec::new()));
                default(obj, "exemplars", Value::Array(Vec::new()));
                to_number(obj, "timeUnixNano");
                to_number(obj, "startTimeUnixNano");

                for side in ["positive", "negative"] {
                    if let Some(buckets) = obj.get_mut(side).and_then(Value::as_object_mut) {
                        default(buckets, "offset", 0.into());
                        default(buckets, "bucketCounts", Value::Array(Vec::new()));
                        to_numbers(buckets, "bucketCounts");
                    }
                }
            }
        }

        to_number(obj, "asInt");
        to_number(obj, "count");
        to_number(obj, "zeroCount");
        to_numbers(obj, "bucketCounts");

        for exemplar in array_mut(point, "exemplars") {
            normalize_exemplar(exemplar);
        }
    }

    fn normalize_exemplar(exemplar: &mut Value) {
        let Some(obj) = exemplar.as_object_mut() else {
            return;
        };

        default(obj, "filteredAttributes", Value::Array(Vec::new()));
        default(obj, "timeUnixNano", 0.into());
        default(obj, "spanId", "".into());
        default(obj, "traceId", "".into());
        to_number(obj, "timeUnixNano");

        // The crate expects the value oneof nested in a `value` object, unlike for data points.
        for key in ["asDouble", "asInt"] {
            if let Some(value) = obj.remove(key) {
                let mut nested = Map::new();
                nested.insert(key.to_owned(), value);
                to_number(&mut nested, "asInt");
                obj.insert("value".to_owned(), Value::Object(nested));
            }
        }
    }

    fn array_mut<'a>(value: &'a mut Value, key: &str) -> impl Iterator<Item = &'a mut Value> {
        value
            .get_mut(key)
            .and_then(Value::as_array_mut)
            .into_iter()
            .flatten()
    }

    fn default(obj: &mut Map<String, Value>, key: &str, value: Value) {
        match obj.get(key) {
            None | Some(Value::Null) => {
                obj.insert(key.to_owned(), value);
            }
            Some(_) => {}
        }
    }

    fn to_string(obj: &mut Map<String, Value>, key: &str) {
        if let Some(value) = obj.get_mut(key)
            && let Value::Number(n) = value
        {
            *value = Value::String(n.to_string());
        }
    }

    fn to_number(obj: &mut Map<String, Value>, key: &str) {
        if let Some(value) = obj.get_mut(key) {
            convert_to_number(value);
        }
    }

    fn to_numbers(obj: &mut Map<String, Value>, key: &str) {
        if let Some(values) = obj.get_mut(key).and_then(Value::as_array_mut) {
            values.iter_mut().for_each(convert_to_number);
        }
    }

    fn convert_to_number(value: &mut Value) {
        let Value::String(s) = value else {
            return;
        };
        if let Ok(n) = s.parse::<u64>() {
            *value = n.into();
        } else if let Ok(n) = s.parse::<i64>() {
            *value = n.into();
        }
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
    use opentelemetry_proto::tonic::metrics::v1::{
        ExponentialHistogram, Gauge, Histogram, ResourceMetrics, ScopeMetrics, Sum,
    };

    use super::*;
    use crate::processing::trace_metrics::process::normalize_trace_metric_derived;
    use crate::processing::trace_metrics::validate::validate_trace_metric;

    const TIME: u64 = 1_700_000_000_000_000_000;
    const START: u64 = 1_699_999_990_000_000_000;

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_owned())),
            }),
        }
    }

    fn metrics_data(metrics: Vec<Metric>) -> MetricsData {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "checkout")],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "my.meter".to_owned(),
                        version: "1.0".to_owned(),
                        ..Default::default()
                    }),
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn metric(name: &str, unit: &str, data: metric::Data) -> Metric {
        Metric {
            name: name.to_owned(),
            unit: unit.to_owned(),
            data: Some(data),
            ..Default::default()
        }
    }

    fn convert(metrics: Vec<Metric>) -> Vec<TraceMetric> {
        let mut rows = Vec::new();
        convert_metrics_data(metrics_data(metrics), &mut |m| rows.push(m));

        // All rows must survive validation and schema normalization of the trace metric pipeline.
        for row in &rows {
            let mut annotated = Annotated::new(row.clone());
            validate_trace_metric(&annotated, "test").unwrap();
            normalize_trace_metric_derived(&mut annotated).unwrap();
            assert_eq!(annotated.value(), Some(row));
        }

        rows
    }

    fn attr<'a>(row: &'a TraceMetric, key: &str) -> Option<&'a Value> {
        row.attributes.value()?.get_value(key)
    }

    fn attr_str<'a>(row: &'a TraceMetric, key: &str) -> &'a str {
        match attr(row, key) {
            Some(Value::String(s)) => s,
            other => panic!("expected string attribute {key}, got {other:?}"),
        }
    }

    fn attr_f64(row: &TraceMetric, key: &str) -> f64 {
        match attr(row, key) {
            Some(Value::F64(f)) => *f,
            other => panic!("expected double attribute {key}, got {other:?}"),
        }
    }

    fn attr_i64(row: &TraceMetric, key: &str) -> i64 {
        match attr(row, key) {
            Some(Value::I64(i)) => *i,
            other => panic!("expected integer attribute {key}, got {other:?}"),
        }
    }

    fn value(row: &TraceMetric) -> f64 {
        match row.value.value() {
            Some(Value::F64(f)) => *f,
            other => panic!("expected double value, got {other:?}"),
        }
    }

    fn assert_close(a: f64, b: f64) {
        assert!(
            (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0),
            "{a} != {b}"
        );
    }

    fn exemplar(value: f64, trace_id: &[u8], span_id: &[u8], attrs: Vec<KeyValue>) -> Exemplar {
        Exemplar {
            filtered_attributes: attrs,
            time_unix_nano: TIME - 1_000,
            span_id: span_id.to_vec(),
            trace_id: trace_id.to_vec(),
            value: Some(exemplar::Value::AsDouble(value)),
        }
    }

    const TRACE_ID: [u8; 16] = [
        0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f, 0xc6,
        0x0c,
    ];
    const SPAN_ID: [u8; 8] = [0xee, 0xe1, 0x9b, 0x7e, 0xc3, 0xc1, 0xb1, 0x74];

    #[test]
    fn test_explicit_histogram_with_exemplars() {
        let point = HistogramDataPoint {
            attributes: vec![kv("http.route", "/checkout"), kv("shared", "point")],
            start_time_unix_nano: START,
            time_unix_nano: TIME,
            count: 10000,
            sum: Some(1569400.0),
            // Buckets: (-inf, 5], (5, 2500], (2500, 5000], (5000, +inf)
            bucket_counts: vec![9400, 0, 600, 0],
            explicit_bounds: vec![5.0, 2500.0, 5000.0],
            exemplars: vec![
                exemplar(3.0, &TRACE_ID, &SPAN_ID, vec![kv("shared", "exemplar")]),
                exemplar(4000.0, &[], &[], vec![]),
            ],
            flags: 0,
            min: Some(0.4),
            max: Some(4870.0),
        };

        let rows = convert(vec![metric(
            "http.server.duration",
            "ms",
            metric::Data::Histogram(Histogram {
                data_points: vec![point],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            }),
        )]);

        assert_eq!(rows.len(), 4);
        let kinds: Vec<_> = rows.iter().map(|r| attr_str(r, ROW_KIND)).collect();
        assert_eq!(kinds, ["bucket", "bucket", "exemplar", "exemplar"]);

        // Shared fields.
        let point_id = attr_str(&rows[0], POINT_ID);
        assert_eq!(point_id.len(), 32);
        for row in &rows {
            assert_eq!(attr_str(row, POINT_ID), point_id);
            assert_eq!(attr_str(row, SOURCE), "otlp");
            assert_eq!(attr_i64(row, START_TIMESTAMP_PRECISE), START as i64);
            assert_eq!(row.name.as_str(), Some("http.server.duration"));
            assert_eq!(row.ty.value(), Some(&MetricType::Histogram));
            assert_eq!(
                row.unit.value().map(|u| u.to_string()).as_deref(),
                Some("millisecond")
            );
            assert_eq!(attr_str(row, "http.route"), "/checkout");
            assert_eq!(attr_str(row, "resource.service.name"), "checkout");
            assert_eq!(attr_str(row, "instrumentation.name"), "my.meter");
            assert_eq!(attr_str(row, "sentry.origin"), "auto.otlp.metrics");
        }

        // Bucket rows share the point trace id, derived from the point id.
        let point_trace_id = rows[0].trace_id.value().unwrap().to_string();
        assert_eq!(point_trace_id, point_id);
        assert_eq!(
            rows[1].trace_id.value().unwrap().to_string(),
            point_trace_id
        );

        let (b0, b1) = (&rows[0], &rows[1]);
        assert_eq!(attr_f64(b0, BUCKET_LOWER), 0.4);
        assert_eq!(attr_f64(b0, BUCKET_UPPER), 5.0);
        assert_eq!(attr_i64(b0, BUCKET_COUNT), 9400);
        assert_eq!(attr_f64(b1, BUCKET_LOWER), 2500.0);
        assert_eq!(attr_f64(b1, BUCKET_UPPER), 5000.0);
        assert_eq!(attr_i64(b1, BUCKET_COUNT), 600);

        // Value is the bucket midpoint.
        assert_eq!(value(b0), 2.7);
        assert_eq!(value(b1), 3750.0);

        // Sum shares are weighted by count * mid and add up to the sum.
        let weighted = 9400.0 * 2.7 + 600.0 * 3750.0;
        assert_close(attr_f64(b0, SUM_SHARE), 9400.0 * 2.7 * 1569400.0 / weighted);
        assert_close(
            attr_f64(b1, SUM_SHARE),
            600.0 * 3750.0 * 1569400.0 / weighted,
        );
        assert_close(attr_f64(b0, SUM_SHARE) + attr_f64(b1, SUM_SHARE), 1569400.0);

        for bucket in [b0, b1] {
            assert_eq!(attr_i64(bucket, HISTOGRAM_COUNT), 10000);
            assert_eq!(attr_f64(bucket, HISTOGRAM_SUM), 1569400.0);
            assert_eq!(attr_f64(bucket, HISTOGRAM_MIN), 0.4);
            assert_eq!(attr_f64(bucket, HISTOGRAM_MAX), 4870.0);
            assert!(attr(bucket, HISTOGRAM_SUM_MISSING).is_none());
            assert_eq!(attr_str(bucket, "shared"), "point");
            assert_eq!(
                bucket.timestamp.value().unwrap().0,
                nanos_to_utc(TIME).unwrap()
            );
        }

        // Exemplars.
        let (e0, e1) = (&rows[2], &rows[3]);
        assert_eq!(value(e0), 3.0);
        assert_eq!(
            e0.trace_id.value().unwrap().to_string(),
            "5b8efff798038103d269b633813fc60c"
        );
        assert_eq!(e0.span_id.value().unwrap().to_string(), "eee19b7ec3c1b174");
        assert_eq!(attr_str(e0, "shared"), "exemplar");
        assert_eq!(
            e0.timestamp.value().unwrap().0,
            nanos_to_utc(TIME - 1_000).unwrap()
        );

        assert_eq!(value(e1), 4000.0);
        assert_eq!(e1.trace_id.value().unwrap().to_string(), point_trace_id);
        assert!(e1.span_id.value().is_none());

        for exemplar in [e0, e1] {
            assert!(attr(exemplar, BUCKET_LOWER).is_none());
            assert!(attr(exemplar, BUCKET_UPPER).is_none());
            assert!(attr(exemplar, BUCKET_COUNT).is_none());
            assert!(attr(exemplar, SUM_SHARE).is_none());
            assert!(attr(exemplar, HISTOGRAM_COUNT).is_none());
        }
    }

    #[test]
    fn test_explicit_histogram_without_sum() {
        let point = HistogramDataPoint {
            time_unix_nano: TIME,
            count: 3,
            bucket_counts: vec![1, 2, 0],
            explicit_bounds: vec![10.0, 20.0],
            ..Default::default()
        };

        let rows = convert(vec![metric(
            "queue.size",
            "1",
            metric::Data::Histogram(Histogram {
                data_points: vec![point],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            }),
        )]);

        assert_eq!(rows.len(), 2);
        // Without min, the first bucket collapses onto its upper bound.
        assert_eq!(attr_f64(&rows[0], BUCKET_LOWER), 10.0);
        assert_eq!(attr_f64(&rows[0], BUCKET_UPPER), 10.0);
        assert_eq!(attr_f64(&rows[0], SUM_SHARE), 10.0);
        assert_eq!(attr_f64(&rows[1], BUCKET_LOWER), 10.0);
        assert_eq!(attr_f64(&rows[1], BUCKET_UPPER), 20.0);
        assert_eq!(attr_f64(&rows[1], SUM_SHARE), 30.0);
        assert_eq!(value(&rows[1]), 15.0);
        for row in &rows {
            assert_eq!(attr(row, HISTOGRAM_SUM_MISSING), Some(&Value::Bool(true)));
            assert!(attr(row, HISTOGRAM_SUM).is_none());
            assert!(attr(row, START_TIMESTAMP_PRECISE).is_none());
            assert_eq!(row.unit.value(), Some(&MetricUnit::None));
        }
    }

    #[test]
    fn test_histogram_without_buckets() {
        let point = HistogramDataPoint {
            time_unix_nano: TIME,
            count: 4,
            sum: Some(20.0),
            ..Default::default()
        };

        let rows = convert(vec![metric(
            "no.buckets",
            "",
            metric::Data::Histogram(Histogram {
                data_points: vec![point],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            }),
        )]);

        assert_eq!(rows.len(), 1);
        assert_eq!(attr_f64(&rows[0], BUCKET_LOWER), 5.0);
        assert_eq!(attr_f64(&rows[0], BUCKET_UPPER), 5.0);
        assert_eq!(attr_i64(&rows[0], BUCKET_COUNT), 4);
        assert_eq!(attr_f64(&rows[0], SUM_SHARE), 20.0);
    }

    #[test]
    fn test_sum_temporality() {
        let point = || NumberDataPoint {
            attributes: vec![kv("status", "ok")],
            start_time_unix_nano: START,
            time_unix_nano: TIME,
            value: Some(number_data_point::Value::AsInt(42)),
            ..Default::default()
        };
        let sum = |temporality: AggregationTemporality| Sum {
            data_points: vec![point()],
            aggregation_temporality: temporality as i32,
            is_monotonic: true,
        };

        let rows = convert(vec![
            metric(
                "cumulative",
                "",
                metric::Data::Sum(sum(AggregationTemporality::Cumulative)),
            ),
            metric(
                "unspecified",
                "",
                metric::Data::Sum(sum(AggregationTemporality::Unspecified)),
            ),
            metric(
                "requests",
                "",
                metric::Data::Sum(sum(AggregationTemporality::Delta)),
            ),
        ]);

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.name.as_str(), Some("requests"));
        assert_eq!(row.ty.value(), Some(&MetricType::Counter));
        assert_eq!(value(row), 42.0);
        assert_eq!(attr_str(row, ROW_KIND), "point");
        assert_eq!(attr_str(row, SOURCE), "otlp");
        assert_eq!(attr_str(row, "status"), "ok");
        assert_eq!(attr_i64(row, START_TIMESTAMP_PRECISE), START as i64);
        assert_eq!(
            row.trace_id.value().unwrap().to_string(),
            attr_str(row, POINT_ID)
        );
        assert!(attr(row, BUCKET_COUNT).is_none());
    }

    #[test]
    fn test_gauge() {
        let points = vec![
            NumberDataPoint {
                time_unix_nano: TIME,
                value: Some(number_data_point::Value::AsDouble(0.75)),
                ..Default::default()
            },
            NumberDataPoint {
                time_unix_nano: TIME,
                value: Some(number_data_point::Value::AsDouble(f64::NAN)),
                ..Default::default()
            },
            NumberDataPoint {
                time_unix_nano: TIME,
                value: Some(number_data_point::Value::AsDouble(f64::INFINITY)),
                ..Default::default()
            },
        ];

        let rows = convert(vec![metric(
            "cpu.utilization",
            "1",
            metric::Data::Gauge(Gauge {
                data_points: points,
            }),
        )]);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ty.value(), Some(&MetricType::Gauge));
        assert_eq!(value(&rows[0]), 0.75);
        assert_eq!(attr_str(&rows[0], ROW_KIND), "point");
    }

    #[test]
    fn test_histogram_empty_dropped() {
        let point = HistogramDataPoint {
            time_unix_nano: TIME,
            count: 0,
            sum: Some(0.0),
            bucket_counts: vec![0, 0],
            explicit_bounds: vec![1.0],
            exemplars: vec![exemplar(1.0, &[], &[], vec![])],
            ..Default::default()
        };

        let rows = convert(vec![metric(
            "empty",
            "",
            metric::Data::Histogram(Histogram {
                data_points: vec![point],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            }),
        )]);
        assert!(rows.is_empty());
    }

    #[test]
    fn test_exponential_histogram() {
        // scale 0 => base 2.
        let point = ExponentialHistogramDataPoint {
            start_time_unix_nano: START,
            time_unix_nano: TIME,
            count: 10,
            sum: Some(13.0),
            scale: 0,
            zero_count: 2,
            zero_threshold: 0.0,
            // Index 1: [-4, -2), count 3.
            negative: Some(Buckets {
                offset: 1,
                bucket_counts: vec![3],
            }),
            // Index 1: (2, 4], count 1; index 2: (4, 8], count 4.
            positive: Some(Buckets {
                offset: 1,
                bucket_counts: vec![1, 4],
            }),
            min: Some(-3.0),
            max: Some(7.0),
            exemplars: vec![exemplar(5.0, &[], &[], vec![])],
            ..Default::default()
        };

        let rows = convert(vec![metric(
            "payload.size",
            "By",
            metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![point],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            }),
        )]);

        let kinds: Vec<_> = rows.iter().map(|r| attr_str(r, ROW_KIND)).collect();
        assert_eq!(kinds, ["bucket", "bucket", "bucket", "bucket", "exemplar"]);

        let bounds: Vec<_> = rows[..4]
            .iter()
            .map(|r| {
                (
                    attr_f64(r, BUCKET_LOWER),
                    attr_f64(r, BUCKET_UPPER),
                    attr_i64(r, BUCKET_COUNT),
                )
            })
            .collect();
        assert_eq!(
            bounds,
            [
                // Negative bucket, lower clamped to min.
                (-3.0, -2.0, 3),
                // Zero bucket.
                (0.0, 0.0, 2),
                (2.0, 4.0, 1),
                // Last bucket, upper clamped to max.
                (4.0, 7.0, 4),
            ]
        );

        let point_id = attr_str(&rows[0], POINT_ID);
        for row in &rows {
            assert_eq!(attr_str(row, POINT_ID), point_id);
            assert_eq!(row.ty.value(), Some(&MetricType::Histogram));
            assert_eq!(
                row.unit.value().map(|u| u.to_string()).as_deref(),
                Some("byte")
            );
        }

        for row in &rows[..4] {
            let mid = (attr_f64(row, BUCKET_LOWER) + attr_f64(row, BUCKET_UPPER)) / 2.0;
            assert_eq!(value(row), mid);
        }

        let total: f64 = rows[..4].iter().map(|r| attr_f64(r, SUM_SHARE)).sum();
        assert_close(total, 13.0);
    }

    #[test]
    fn test_exponential_histogram_zero_threshold_and_downscale() {
        let point = ExponentialHistogramDataPoint {
            time_unix_nano: TIME,
            count: 201,
            sum: Some(1000.0),
            scale: 3,
            zero_count: 1,
            zero_threshold: 0.5,
            positive: Some(Buckets {
                offset: 0,
                bucket_counts: vec![2; 100],
            }),
            ..Default::default()
        };

        let rows = convert(vec![metric(
            "downscaled",
            "",
            metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![point],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            }),
        )]);

        // Zero bucket.
        assert_eq!(attr_f64(&rows[0], BUCKET_LOWER), -0.5);
        assert_eq!(attr_f64(&rows[0], BUCKET_UPPER), 0.5);
        assert_eq!(attr_i64(&rows[0], BUCKET_COUNT), 1);

        // 100 buckets downscaled once to 50 buckets of 4.
        let buckets = &rows[1..];
        assert_eq!(buckets.len(), 50);
        assert!(buckets.iter().all(|r| attr_i64(r, BUCKET_COUNT) == 4));
        // scale 2 => base 2^(1/4), index 0 covers (1, 2^(1/4)].
        assert_eq!(attr_f64(&buckets[0], BUCKET_LOWER), 1.0);
        assert_close(attr_f64(&buckets[0], BUCKET_UPPER), 2f64.powf(0.25));
        for pair in buckets.windows(2) {
            assert_eq!(
                attr_f64(&pair[0], BUCKET_UPPER),
                attr_f64(&pair[1], BUCKET_LOWER)
            );
        }

        let total: f64 = rows.iter().map(|r| attr_f64(r, SUM_SHARE)).sum();
        assert_close(total, 1000.0);
    }

    #[test]
    fn test_exponential_histogram_symmetric_sum_share() {
        // Symmetric negative and positive buckets, the weighted mids cancel out.
        let point = ExponentialHistogramDataPoint {
            time_unix_nano: TIME,
            count: 2,
            sum: Some(0.5),
            scale: 0,
            negative: Some(Buckets {
                offset: 0,
                bucket_counts: vec![1],
            }),
            positive: Some(Buckets {
                offset: 0,
                bucket_counts: vec![1],
            }),
            ..Default::default()
        };

        let rows = convert(vec![metric(
            "symmetric",
            "",
            metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![point],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            }),
        )]);

        assert_eq!(rows.len(), 2);
        assert_eq!(attr_f64(&rows[0], SUM_SHARE), 0.25);
        assert_eq!(attr_f64(&rows[1], SUM_SHARE), 0.25);
    }

    #[test]
    fn test_unit_mapping_and_summary_dropped() {
        assert_eq!(map_unit("ms"), Some("millisecond".parse().unwrap()));
        assert_eq!(map_unit("s"), Some("second".parse().unwrap()));
        assert_eq!(map_unit("us"), Some("microsecond".parse().unwrap()));
        assert_eq!(map_unit("ns"), Some("nanosecond".parse().unwrap()));
        assert_eq!(map_unit("By"), Some("byte".parse().unwrap()));
        assert_eq!(map_unit("1"), Some(MetricUnit::None));
        assert_eq!(map_unit(""), Some(MetricUnit::None));
        assert_eq!(map_unit("kibibyte"), Some("kibibyte".parse().unwrap()));

        let rows = convert(vec![metric(
            "summary",
            "",
            metric::Data::Summary(Default::default()),
        )]);
        assert!(rows.is_empty());
    }

    #[test]
    fn test_parse_json() {
        let json = r#"{
          "resourceMetrics": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "svc"}}]},
            "scopeMetrics": [{
              "scope": {"name": "meter"},
              "metrics": [
                {
                  "name": "requests",
                  "unit": "1",
                  "sum": {
                    "aggregationTemporality": 1,
                    "isMonotonic": true,
                    "dataPoints": [{"timeUnixNano": "1700000000000000000", "asInt": "5"}]
                  }
                },
                {
                  "name": "latency",
                  "unit": "ms",
                  "histogram": {
                    "aggregationTemporality": "AGGREGATION_TEMPORALITY_DELTA",
                    "dataPoints": [{
                      "startTimeUnixNano": 1699999990000000000,
                      "timeUnixNano": "1700000000000000000",
                      "count": "3",
                      "sum": 30,
                      "bucketCounts": ["1", "2"],
                      "explicitBounds": [10],
                      "min": 5,
                      "max": 15,
                      "exemplars": [{
                        "timeUnixNano": "1700000000000000000",
                        "asDouble": 12,
                        "traceId": "5b8efff798038103d269b633813fc60c",
                        "spanId": "eee19b7ec3c1b174"
                      }]
                    }]
                  }
                },
                {
                  "name": "sizes",
                  "exponentialHistogram": {
                    "aggregationTemporality": 1,
                    "dataPoints": [{
                      "timeUnixNano": "1700000000000000000",
                      "count": "1",
                      "sum": 3,
                      "scale": 0,
                      "positive": {"offset": 1, "bucketCounts": ["1"]}
                    }]
                  }
                },
                {
                  "name": "summary",
                  "summary": {"dataPoints": [{"count": "1", "sum": 1}]}
                }
              ]
            }]
          }]
        }"#;

        let mut rows = Vec::new();
        expand(OtelFormat::Json, json.as_bytes(), |m| rows.push(m)).unwrap();

        let kinds: Vec<_> = rows
            .iter()
            .map(|r| (r.name.as_str().unwrap(), attr_str(r, ROW_KIND)))
            .collect();
        assert_eq!(
            kinds,
            [
                ("requests", "point"),
                ("latency", "bucket"),
                ("latency", "bucket"),
                ("latency", "exemplar"),
                ("sizes", "bucket"),
            ]
        );
        assert_eq!(value(&rows[0]), 5.0);
        assert_eq!(
            attr_i64(&rows[1], START_TIMESTAMP_PRECISE),
            1699999990000000000
        );
        assert_eq!(
            rows[3].trace_id.value().unwrap().to_string(),
            "5b8efff798038103d269b633813fc60c"
        );
        assert_eq!(attr_f64(&rows[4], BUCKET_LOWER), 2.0);
        assert_eq!(attr_f64(&rows[4], BUCKET_UPPER), 4.0);
    }

    #[test]
    fn test_parse_protobuf() {
        let data = metrics_data(vec![metric(
            "requests",
            "",
            metric::Data::Sum(Sum {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: TIME,
                    value: Some(number_data_point::Value::AsDouble(1.5)),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
                is_monotonic: false,
            }),
        )]);

        let mut rows = Vec::new();
        expand(OtelFormat::Protobuf, &data.encode_to_vec(), |m| {
            rows.push(m)
        })
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(value(&rows[0]), 1.5);

        assert!(expand(OtelFormat::Protobuf, b"\xff\xff", |_| ()).is_err());
        assert!(expand(OtelFormat::Json, b"{", |_| ()).is_err());
    }

    #[cfg(feature = "processing")]
    #[test]
    fn test_store_conversion() {
        use relay_base_schema::organization::OrganizationId;
        use relay_base_schema::project::ProjectId;
        use relay_quotas::Scoping;
        use sentry_protos::snuba::v1::any_value;

        use crate::envelope::WithHeader;
        use crate::processing::trace_metrics::store;

        let point = HistogramDataPoint {
            start_time_unix_nano: START,
            time_unix_nano: TIME,
            count: 2,
            sum: Some(3.0),
            bucket_counts: vec![1, 1],
            explicit_bounds: vec![1.0],
            min: Some(0.5),
            max: Some(2.5),
            ..Default::default()
        };
        let rows = convert(vec![metric(
            "latency",
            "ms",
            metric::Data::Histogram(Histogram {
                data_points: vec![point],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            }),
        )]);

        let ctx = store::Context {
            received_at: nanos_to_utc(TIME).unwrap(),
            scoping: Scoping {
                organization_id: OrganizationId::new(1),
                project_id: ProjectId::new(42),
                project_key: "a94ae32be2584e0bbd7a4cbb95971fee".parse().unwrap(),
                key_id: Some(3),
            },
            retention: crate::processing::Retention {
                standard: 90,
                downsampled: 90,
            },
        };

        let item = store::convert(
            WithHeader {
                header: Some(relay_event_schema::protocol::TraceMetricHeader {
                    byte_size: Some(1),
                    other: Default::default(),
                }),
                value: Annotated::new(rows[0].clone()),
            },
            &ctx,
        )
        .unwrap()
        .trace_item;

        let get = |key: &str| {
            item.attributes
                .get(key)
                .and_then(|v| v.value.clone())
                .unwrap_or_else(|| panic!("missing attribute {key}"))
        };

        assert_eq!(item.trace_id, attr_str(&rows[0], POINT_ID));
        assert_eq!(
            get("sentry.metric_type"),
            any_value::Value::StringValue("histogram".to_owned())
        );
        assert_eq!(
            get("sentry.metric_name"),
            any_value::Value::StringValue("latency".to_owned())
        );
        assert_eq!(
            get("sentry.metric_unit"),
            any_value::Value::StringValue("millisecond".to_owned())
        );
        assert_eq!(get("sentry.value"), any_value::Value::DoubleValue(0.75));
        assert_eq!(
            get(ROW_KIND),
            any_value::Value::StringValue("bucket".to_owned())
        );
        assert_eq!(
            get(SOURCE),
            any_value::Value::StringValue("otlp".to_owned())
        );
        assert_eq!(get(BUCKET_LOWER), any_value::Value::DoubleValue(0.5));
        assert_eq!(get(BUCKET_UPPER), any_value::Value::DoubleValue(1.0));
        assert_eq!(get(BUCKET_COUNT), any_value::Value::IntValue(1));
        assert_eq!(get(HISTOGRAM_COUNT), any_value::Value::IntValue(2));
        assert_eq!(
            get(START_TIMESTAMP_PRECISE),
            any_value::Value::IntValue(START as i64)
        );
        assert!(matches!(get(SUM_SHARE), any_value::Value::DoubleValue(_)));
    }
}
