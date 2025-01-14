//! Aggregating metrics wrapper ported from
//! <https://github.com/getsentry/symbolicator/blob/2fc68b6dfde7bc3dcd68b3b466c6673cf0b525fd/crates/symbolicator-service/src/metrics.rs.

use std::fmt::Write;
use std::sync::{Arc, Mutex};

use cadence::{MetricSink, QueuingMetricSink, StatsdClient};
use crossbeam_utils::CachePadded;
use rustc_hash::FxHashMap;
use thread_local::ThreadLocal;

/// The globally configured Metrics, including a `cadence` client, and a local aggregator.
#[derive(Debug)]
pub struct MetricsWrapper {
    /// The raw `cadence` client.
    statsd_client: StatsdClient,

    /// A thread local aggregator.
    local_aggregator: LocalAggregators,
}

impl MetricsWrapper {
    /// Invokes the provided callback with a mutable reference to a thread-local [`LocalAggregator`].
    fn with_local_aggregator(&self, f: impl FnOnce(&mut LocalAggregator)) {
        let mut local_aggregator = self
            .local_aggregator
            .get_or(Default::default)
            .lock()
            .unwrap();
        f(&mut local_aggregator)
    }
}

#[derive(Default, Debug)]
pub struct LocalAggregator {
    /// A mutable scratch-buffer that is reused to format tags into it.
    buf: String,
    /// A map of all the `counter` and `gauge` metrics we have aggregated thus far.
    aggregated_counters: AggregatedCounters,
    /// A map of all the `timer` and `histogram` metrics we have aggregated thus far.
    aggregated_distributions: AggregatedDistributions,
}

impl LocalAggregator {
    /// Formats the `tags` into a `statsd` like format with the help of our scratch buffer.
    fn format_tags(&mut self, tags: &[(&str, &str)]) -> Option<Box<str>> {
        if tags.is_empty() {
            return None;
        }

        // to avoid reallocation, just reserve some space.
        // the size is rather arbitrary, but should be large enough for reasonable tags.
        self.buf.reserve(128);
        for (key, value) in tags {
            if !self.buf.is_empty() {
                self.buf.push(',');
            }
            let _ = write!(&mut self.buf, "{key}:{value}");
        }
        let formatted_tags = self.buf.as_str().into();
        self.buf.clear();

        Some(formatted_tags)
    }

    /// Emit a `count` metric, which is aggregated by summing up all values.
    pub fn emit_count(&mut self, name: &'static str, value: i64, tags: &[(&'static str, &str)]) {
        let tags = self.format_tags(tags);

        let key = AggregationKey {
            ty: "|c",
            name,
            tags,
        };

        let aggregation = self.aggregated_counters.entry(key).or_default();
        *aggregation += value;
    }

    /// Emit a `gauge` metric, for which only the latest value is retained.
    pub fn emit_gauge(&mut self, name: &'static str, value: u64, tags: &[(&'static str, &str)]) {
        let tags = self.format_tags(tags);

        let key = AggregationKey {
            // TODO: maybe we want to give gauges their own aggregations?
            ty: "|g",
            name,
            tags,
        };

        let aggregation = self.aggregated_counters.entry(key).or_default();
        *aggregation = value as i64;
    }

    /// Emit a `timer` metric, for which every value is accumulated
    pub fn emit_timer(&mut self, name: &'static str, value: f64, tags: &[(&'static str, &str)]) {
        let tags = self.format_tags(tags);
        self.emit_distribution_inner("|ms", name, value, tags)
    }

    /// Emit a `histogram` metric, for which every value is accumulated
    pub fn emit_histogram(
        &mut self,
        name: &'static str,
        value: f64,
        tags: &[(&'static str, &str)],
    ) {
        let tags = self.format_tags(tags);
        self.emit_distribution_inner("|h", name, value, tags)
    }

    /// Emit a distribution metric, which is aggregated by appending to a list of values.
    fn emit_distribution_inner(
        &mut self,
        ty: &'static str,
        name: &'static str,
        value: f64,
        tags: Option<Box<str>>,
    ) {
        let key = AggregationKey { ty, name, tags };

        let aggregation = self.aggregated_distributions.entry(key).or_default();
        aggregation.push(value);
    }
}

#[derive(Debug, Clone)]
struct Sink(Arc<QueuingMetricSink>);

impl MetricSink for Sink {
    fn emit(&self, metric: &str) -> std::io::Result<usize> {
        self.0.emit(metric)
    }
    fn flush(&self) -> std::io::Result<()> {
        self.0.flush()
    }
}

type LocalAggregators = Arc<ThreadLocal<CachePadded<Mutex<LocalAggregator>>>>;

/// The key by which we group/aggregate metrics.
#[derive(Eq, Ord, PartialEq, PartialOrd, Hash, Debug)]
struct AggregationKey {
    /// The metric type, pre-formatted as a statsd suffix such as `|c`.
    ty: &'static str,
    /// The name of the metric.
    name: &'static str,
    /// The metric tags, pre-formatted as a statsd suffix, excluding the `|#` prefix.
    tags: Option<Box<str>>,
}

type AggregatedCounters = FxHashMap<AggregationKey, i64>;
type AggregatedDistributions = FxHashMap<AggregationKey, Vec<f64>>;
