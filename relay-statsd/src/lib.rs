//! A high-level StatsD metric client built on cadence.
//!
//! ## Defining Metrics
//!
//! In order to use metrics, one needs to first define one of the metric traits on a custom enum.
//! The following types of metrics are available: `counter`, `timer`, `gauge`, `histogram`, and
//! `set`. For explanations on what that means see [Metric Types].
//!
//! The metric traits serve only to provide a type safe metric name. All metric types have exactly
//! the same form, they are different only to ensure that a metric can only be used for the type for
//! which it was defined, (e.g. a counter metric cannot be used as a timer metric). See the traits
//! for more detailed examples.
//!
//! ## Initializing the Client
//!
//! Metrics can be used without initializing a statsd client. In that case, invoking `with_client`
//! or the [`metric!`] macro will become a noop. Only when configured, metrics will actually be
//! collected.
//!
//! To initialize the client use [`init`] to create a default client with known arguments:
//!
//! ```no_run
//! # use std::collections::BTreeMap;
//!
//! relay_statsd::init("myprefix", "localhost:8125", BTreeMap::new());
//! ```
//!
//! ## Macro Usage
//!
//! The recommended way to record metrics is by using the [`metric!`] macro. See the trait docs
//! for more information on how to record each type of metric.
//!
//! ```
//! use relay_statsd::{metric, CounterMetric};
//!
//! struct MyCounter;
//!
//! impl CounterMetric for MyCounter {
//!     fn name(&self) -> &'static str {
//!         "counter"
//!     }
//! }
//!
//! metric!(counter(MyCounter) += 1);
//! ```
//!
//! [Metric Types]: https://github.com/statsd/statsd/blob/master/docs/metric_types.md
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::net::ToSocketAddrs;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use cadence::{BufferedUdpMetricSink, MetricSink, QueuingMetricSink};
use crossbeam_utils::CachePadded;
use rustc_hash::FxHashMap;
use thread_local::ThreadLocal;

mod types;
pub use types::{CounterMetric, GaugeMetric, HistogramMetric, SetMetric, TimerMetric};

static METRICS_CLIENT: OnceLock<MetricsWrapper> = OnceLock::new();

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

/// The globally configured Metrics, including a `cadence` client, and a local aggregator.
#[derive(Debug)]
pub struct MetricsWrapper {
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

/// We are not (yet) aggregating distributions, but keeping every value.
/// To not overwhelm downstream services, we send them in batches instead of all at once.
const DISTRIBUTION_BATCH_SIZE: usize = 1;

/// The interval in which to flush out metrics.
/// NOTE: In particular for timer metrics, we have observed that for some reason, *some* of the timer
/// metrics are getting lost (interestingly enough, not all of them) and are not being aggregated into the `.count`
/// sub-metric collected by `veneur`. Lets just flush a lot more often in order to emit less metrics per-flush.
const SEND_INTERVAL: Duration = Duration::from_millis(125);

/// Creates [`LocalAggregators`] and starts a thread that will periodically
/// send aggregated metrics upstream to the `sink`.
fn make_aggregator(prefix: &str, formatted_global_tags: String, sink: Sink) -> LocalAggregators {
    let local_aggregators = LocalAggregators::default();

    let aggregators = Arc::clone(&local_aggregators);
    let prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{}.", prefix.trim_end_matches('.'))
    };

    let thread_fn = move || {
        // to avoid reallocation, just reserve some space.
        // the size is rather arbitrary, but should be large enough for formatted metrics.
        let mut formatted_metric = String::with_capacity(256);
        let mut suffix = String::with_capacity(128);

        loop {
            thread::sleep(SEND_INTERVAL);

            let LocalAggregator {
                buf: _,
                integers,
                floats,
                distributions,
                sets,
            } = aggregate_all(&aggregators);

            // send all the aggregated "counter like" metrics
            for (AggregationKey { ty, name, tags }, value) in integers {
                formatted_metric.push_str(&prefix);
                formatted_metric.push_str(name);

                let _ = write!(&mut formatted_metric, ":{value}{ty}{formatted_global_tags}");

                if let Some(tags) = tags {
                    if formatted_global_tags.is_empty() {
                        formatted_metric.push_str("|#");
                    } else {
                        formatted_metric.push(',');
                    }
                    formatted_metric.push_str(&tags);
                }

                let _ = sink.emit(&formatted_metric);

                formatted_metric.clear();
            }

            // send all the aggregated "counter like" metrics
            for (AggregationKey { ty, name, tags }, value) in floats {
                formatted_metric.push_str(&prefix);
                formatted_metric.push_str(name);

                let _ = write!(&mut formatted_metric, ":{value}{ty}{formatted_global_tags}");

                if let Some(tags) = tags {
                    if formatted_global_tags.is_empty() {
                        formatted_metric.push_str("|#");
                    } else {
                        formatted_metric.push(',');
                    }
                    formatted_metric.push_str(&tags);
                }

                let _ = sink.emit(&formatted_metric);

                formatted_metric.clear();
            }

            // send all the aggregated "distribution like" metrics
            // we do this in a batched manner, as we do not actually *aggregate* them,
            // but still send each value individually.
            for (AggregationKey { ty, name, tags }, value) in distributions {
                suffix.push_str(&formatted_global_tags);
                if let Some(tags) = tags {
                    if formatted_global_tags.is_empty() {
                        suffix.push_str("|#");
                    } else {
                        suffix.push(',');
                    }
                    suffix.push_str(&tags);
                }

                for batch in value.chunks(DISTRIBUTION_BATCH_SIZE) {
                    formatted_metric.push_str(&prefix);
                    formatted_metric.push_str(name);

                    for value in batch {
                        let _ = write!(&mut formatted_metric, ":{value}");
                    }

                    formatted_metric.push_str(ty);
                    formatted_metric.push_str(&suffix);

                    let _ = sink.emit(&formatted_metric);
                    formatted_metric.clear();
                }

                suffix.clear();
            }

            for (AggregationKey { ty, name, tags }, value) in sets {
                suffix.push_str(&formatted_global_tags);
                if let Some(tags) = tags {
                    if formatted_global_tags.is_empty() {
                        suffix.push_str("|#");
                    } else {
                        suffix.push(',');
                    }
                    suffix.push_str(&tags);
                }

                for value in value {
                    formatted_metric.push_str(&prefix);
                    formatted_metric.push_str(name);

                    let _ = write!(&mut formatted_metric, ":{value}");

                    formatted_metric.push_str(ty);
                    formatted_metric.push_str(&suffix);

                    let _ = sink.emit(&formatted_metric);
                    formatted_metric.clear();
                }

                suffix.clear();
            }
        }
    };

    thread::Builder::new()
        .name("metrics-aggregator".into())
        .spawn(thread_fn)
        .unwrap();

    local_aggregators
}

fn aggregate_all(aggregators: &LocalAggregators) -> LocalAggregator {
    let mut total = LocalAggregator::default();

    for local_aggregator in aggregators.iter() {
        let LocalAggregator {
            buf: _,
            integers,
            floats,
            sets,
            distributions,
        } = local_aggregator.lock().unwrap().take();

        // aggregate all the "counter like" metrics
        if total.integers.is_empty() {
            total.integers = integers;
        } else {
            for (key, value) in integers {
                let ty = key.ty;
                let aggregated_value = total.integers.entry(key).or_default();
                if ty == "|c" {
                    *aggregated_value += value;
                } else if ty == "|g" {
                    *aggregated_value = value;
                }
            }
        }

        // aggregate all the "counter like" metrics
        if total.floats.is_empty() {
            total.floats = floats;
        } else {
            for (key, value) in floats {
                let ty = key.ty;
                let aggregated_value = total.floats.entry(key).or_default();
                if ty == "|c" {
                    *aggregated_value += value;
                } else if ty == "|g" {
                    *aggregated_value = value;
                }
            }
        }

        // aggregate all the "distribution like" metrics
        if total.distributions.is_empty() {
            total.distributions = distributions;
        } else {
            for (key, value) in distributions {
                let aggregated_value = total.distributions.entry(key).or_default();
                aggregated_value.extend(value);
            }
        }

        // aggregate all the "distribution like" metrics
        if total.sets.is_empty() {
            total.sets = sets;
        } else {
            for (key, value) in sets {
                let aggregated_value = total.sets.entry(key).or_default();
                aggregated_value.extend(value);
            }
        }
    }

    total
}

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

type AggregatedIntegers = FxHashMap<AggregationKey, i64>;
type AggregatedFloats = FxHashMap<AggregationKey, f64>;
type AggregatedSets = FxHashMap<AggregationKey, BTreeSet<u64>>;
type AggregatedDistributions = FxHashMap<AggregationKey, Vec<f64>>;

pub trait IntoDistributionValue {
    fn into_value(self) -> f64;
}

impl IntoDistributionValue for Duration {
    fn into_value(self) -> f64 {
        self.as_secs_f64() * 1_000.
    }
}

impl IntoDistributionValue for usize {
    fn into_value(self) -> f64 {
        self as f64
    }
}

impl IntoDistributionValue for u64 {
    fn into_value(self) -> f64 {
        self as f64
    }
}

impl IntoDistributionValue for i32 {
    fn into_value(self) -> f64 {
        self as f64
    }
}

impl IntoDistributionValue for f64 {
    fn into_value(self) -> f64 {
        self
    }
}

/// The `thread_local` aggregator which pre-aggregates metrics per-thread.
#[derive(Default, Debug)]
pub struct LocalAggregator {
    /// A mutable scratch-buffer that is reused to format tags into it.
    buf: String,
    /// A map of all the `counter` and `gauge` metrics we have aggregated thus far.
    integers: AggregatedIntegers,
    /// A map of all the `counter` and `gauge` metrics we have aggregated thus far.
    floats: AggregatedFloats,
    /// A map of all the `timer` and `histogram` metrics we have aggregated thus far.
    distributions: AggregatedDistributions,
    /// A map of all the `set` metrics we have aggregated thus far.
    sets: AggregatedSets,
}

impl LocalAggregator {
    fn take(&mut self) -> Self {
        Self {
            buf: String::new(),
            integers: std::mem::take(&mut self.integers),
            floats: std::mem::take(&mut self.floats),
            sets: std::mem::take(&mut self.sets),
            distributions: std::mem::take(&mut self.distributions),
        }
    }

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

        let aggregation = self.integers.entry(key).or_default();
        *aggregation += value;
    }

    /// Emit a `set` metric, which is aggregated in a set.
    pub fn emit_set(&mut self, name: &'static str, value: u64, tags: &[(&'static str, &str)]) {
        let tags = self.format_tags(tags);

        let key = AggregationKey {
            ty: "|s",
            name,
            tags,
        };

        let aggregation = self.sets.entry(key).or_default();
        aggregation.insert(value);
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

        let aggregation = self.integers.entry(key).or_default();
        *aggregation = value as i64;
    }

    /// Emit a `gauge` metric, for which only the latest value is retained.
    pub fn emit_gauge_float(
        &mut self,
        name: &'static str,
        value: f64,
        tags: &[(&'static str, &str)],
    ) {
        let tags = self.format_tags(tags);

        let key = AggregationKey {
            ty: "|g",
            name,
            tags,
        };

        let aggregation = self.floats.entry(key).or_default();
        *aggregation = value;
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

        let aggregation = self.distributions.entry(key).or_default();
        aggregation.push(value);
    }
}

/// Tell the metrics system to report to statsd.
pub fn init<A: ToSocketAddrs>(prefix: &str, host: A, tags: BTreeMap<String, String>) {
    let addrs: Vec<_> = host.to_socket_addrs().unwrap().collect();

    let socket = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let udp_sink = BufferedUdpMetricSink::from(&addrs[..], socket).unwrap();
    let queuing_sink = QueuingMetricSink::from(udp_sink);
    let sink = Sink(Arc::new(queuing_sink));

    // pre-format the global tags in `statsd` format, including a leading `|#`.
    let mut formatted_global_tags = String::new();
    for (key, value) in tags {
        if formatted_global_tags.is_empty() {
            formatted_global_tags.push_str("|#");
        } else {
            formatted_global_tags.push(',');
        }
        let _ = write!(&mut formatted_global_tags, "{key}:{value}");
    }

    let local_aggregator = make_aggregator(prefix, formatted_global_tags, sink);

    let wrapper = MetricsWrapper { local_aggregator };

    METRICS_CLIENT.set(wrapper).unwrap();
}

/// Invoke a callback with the current [`MetricsWrapper`] and [`LocalAggregator`].
///
/// If metrics have not been configured, the callback is not invoked.
/// For the most part the [`metric!`](crate::metric) macro should be used instead.
#[inline(always)]
pub fn with_client<F>(f: F)
where
    F: FnOnce(&mut LocalAggregator),
{
    if let Some(client) = METRICS_CLIENT.get() {
        client.with_local_aggregator(f)
    }
}

/// Emits a metric.
#[macro_export]
macro_rules! metric {
    // counters
    (counter($id:expr) += $value:expr $(, $k:ident = $v:expr)* $(,)?) => {
        $crate::with_client(|local| {
            let tags: &[(&'static str, &str)] = &[
                $((stringify!($k), $v)),*
            ];
            local.emit_count(&$crate::CounterMetric::name(&$id), $value as i64, tags);
        });
    };

    // counters
    (set($id:expr) = $value:expr $(, $k:ident = $v:expr)* $(,)?) => {
        $crate::with_client(|local| {
            let tags: &[(&'static str, &str)] = &[
                $((stringify!($k), $v)),*
            ];
            local.emit_set(&$crate::SetMetric::name(&$id), $value as u64, tags);
        });
    };

    // gauges
    (gauge($id:expr) = $value:expr $(, $k:ident = $v:expr)* $(,)?) => {
        $crate::with_client(|local| {
            let tags: &[(&'static str, &str)] = &[
                $((stringify!($k), $v)),*
            ];
            local.emit_gauge(&$crate::GaugeMetric::name(&$id), $value, tags);
        })
    };

    // floating point gauges
    (gauge_f($id:expr) = $value:expr $(, $k:ident = $v:expr)* $(,)?) => {
        $crate::with_client(|local| {
            let tags: &[(&'static str, &str)] = &[
                $((stringify!($k), $v)),*
            ];
            local.emit_gauge_float(&$crate::GaugeMetric::name(&$id), $value, tags);
        })
    };

    // timers
    (timer($id:expr) = $value:expr $(, $k:ident = $v:expr)* $(,)?) => {
        $crate::with_client(|local| {
            let tags: &[(&'static str, &str)] = &[
                $((stringify!($k), $v)),*
            ];
            use $crate::IntoDistributionValue;
            local.emit_timer(&$crate::TimerMetric::name(&$id), ($value).into_value(), tags);
        });
    };

    // timed block
    (timer($id:expr), $($k:ident = $v:expr,)* $block:block) => {{
        let now = std::time::Instant::now();
        let rv = {$block};
        $crate::with_client(|local| {
            let tags: &[(&'static str, &str)] = &[
                $((stringify!($k), $v)),*
            ];
            use $crate::IntoDistributionValue;
            local.emit_timer(&$crate::TimerMetric::name(&$id), now.elapsed().into_value(), tags);
        });
        rv
    }};

    // we use statsd timers to send things such as filesizes as well.
    (time_raw($id:expr) = $value:expr $(, $k:ident = $v:expr)* $(,)?) => {
        $crate::with_client(|local| {
            let tags: &[(&'static str, &str)] = &[
                $((stringify!($k), $v)),*
            ];
            use $crate::IntoDistributionValue;
            local.emit_timer(&$crate::TimerMetric::name(&$id), ($value).into_value(), tags);
        });
    };

    // histograms
    (histogram($id:expr) = $value:expr $(, $k:ident = $v:expr)* $(,)?) => {
        $crate::with_client(|local| {
            let tags: &[(&'static str, &str)] = &[
                $((stringify!($k), $v)),*
            ];
            use $crate::IntoDistributionValue;
            local.emit_histogram(&$crate::HistogramMetric::name(&$id), ($value).into_value(), tags);
        });
    };
}
