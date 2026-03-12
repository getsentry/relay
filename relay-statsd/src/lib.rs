//! A high-level StatsD metric client built on cadence.
//!
//! ## Defining Metrics
//!
//! In order to use metrics, one needs to first define one of the metric traits on a custom enum.
//! The following types of metrics are available: `counter`, `timer`, `gauge`, `distribution`, and
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
//! To initialize the client, use [`init`] to create a default client with known arguments:
//!
//! ```no_run
//! # use std::collections::BTreeMap;
//! # use relay_statsd::MetricsConfig;
//!
//! relay_statsd::init(MetricsConfig {
//!     prefix: "myprefix".to_owned(),
//!     host: "localhost:8125".to_owned(),
//!     buffer_size: None,
//!     default_tags: BTreeMap::new(),
//! });
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
//! [Metric Types]: https://github.com/statsd/statsd/blob/master/docs/metric_types.md
use metrics_exporter_dogstatsd::{AggregationMode, BuildError, DogStatsDBuilder};

use std::{collections::BTreeMap, fmt};

use crate::mock::MockRecorder;

mod mock;

#[doc(hidden)]
pub mod _metrics {
    pub use ::metrics::*;
}

/// Client configuration used for initialization of the metrics sub-system.
#[derive(Debug)]
pub struct MetricsConfig {
    /// Prefix which is appended to all metric names.
    pub prefix: String,
    /// Host of the metrics upstream.
    pub host: String,
    /// The buffer size to use for the socket.
    pub buffer_size: Option<usize>,
    /// Tags that are added to all metrics.
    pub default_tags: BTreeMap<String, String>,
}

/// Error returned from [`init`].
#[derive(Debug)]
pub struct Error(BuildError);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Error {}

impl From<BuildError> for Error {
    fn from(value: BuildError) -> Self {
        Self(value)
    }
}

/// Set a test client for the period of the called function (only affects the current thread).
pub fn with_capturing_test_client(f: impl FnOnce()) -> Vec<String> {
    let recorder = MockRecorder::default();
    metrics::with_local_recorder(&recorder, f);
    recorder.consume()
}

/// Tell the metrics system to report to statsd.
pub fn init(config: MetricsConfig) -> Result<(), Error> {
    relay_log::info!("reporting metrics to statsd at {}", config.host);

    let default_labels = config
        .default_tags
        .into_iter()
        .map(|(key, value)| metrics::Label::new(key, value))
        .collect();

    let mut statsd = DogStatsDBuilder::default()
        .with_remote_address(&config.host)?
        .with_telemetry(true)
        .with_aggregation_mode(AggregationMode::Aggressive)
        .send_histograms_as_distributions(true)
        .with_histogram_sampling(true)
        .set_global_prefix(config.prefix)
        .with_global_labels(default_labels);

    if let Some(buffer_size) = config.buffer_size {
        statsd = statsd.with_maximum_payload_length(buffer_size)?;
    };

    statsd.install()?;

    Ok(())
}

/// A metric for capturing timings.
///
/// Timings are a positive number of milliseconds between a start and end time. Examples include
/// time taken to render a web page or time taken for a database call to return.
///
/// ## Example
///
/// ```
/// use relay_statsd::{metric, TimerMetric};
///
/// enum MyTimer {
///     ProcessA,
///     ProcessB,
/// }
///
/// impl TimerMetric for MyTimer {
///     fn name(&self) -> &'static str {
///         match self {
///             Self::ProcessA => "process_a",
///             Self::ProcessB => "process_b",
///         }
///     }
/// }
///
/// # fn process_a() {}
///
/// // measure time by explicitly setting a std::timer::Duration
/// # use std::time::Instant;
/// let start_time = Instant::now();
/// process_a();
/// metric!(timer(MyTimer::ProcessA) = start_time.elapsed());
///
/// // provide tags to a timer
/// metric!(
///     timer(MyTimer::ProcessA) = start_time.elapsed(),
///     server = "server1",
///     host = "host1",
/// );
///
/// // measure time implicitly by enclosing a code block in a metric
/// metric!(timer(MyTimer::ProcessA), {
///     process_a();
/// });
///
/// // measure block and also provide tags
/// metric!(
///     timer(MyTimer::ProcessB),
///     server = "server1",
///     host = "host1",
///     {
///         process_a();
///     }
/// );
/// ```
pub trait TimerMetric {
    /// Returns the timer metric name that will be sent to statsd.
    fn name(&self) -> &'static str;
}

/// A metric for capturing counters.
///
/// Counters are simple values incremented or decremented by a client. The rates at which these
/// events occur or average values will be determined by the server receiving them. Examples of
/// counter uses include number of logins to a system or requests received.
///
/// ## Example
///
/// ```
/// use relay_statsd::{metric, CounterMetric};
///
/// enum MyCounter {
///     TotalRequests,
///     TotalBytes,
/// }
///
/// impl CounterMetric for MyCounter {
///     fn name(&self) -> &'static str {
///         match self {
///             Self::TotalRequests => "total_requests",
///             Self::TotalBytes => "total_bytes",
///         }
///     }
/// }
///
/// # let buffer = &[(), ()];
///
/// // add to the counter
/// metric!(counter(MyCounter::TotalRequests) += 1);
/// metric!(counter(MyCounter::TotalBytes) += buffer.len() as u64);
///
/// // add to the counter and provide tags
/// metric!(
///     counter(MyCounter::TotalRequests) += 1,
///     server = "s1",
///     host = "h1"
/// );
/// ```
pub trait CounterMetric {
    /// Returns the counter metric name that will be sent to statsd.
    fn name(&self) -> &'static str;
}

/// A metric for capturing distributions.
///
/// A distribution is often similar to timers. Distributions can be thought of as a
/// more general (not limited to timing things) form of timers.
///
/// ## Example
///
/// ```
/// use relay_statsd::{metric, DistributionMetric};
///
/// struct QueueSize;
///
/// impl DistributionMetric for QueueSize {
///     fn name(&self) -> &'static str {
///         "queue_size"
///     }
/// }
///
/// # use std::collections::VecDeque;
/// let queue = VecDeque::new();
/// # let _hint: &VecDeque<()> = &queue;
///
/// // record a distribution value (uses global sample rate)
/// metric!(distribution(QueueSize) = queue.len() as u64);
///
/// // record with tags
/// metric!(
///     distribution(QueueSize) = queue.len() as u64,
///     server = "server1",
///     host = "host1",
/// );
/// ```
pub trait DistributionMetric {
    /// Returns the distribution metric name that will be sent to statsd.
    fn name(&self) -> &'static str;
}

/// A metric for capturing gauges.
///
/// Gauge values are an instantaneous measurement of a value determined by the client. They do not
/// change unless changed by the client. Examples include things like load average or how many
/// connections are active.
///
/// ## Example
///
/// ```
/// use relay_statsd::{metric, GaugeMetric};
///
/// struct QueueSize;
///
/// impl GaugeMetric for QueueSize {
///     fn name(&self) -> &'static str {
///         "queue_size"
///     }
/// }
///
/// # use std::collections::VecDeque;
/// let queue = VecDeque::new();
/// # let _hint: &VecDeque<()> = &queue;
///
/// // a simple gauge value
/// metric!(gauge(QueueSize) = queue.len() as u64);
///
/// // a gauge with tags
/// metric!(
///     gauge(QueueSize) = queue.len() as u64,
///     server = "server1",
///     host = "host1"
/// );
///
/// // subtract from the gauge
/// metric!(gauge(QueueSize) -= 1);
///
/// // subtract from the gauge and provide tags
/// metric!(
///     gauge(QueueSize) -= 1,
///     server = "s1",
///     host = "h1"
/// );
/// ```
pub trait GaugeMetric {
    /// Returns the gauge metric name that will be sent to statsd.
    fn name(&self) -> &'static str;
}

#[doc(hidden)]
#[macro_export]
macro_rules! key_var {
    ($id:expr $(,)*) => {{
        let name = $crate::_metrics::KeyName::from_const_str($id);
        $crate::_metrics::Key::from_static_labels(name, &[])
    }};
    ($id:expr $(, $k:expr => $v:expr)* $(,)?) => {{
        let name = $crate::_metrics::KeyName::from_const_str($id);
        let labels = ::std::vec![
            $($crate::_metrics::Label::new(
                $crate::_metrics::SharedString::const_str($k),
                $crate::_metrics::SharedString::from_owned($v.into())
            )),*
        ];

        $crate::_metrics::Key::from_parts(name, labels)
    }};
}

/// Emits a metric.
///
/// See [crate-level documentation](self) for examples.
#[macro_export]
macro_rules! metric {
    // counter increment
    (counter($id:expr) += $value:expr $(, $($k:ident).* = $v:expr)* $(,)?) => {{
        match $value {
            value if value != 0 => {
                let key = $crate::key_var!($crate::CounterMetric::name(&$id) $(, stringify!($($k).*) => $v)*);
                let metadata = $crate::_metrics::metadata_var!(::std::module_path!(), $crate::_metrics::Level::INFO);
                $crate::_metrics::with_recorder(|recorder| recorder.register_counter(&key, metadata))
                    .increment(value);
            }
            _ => {}
        }
    }};

    // gauge set
    (gauge($id:expr) = $value:expr $(, $($k:ident).* = $v:expr)* $(,)?) => {{
        let key = $crate::key_var!($crate::GaugeMetric::name(&$id) $(, stringify!($($k).*) => $v)*);
        let metadata = $crate::_metrics::metadata_var!(::std::module_path!(), $crate::_metrics::Level::INFO);
        $crate::_metrics::with_recorder(|recorder| recorder.register_gauge(&key, metadata))
            .set($value as f64);
    }};
    // gauge increment
    (gauge($id:expr) += $value:expr $(, $($k:ident).* = $v:expr)* $(,)?) => {{
        let key = $crate::key_var!($crate::GaugeMetric::name(&$id) $(, stringify!($($k).*) => $v)*);
        let metadata = $crate::_metrics::metadata_var!(::std::module_path!(), $crate::_metrics::Level::INFO);
        $crate::_metrics::with_recorder(|recorder| recorder.register_gauge(&key, metadata))
            .increment($value as f64);
    }};
    // gauge decrement
    (gauge($id:expr) -= $value:expr $(, $($k:ident).* = $v:expr)* $(,)?) => {{
        let key = $crate::key_var!($crate::GaugeMetric::name(&$id) $(, stringify!($($k).*) => $v)*);
        let metadata = $crate::_metrics::metadata_var!(::std::module_path!(), $crate::_metrics::Level::INFO);
        $crate::_metrics::with_recorder(|recorder| recorder.register_gauge(&key, metadata))
            .decrement($value as f64);
    }};

    // distribution
    (distribution($id:expr) = $value:expr $(, $($k:ident).* = $v:expr)* $(,)?) => {{
        let key = $crate::key_var!($crate::DistributionMetric::name(&$id) $(, stringify!($($k).*) => $v)*);
        let metadata = $crate::_metrics::metadata_var!(::std::module_path!(), $crate::_metrics::Level::INFO);
        $crate::_metrics::with_recorder(|recorder| recorder.register_histogram(&key, metadata))
            .record($value as f64);
    }};

    // timer value
    (timer($id:expr) = $value:expr $(, $($k:ident).* = $v:expr)* $(,)?) => {{
        let key = $crate::key_var!($crate::TimerMetric::name(&$id) $(, stringify!($($k).*) => $v)*);
        let metadata = $crate::_metrics::metadata_var!(::std::module_path!(), $crate::_metrics::Level::INFO);
        $crate::_metrics::with_recorder(|recorder| recorder.register_histogram(&key, metadata))
            .record($value.as_nanos() as f64 / 1e6);
    }};

    // timed block
    (timer($id:expr), $($($k:ident).* = $v:expr,)* $block:block) => {{
        let now = std::time::Instant::now();
        let rv = {$block};
        $crate::metric!(timer($id) = now.elapsed() $(, $($k).* = $v)*);
        rv
    }};
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    enum TestGauges {
        Foo,
        Bar,
    }

    impl GaugeMetric for TestGauges {
        fn name(&self) -> &'static str {
            match self {
                Self::Foo => "foo",
                Self::Bar => "bar",
            }
        }
    }

    struct TestCounter;

    impl CounterMetric for TestCounter {
        fn name(&self) -> &'static str {
            "counter"
        }
    }

    struct TestDistribution;

    impl DistributionMetric for TestDistribution {
        fn name(&self) -> &'static str {
            "distribution"
        }
    }

    struct TestTimer;

    impl TimerMetric for TestTimer {
        fn name(&self) -> &'static str {
            "timer"
        }
    }

    #[test]
    fn test_capturing_client() {
        let captures = with_capturing_test_client(|| {
            metric!(
                gauge(TestGauges::Foo) = 123,
                server = "server1",
                host = "host1"
            );
            metric!(
                gauge(TestGauges::Bar) = 456,
                server = "server2",
                host = "host2"
            );
        });

        assert_eq!(
            captures,
            [
                "foo:123|g|#server:server1,host:host1",
                "bar:456|g|#server:server2,host:host2"
            ]
        )
    }

    #[test]
    fn test_counter_tags_with_dots() {
        let captures = with_capturing_test_client(|| {
            metric!(
                counter(TestCounter) += 10,
                hc.project_id = "567",
                server = "server1",
            );
            metric!(
                counter(TestCounter) += 5,
                hc.project_id = "567",
                server = "server1",
            );
        });
        assert_eq!(
            captures,
            [
                "counter:10|c|#hc.project_id:567,server:server1",
                "counter:5|c|#hc.project_id:567,server:server1"
            ]
        );
    }

    #[test]
    fn test_gauge_tags_with_dots() {
        let captures = with_capturing_test_client(|| {
            metric!(
                gauge(TestGauges::Foo) = 123,
                hc.project_id = "567",
                server = "server1",
            );
        });
        assert_eq!(captures, ["foo:123|g|#hc.project_id:567,server:server1"]);
    }

    #[test]
    fn test_distribution_tags_with_dots() {
        let captures = with_capturing_test_client(|| {
            metric!(
                distribution(TestDistribution) = 123,
                hc.project_id = "567",
                server = "server1",
            );
        });
        assert_eq!(
            captures,
            ["distribution:123|d|#hc.project_id:567,server:server1"]
        );
    }

    #[test]
    fn test_timer_tags_with_dots() {
        let captures = with_capturing_test_client(|| {
            let duration = Duration::from_secs(100);
            metric!(
                timer(TestTimer) = duration,
                hc.project_id = "567",
                server = "server1",
            );
        });
        assert_eq!(
            captures,
            ["timer:100000|d|#hc.project_id:567,server:server1"]
        );
    }

    #[test]
    fn test_timed_block_tags_with_dots() {
        let captures = with_capturing_test_client(|| {
            metric!(
                timer(TestTimer),
                hc.project_id = "567",
                server = "server1",
                {
                    // your code could be here
                }
            )
        });
        // just check the tags to not make this flaky
        assert!(captures[0].ends_with("|d|#hc.project_id:567,server:server1"));
    }

    #[test]
    fn nanos_rounding_error() {
        let one_day = Duration::from_secs(60 * 60 * 24);
        let captures = with_capturing_test_client(|| {
            metric!(timer(TestTimer) = one_day + Duration::from_nanos(1),);
        });

        // for "short" durations, precision is preserved:
        assert_eq!(captures, ["timer:86400000.000001|d|#"]);

        let one_year = Duration::from_secs(60 * 60 * 24 * 365);
        let captures = with_capturing_test_client(|| {
            metric!(timer(TestTimer) = one_year + Duration::from_nanos(1),);
        });

        // for very long durations, precision is lost:
        assert_eq!(captures, ["timer:31536000000|d|#"]);
    }
}
