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
//! To initialize the client, either use [`set_client`] to pass a custom client, or use
//! [`init`] to create a default client with known arguments:
//!
//! ```no_run
//! # use std::collections::BTreeMap;
//!
//! relay_statsd::init("myprefix", "localhost:8125", BTreeMap::new(), true, true, 1.0);
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
//! ## Manual Usage
//!
//! ```
//! use relay_statsd::prelude::*;
//!
//! relay_statsd::with_client(|client| {
//!     client.count("mymetric", 1).ok();
//! });
//! ```
//!
//! [Metric Types]: https://github.com/statsd/statsd/blob/master/docs/metric_types.md
use std::collections::BTreeMap;
use std::net::{ToSocketAddrs, UdpSocket};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use cadence::{
    BufferedUdpMetricSink, Metric, MetricBuilder, QueuingMetricSink, StatsdClient, UdpMetricSink,
};
use parking_lot::RwLock;
use rand::distributions::{Distribution, Uniform};
use statsdproxy::cadence::StatsdProxyMetricSink;
use statsdproxy::config::AggregateMetricsConfig;

/// Maximum number of metric events that can be queued before we start dropping them
const METRICS_MAX_QUEUE_SIZE: usize = 100_000;

/// Client configuration object to store globally.
#[derive(Debug)]
pub struct MetricsClient {
    /// The raw statsd client.
    pub statsd_client: StatsdClient,
    /// Default tags to apply to every metric.
    pub default_tags: BTreeMap<String, String>,
    /// Global sample rate.
    pub sample_rate: f32,
    /// Receiver for external listeners.
    ///
    /// Only available when the client was initialized with `init_basic`.
    pub rx: Option<crossbeam_channel::Receiver<Vec<u8>>>,
}

impl Deref for MetricsClient {
    type Target = StatsdClient;

    fn deref(&self) -> &StatsdClient {
        &self.statsd_client
    }
}

impl DerefMut for MetricsClient {
    fn deref_mut(&mut self) -> &mut StatsdClient {
        &mut self.statsd_client
    }
}

impl MetricsClient {
    /// Send a metric with the default tags defined on this `MetricsClient`.
    #[inline(always)]
    pub fn send_metric<'a, T>(&'a self, mut metric: MetricBuilder<'a, '_, T>)
    where
        T: Metric + From<String>,
    {
        if !self._should_send() {
            return;
        }

        for (k, v) in &self.default_tags {
            metric = metric.with_tag(k, v);
        }

        if let Err(error) = metric.try_send() {
            relay_log::error!(
                error = &error as &dyn std::error::Error,
                maximum_capacity = METRICS_MAX_QUEUE_SIZE,
                "Error sending a metric",
            );
        }
    }

    fn _should_send(&self) -> bool {
        if self.sample_rate <= 0.0 {
            false
        } else if self.sample_rate >= 1.0 {
            true
        } else {
            // Using thread local RNG and uniform distribution here because Rng::gen_range is
            // "optimized for the case that only a single sample is made from the given range".
            // See https://docs.rs/rand/0.7.3/rand/distributions/uniform/struct.Uniform.html for more
            // details.
            let mut rng = rand::thread_rng();
            RNG_UNIFORM_DISTRIBUTION
                .with(|uniform_dist| uniform_dist.sample(&mut rng) <= self.sample_rate)
        }
    }
}

static METRICS_CLIENT: RwLock<Option<Arc<MetricsClient>>> = RwLock::new(None);

thread_local! {
    static CURRENT_CLIENT: std::cell::RefCell<Option<Arc<MetricsClient>>>  = METRICS_CLIENT.read().clone().into();
    static RNG_UNIFORM_DISTRIBUTION: Uniform<f32> = Uniform::new(0.0, 1.0);
}

/// Internal prelude for the macro
#[doc(hidden)]
pub mod _pred {
    pub use cadence::prelude::*;
}

/// The metrics prelude that is necessary to use the client.
pub mod prelude {
    pub use cadence::prelude::*;
}

/// Set a new statsd client.
pub fn set_client(client: MetricsClient) {
    *METRICS_CLIENT.write() = Some(Arc::new(client));
    CURRENT_CLIENT.with(|cell| cell.replace(METRICS_CLIENT.read().clone()));
}

/// Set a test client for the period of the called function (only affects the current thread).
// TODO: replace usages with `init_basic`
pub fn with_capturing_test_client(f: impl FnOnce()) -> Vec<String> {
    let (rx, sink) = cadence::SpyMetricSink::new();
    let test_client = MetricsClient {
        statsd_client: StatsdClient::from_sink("", sink),
        default_tags: Default::default(),
        sample_rate: 1.0,
        rx: None,
    };

    CURRENT_CLIENT.with(|cell| {
        let old_client = cell.replace(Some(Arc::new(test_client)));
        f();
        cell.replace(old_client);
    });

    rx.iter().map(|x| String::from_utf8(x).unwrap()).collect()
}

// Setup a simple metrics listener.
//
// Returns `None` if the global metrics client has already been configured.
pub fn init_basic() -> Option<crossbeam_channel::Receiver<Vec<u8>>> {
    CURRENT_CLIENT.with(|cell| {
        if cell.borrow().is_none() {
            // Setup basic observable metrics sink.
            let (receiver, sink) = cadence::SpyMetricSink::new();
            let test_client = MetricsClient {
                statsd_client: StatsdClient::from_sink("", sink),
                default_tags: Default::default(),
                sample_rate: 1.0,
                rx: Some(receiver.clone()),
            };
            cell.replace(Some(Arc::new(test_client)));
        }
    });

    CURRENT_CLIENT.with(|cell| {
        cell.borrow()
            .as_deref()
            .and_then(|client| match &client.rx {
                Some(rx) => Some(rx.clone()),
                None => {
                    relay_log::error!("Metrics client was already set up.");
                    None
                }
            })
    })
}

/// Disable the client again.
pub fn disable() {
    *METRICS_CLIENT.write() = None;
}

/// Tell the metrics system to report to statsd.
pub fn init<A: ToSocketAddrs>(
    prefix: &str,
    host: A,
    default_tags: BTreeMap<String, String>,
    buffering: bool,
    aggregating: bool,
    sample_rate: f32,
) {
    let addrs: Vec<_> = host.to_socket_addrs().unwrap().collect();
    if !addrs.is_empty() {
        relay_log::info!("reporting metrics to statsd at {}", addrs[0]);
    }

    // Normalize sample_rate
    let sample_rate = sample_rate.clamp(0., 1.);
    relay_log::debug!(
        "metrics sample rate is set to {sample_rate}{}",
        if sample_rate == 0.0 {
            ", no metrics will be reported"
        } else {
            ""
        }
    );

    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.set_nonblocking(true).unwrap();

    let statsd_client = if aggregating {
        let host = host.to_socket_addrs().unwrap().next().unwrap();
        let statsdproxy_sink = StatsdProxyMetricSink::new(move || {
            let next_step = statsdproxy::middleware::upstream::Upstream::new(host)
                .expect("failed to create statsdproxy metric sink");
            statsdproxy::middleware::aggregate::AggregateMetrics::new(
                AggregateMetricsConfig {
                    aggregate_gauges: true,
                    aggregate_counters: true,
                    flush_interval: 1,
                    flush_offset: 0,
                    max_map_size: None,
                },
                next_step,
            )
        });
        StatsdClient::from_sink(prefix, statsdproxy_sink)
    } else if buffering {
        let udp_sink = BufferedUdpMetricSink::from(host, socket).unwrap();
        let queuing_sink = QueuingMetricSink::with_capacity(udp_sink, METRICS_MAX_QUEUE_SIZE);
        StatsdClient::from_sink(prefix, queuing_sink)
    } else {
        let simple_sink = UdpMetricSink::from(host, socket).unwrap();
        StatsdClient::from_sink(prefix, simple_sink)
    };
    relay_log::debug!(
        "metrics buffering is {}",
        if buffering { "enabled" } else { "disabled" }
    );

    set_client(MetricsClient {
        statsd_client,
        default_tags,
        sample_rate,
        rx: None,
    });
}

/// Invoke a callback with the current statsd client.
///
/// If statsd is not configured the callback is not invoked.  For the most part
/// the [`metric!`] macro should be used instead.
#[inline(always)]
pub fn with_client<F, R>(f: F) -> R
where
    F: FnOnce(&MetricsClient) -> R,
    R: Default,
{
    CURRENT_CLIENT.with(|client| {
        if let Some(client) = client.borrow().as_deref() {
            f(client)
        } else {
            R::default()
        }
    })
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
///
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
/// metric!(counter(MyCounter::TotalBytes) += buffer.len() as i64);
///
/// // add to the counter and provide tags
/// metric!(
///     counter(MyCounter::TotalRequests) += 1,
///     server = "s1",
///     host = "h1"
/// );
///
/// // subtract from the counter
/// metric!(counter(MyCounter::TotalRequests) -= 1);
///
/// // subtract from the counter and provide tags
/// metric!(
///     counter(MyCounter::TotalRequests) -= 1,
///     server = "s1",
///     host = "h1"
/// );
/// ```
pub trait CounterMetric {
    /// Returns the counter metric name that will be sent to statsd.
    fn name(&self) -> &'static str;
}

/// A metric for capturing histograms.
///
/// Histograms are values whose distribution is calculated by the server. The distribution
/// calculated for histograms is often similar to that of timers. Histograms can be thought of as a
/// more general (not limited to timing things) form of timers.
///
/// ## Example
///
/// ```
/// use relay_statsd::{metric, HistogramMetric};
///
/// struct QueueSize;
///
/// impl HistogramMetric for QueueSize {
///     fn name(&self) -> &'static str {
///         "queue_size"
///     }
/// }
///
/// # use std::collections::VecDeque;
/// let queue = VecDeque::new();
/// # let _hint: &VecDeque<()> = &queue;
///
/// // record a histogram value
/// metric!(histogram(QueueSize) = queue.len() as u64);
///
/// // record with tags
/// metric!(
///     histogram(QueueSize) = queue.len() as u64,
///     server = "server1",
///     host = "host1",
/// );
/// ```
pub trait HistogramMetric {
    /// Returns the histogram metric name that will be sent to statsd.
    fn name(&self) -> &'static str;
}

/// A metric for capturing sets.
///
/// Sets count the number of unique elements in a group. You can use them to, for example, count the
/// unique visitors to your site.
///
/// ## Example
///
/// ```
/// use relay_statsd::{metric, SetMetric};
///
/// enum MySet {
///     UniqueProjects,
///     UniqueUsers,
/// }
///
/// impl SetMetric for MySet {
///     fn name(&self) -> &'static str {
///         match self {
///             MySet::UniqueProjects => "unique_projects",
///             MySet::UniqueUsers => "unique_users",
///         }
///     }
/// }
///
/// # use std::collections::HashSet;
/// let users = HashSet::new();
/// # let _hint: &HashSet<()> = &users;
///
/// // use a set metric
/// metric!(set(MySet::UniqueUsers) = users.len() as i64);
///
/// // use a set metric with tags
/// metric!(
///     set(MySet::UniqueUsers) = users.len() as i64,
///     server = "server1",
///     host = "host1",
/// );
/// ```
pub trait SetMetric {
    /// Returns the set metric name that will be sent to statsd.
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
/// ```
pub trait GaugeMetric {
    /// Returns the gauge metric name that will be sent to statsd.
    fn name(&self) -> &'static str;
}

/// Emits a metric.
///
/// See [crate-level documentation](self) for examples.
#[macro_export]
macro_rules! metric {
    // counter increment
    (counter($id:expr) += $value:expr $(, $k:ident = $v:expr)* $(,)?) => {
        $crate::with_client(|client| {
            use $crate::_pred::*;
            client.send_metric(
                client.count_with_tags(&$crate::CounterMetric::name(&$id), $value)
                $(.with_tag(stringify!($k), $v))*
            )
        })
    };

    // counter decrement
    (counter($id:expr) -= $value:expr $(, $k:ident = $v:expr)* $(,)?) => {
        $crate::with_client(|client| {
            use $crate::_pred::*;
            client.send_metric(
                client.count_with_tags(&$crate::CounterMetric::name(&$id), -$value)
                    $(.with_tag(stringify!($k), $v))*
            )
        })
    };

    // gauge set
    (gauge($id:expr) = $value:expr $(, $k:ident = $v:expr)* $(,)?) => {
        $crate::with_client(|client| {
            use $crate::_pred::*;
            client.send_metric(
                client.gauge_with_tags(&$crate::GaugeMetric::name(&$id), $value)
                    $(.with_tag(stringify!($k), $v))*
            )
        })
    };

    // histogram
    (histogram($id:expr) = $value:expr $(, $k:ident = $v:expr)* $(,)?) => {
        $crate::with_client(|client| {
            use $crate::_pred::*;
            client.send_metric(
                client.histogram_with_tags(&$crate::HistogramMetric::name(&$id), $value)
                    $(.with_tag(stringify!($k), $v))*
            )
        })
    };

    // sets (count unique occurrences of a value per time interval)
    (set($id:expr) = $value:expr $(, $k:ident = $v:expr)* $(,)?) => {
        $crate::with_client(|client| {
            use $crate::_pred::*;
            client.send_metric(
                client.set_with_tags(&$crate::SetMetric::name(&$id), $value)
                    $(.with_tag(stringify!($k), $v))*
            )
        })
    };

    // timer value (duration)
    (timer($id:expr) = $value:expr $(, $k:ident = $v:expr)* $(,)?) => {
        $crate::with_client(|client| {
            use $crate::_pred::*;
            client.send_metric(
                client.time_with_tags(&$crate::TimerMetric::name(&$id), $value)
                    $(.with_tag(stringify!($k), $v))*
            )
        })
    };

    // timed block
    (timer($id:expr), $($k:ident = $v:expr,)* $block:block) => {{
        let now = std::time::Instant::now();
        let rv = {$block};
        $crate::with_client(|client| {
            use $crate::_pred::*;
            client.send_metric(
                client.time_with_tags(&$crate::TimerMetric::name(&$id), now.elapsed())
                    $(.with_tag(stringify!($k), $v))*
            )
        });
        rv
    }};
}

#[cfg(test)]
mod tests {
    use cadence::{NopMetricSink, StatsdClient};

    use crate::{set_client, with_capturing_test_client, with_client, GaugeMetric, MetricsClient};

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
    fn current_client_is_global_client() {
        let client1 = with_client(|c| format!("{c:?}"));
        set_client(MetricsClient {
            statsd_client: StatsdClient::from_sink("", NopMetricSink),
            default_tags: Default::default(),
            sample_rate: 1.0,
            rx: None,
        });
        let client2 = with_client(|c| format!("{c:?}"));

        // After setting the global client,the current client must change:
        assert_ne!(client1, client2);
    }
}
