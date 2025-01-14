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
