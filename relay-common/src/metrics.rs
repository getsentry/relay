//! Provides access to the metrics system.
//!
//! The following types of metrics are available: counter, timer, gauge, histogram and set.
//! For explanations on what that means see:
//! [https://github.com/statsd/statsd/blob/master/docs/metric_types.md]
//!
//! In order to use metrics one needs to first define a particular metric in a type.
//!
//! For a type instance to be used as a metric the type needs to implement one of the
//! metric traits.
//!
//! The metric traits serve only to provide a type safe metric name.
//! All metric types have exactly the same form, they are different only to ensure
//! that a metric can only be used for the type for which it was defined, (e.g. a
//! counter metric cannot be used as a timer metric).
//!
//! ## Examples:
//!
//! ```no_run
//! use relay_common::{
//!    metric,
//!    metrics::{ TimerMetric, GaugeMetric, HistogramMetric, SetMetric, CounterMetric}
//! };
//!
//! enum MyTimer {
//!    TimeSpentDoingA,
//!    TimeSpentDoingB,
//! }
//!
//! impl TimerMetric for MyTimer {
//!    fn name(&self) -> &'static str {
//!        match self {
//!            Self::TimeSpentDoingA => "processA.millisecs",
//!            Self::TimeSpentDoingB => "processB.millisecs",
//!        }
//!    }
//! }
//!
//! let start_time  = std::time::Instant::now();
//! // measure time by explicitly setting a std::timer::Duration
//! metric!(timer(MyTimer::TimeSpentDoingA) = start_time.elapsed());
//! // provide tags to a timer
//! metric!(timer(MyTimer::TimeSpentDoingA) = start_time.elapsed(),
//!    server_name = "server1",
//!    host_name = "host1");
//! // measure time implicitly by enclosing a code block in a metric
//! metric!(timer(MyTimer::TimeSpentDoingB), {
//!    // insert here the code that needs to be timed
//! });
//! // measure block and also provide tags
//! metric!(timer(MyTimer::TimeSpentDoingB),
//!    server = "server1",
//!    host = "host1",
//!    {
//!        // insert here the code that needs to be timed
//!    }
//! );
//! // using a raw timer (directly sending a value, presumably milliseconds)
//! let raw_duration = start_time.elapsed().as_millis() as u64;
//! metric!(time_raw(MyTimer::TimeSpentDoingA)=raw_duration, server="s1", some_thing="some_value" );
//!
//! struct MyGauge ;
//!
//! impl GaugeMetric for MyGauge {
//!    fn name(&self) -> &'static str { "MyGauge"}
//! }
//! // a simple gauge value
//! metric!(gauge(MyGauge)=123);
//! // a gauge with tags
//! metric!(gauge(MyGauge)=123, server="s1", some_thing="some_value" );
//!
//! //A counter metric that knows with how much should the counter be increased/decreased
//! struct MyCounter(pub i64);
//!
//! impl CounterMetric for MyCounter{
//!    fn name(&self) -> &'static str { "MyCounter"}
//! }
//!
//! let by_two = MyCounter(2);
//! let by_one = MyCounter(1);
//! // add to the counter
//! metric!(counter(by_two) += by_two.0);
//! // add to the counter and provide tags
//! metric!(counter(by_two) += by_two.0, server="s1", host="h1");
//! // subtract from the counter
//! metric!(counter(by_one) -= by_one.0);
//! // subtract from the counter and provide tags
//! metric!(counter(by_one) -= by_one.0, server="s1", host="h1");
//! // nonsensical but valid, ignore the baked-in increment
//! metric!(counter(by_one) += 1000);
//!
//! struct FancyHistogram<'s> {
//!     pub tag1: &'s str,
//!     pub tag2: &'s str,
//! }
//! impl HistogramMetric for FancyHistogram<'_>{
//!    fn name(&self) -> &'static str { "my_histogram"}
//! }
//!
//! let h1 = FancyHistogram{ tag1: "s1", tag2: "m1"};
//! let h2 = FancyHistogram{ tag1: "s2", tag2: "m2"};
//!
//! // log a histogram value
//! metric!( histogram(h1) = 33);
//! // log with tags
//! metric!( histogram(h2) = 33, t1 = h2.tag1, t2 = h2.tag2);
//!
//! struct UsersSet;
//! impl SetMetric for UsersSet{
//!    fn name(&self) -> &'static str { "num_users"}
//! }
//!
//! // use a set metric
//! let user1_id = 123;
//! metric!(set(UsersSet)=user1_id);
//! let user2_id:i64 = 44;
//! metric!(set(UsersSet)=user2_id);
//! // use a set metric with tags
//! metric!(set(UsersSet)=user2_id, tag1="tag_val", tag2="tag_val_23");
//!
//!
//! ```
//!

use std::net::ToSocketAddrs;
use std::sync::Arc;

use cadence::StatsdClient;
use lazy_static::lazy_static;
use parking_lot::RwLock;

lazy_static! {
    static ref METRICS_CLIENT: RwLock<Option<Arc<StatsdClient>>> = RwLock::new(None);
}

thread_local! {
    static CURRENT_CLIENT: Option<Arc<StatsdClient>> = METRICS_CLIENT.read().clone();
}

/// Internal prelude for the macro
#[doc(hidden)]
pub mod _pred {
    pub use cadence::prelude::*;
    pub use std::time::Instant;
}

/// The metrics prelude that is necessary to use the client.
pub mod prelude {
    pub use cadence::prelude::*;
}

/// Set a new statsd client.
pub fn set_client(statsd_client: StatsdClient) {
    *METRICS_CLIENT.write() = Some(Arc::new(statsd_client));
}

/// Disable the client again.
pub fn disable() {
    *METRICS_CLIENT.write() = None;
}

/// Tell the metrics system to report to statsd.
pub fn configure_statsd<A: ToSocketAddrs>(prefix: &str, host: A) {
    let addrs: Vec<_> = host.to_socket_addrs().unwrap().collect();
    if !addrs.is_empty() {
        log::info!("reporting metrics to statsd at {}", addrs[0]);
    }
    set_client(StatsdClient::from_udp_host(prefix, &addrs[..]).unwrap());
}

/// Invoke a callback with the current statsd client.
///
/// If statsd is not configured the callback is not invoked.  For the most part
/// the `metric!` macro should be used instead.
#[inline(always)]
pub fn with_client<F, R>(f: F) -> R
where
    F: FnOnce(&StatsdClient) -> R,
    R: Default,
{
    CURRENT_CLIENT.with(|client| {
        if let Some(client) = client {
            f(&*client)
        } else {
            R::default()
        }
    })
}

/// Trait that designates instances as representing timer metrics.
pub trait TimerMetric {
    /// Returns the timer metric name that will be sent to statsd (DataDog or whatever
    /// collection server you use)
    fn name(&self) -> &'static str;
}

/// Trait that designates instances as representing counter metrics.
pub trait CounterMetric {
    /// Returns the counter metric name that will be sent to statsd (DataDog or whatever
    /// collection server you use)
    fn name(&self) -> &'static str;
}

/// Trait that designates instances as representing histogram metrics.
pub trait HistogramMetric {
    /// Returns the histogram metric name that will be sent to statsd (DataDog or whatever
    /// collection server you use)
    fn name(&self) -> &'static str;
}

/// Trait that designates instances as representing set metrics.
pub trait SetMetric {
    /// Returns the set metric name that will be sent to statsd (DataDog or whatever
    /// collection server you use)
    fn name(&self) -> &'static str;
}

/// Trait that designates instances as representing gauge metrics.
pub trait GaugeMetric {
    /// Returns the gauge metric name that will be sent to statsd (DataDog or whatever
    /// collection server you use)
    fn name(&self) -> &'static str;
}

/// Emits a metric.
#[macro_export]
macro_rules! metric {
    // counters
    (counter($id:expr) += $value:expr) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.count(&$crate::metrics::CounterMetric::name(&$id), $value).ok();
        })
    }};
    (counter($id:expr) += $value:expr, $($k:ident = $v:expr),*) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.count_with_tags(&$crate::metrics::CounterMetric::name(&$id), $value)
                $(.with_tag(stringify!($k), $v))*
                .send();
        })
    }};
    (counter($id:expr) -= $value:expr) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| { client.count(&$crate::metrics::CounterMetric::name(&$id), -$value).ok(); })
    }};
    (counter($id:expr) -= $value:expr, $($k:ident = $v:expr),*) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.count_with_tags(&$crate::metrics::CounterMetric::name(&$id), -$value)
                $(.with_tag(stringify!(stringify!($k)), $v))*
                .send();
        })
    }};

    // gauges
    (gauge($id:expr) = $value:expr) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.gauge(&$crate::metrics::GaugeMetric::name(&$id), $value).ok();
        })
    }};
    (gauge($id:expr) = $value:expr, $($k:ident = $v:expr),*) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.gauge_with_tags(&$crate::metrics::GaugeMetric::name(&$id), $value)
                $(.with_tag(stringify!($k), $v))*
                .send();
        })
    }};

    // histograms
    (histogram($id:expr) = $value:expr) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.histogram(&$crate::metrics::HistogramMetric::name(&$id), $value).ok();
        })
    }};
    (histogram($id:expr) = $value:expr, $($k:ident = $v:expr),*) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.histogram_with_tags(&$crate::metrics::HistogramMetric::name(&$id), $value)
                $(.with_tag(stringify!($k), $v))*
                .send();
        })
    }};

    // sets ( count unique occurrences of a value per time interval)
    (set($id:expr) = $value:expr) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.set(&$crate::metrics::SetMetric::name(&$id), $value).ok();
        })
    }};
    (set($id:expr) = $value:expr, $($k:ident = $v:expr),*) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.set_with_tags(&$crate::metrics::SetMetric::name(&$id), $value)
                $(.with_tag(stringify!($k), $v))*
                .send();
        })
    }};

    // timers
    (timer($id:expr) = $value:expr) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.time_duration(&$crate::metrics::TimerMetric::name(&$id), $value).ok();
        })
    }};
    (timer($id:expr) = $value:expr, $($k:ident = $v:expr),*) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.time_duration_with_tags(&$crate::metrics::TimerMetric::name(&$id), $value)
                $(.with_tag(stringify!($k), $v))*
                .send();
        })
    }};
    (timer($id:expr), $block:block) => {{
        use $crate::metrics::_pred::*;
        let now = Instant::now();
        let rv = {$block};
        $crate::metrics::with_client(|client| {
            client.time_duration(&$crate::metrics::TimerMetric::name(&$id), now.elapsed()).ok();
        });
        rv
    }};
    (timer($id:expr), $($k:ident = $v:expr),*, $block:block) => {{
        use $crate::metrics::_pred::*;
        let now = Instant::now();
        let rv = {$block};
        $crate::metrics::with_client(|client| {
            client.time_duration_with_tags(&$crate::metrics::TimerMetric::name(&$id), now.elapsed())
                $(.with_tag(stringify!($k), $v))*
                .send();
        });
        rv
    }};

    // we use statsd timers to send things such as filesizes as well.
    (time_raw($id:expr) = $value:expr  $(, $k:ident = $v:expr)* $(,)?) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.time_with_tags(&$crate::metrics::TimerMetric::name(&$id), $value)
                $(.with_tag(stringify!($k), $v))*
                .send();
        })
    }};

}
