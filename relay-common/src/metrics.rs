//! Provides access to the metrics sytem.
use std::borrow::Cow;
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

/// Any type that implements this trait can be set as a timer
/// A typical implementation would be something like this
///
/// ```ignore
///
///enum MyTimer {
///    TimeSpentDoingA,
///    TimeSpentDoingB,
///}
///
///impl TimerMetric for MyTimer {
///    fn name(&self) -> &str {
///        match self {
///            Self::TimeSpentDoingA => "processA.millisecs",
///            Self::TimeSpentDoingB => "processB.millisecs",
///        }
///    }
///}
/// ```
///
/// The type can then be used with any of the metric timer macros.
///
/// ```ignore
/// let start_time  = std::time::Instant::now();
/// metric!(timer(MyTimer::TimeSpentDoingA) = start_time.elapsed());
/// ```
pub trait TimerMetric {
    /// Returns the timer metric name that will be sent to statsd (DataDog or whatever
    /// collection server you use)
    fn name(&self) -> Cow<'_, str>;
}

/// Any type that implements this trait can be set as a counter
/// This is very similar with the [TimerMetric] trait, see [TimerMetric] for details.
pub trait CounterMetric {
    /// Returns the counter metric name that will be sent to statsd (DataDog or whatever
    /// collection server you use)
    fn name(&self) -> Cow<'_, str>;
}

/// Any type that implements this trait can be set as a histogram
/// This is very similar with the [TimerMetric] trait, see [TimerMetric] for details.
pub trait HistogramMetric {
    /// Returns the histogram metric name that will be sent to statsd (DataDog or whatever
    /// collection server you use)
    fn name(&self) -> Cow<'_, str>;
}

/// Any type that implements this trait can be set as a timer
/// This is very similar with the [TimerMetric] trait, see [TimerMetric] for details.
pub trait SetMetric {
    /// Returns the set metric name that will be sent to statsd (DataDog or whatever
    /// collection server you use)
    fn name(&self) -> Cow<'_, str>;
}

/// Any type that implements this trait can be set as a gauge
/// This is very similar with the [TimerMetric] trait, see [TimerMetric] for details.
pub trait GaugeMetric {
    /// Returns the gauge metric name that will be sent to statsd (DataDog or whatever
    /// collection server you use)
    fn name(&self) -> Cow<'_, str>;
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
    (counter($id:expr) += $value:expr, $($k:expr => $v:expr),*) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.count_with_tags(&$crate::metrics::CounterMetric::name(&$id), $value)
                $(.with_tag($k, $v))*
                .send();
        })
    }};
    (counter($id:expr) -= $value:expr) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| { client.count(&$crate::metrics::CounterMetric::name(&$id), -$value).ok(); })
    }};
    (counter($id:expr) -= $value:expr, $($k:expr => $v:expr),*) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.count_with_tags(&$crate::metrics::CounterMetric::name(&$id), -$value)
                $(.with_tag($k, $v))*
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
    (gauge($id:expr) = $value:expr, $($k:expr => $v:expr),*) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.gauge_with_tags(&$crate::metrics::GaugeMetric::name(&$id), $value)
                $(.with_tag($k, $v))*
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
    (histogram($id:expr) = $value:expr, $($k:expr => $v:expr),*) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.histogram_with_tags(&$crate::metrics::HistogramMetric::name(&$id), $value)
                $(.with_tag($k, $v))*
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
    (set($id:expr) = $value:expr, $($k:expr => $v:expr),*) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.set_with_tags(&$crate::metrics::SetMetric::name(&$id), $value)
                $(.with_tag($k, $v))*
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
    (timer($id:expr) = $value:expr, $($k:expr => $v:expr),*) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.time_duration_with_tags(&$crate::metrics::TimerMetric::name(&$id), $value)
                $(.with_tag($k, $v))*
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
    (timer($id:expr), $block:block, $($k:expr => $v:expr)*) => {{
        use $crate::metrics::_pred::*;
        let now = Instant::now();
        let rv = {$block};
        $crate::metrics::with_client(|client| {
            client.time_duration_with_tags(&$crate::metrics::TimerMetric::name(&$id), now.elapsed())
                $(.with_tag($k, $v))*
                .send();
        });
        rv
    }};

    // we use statsd timers to send things such as filesizes as well.
    (time_raw($id:expr) = $value:expr $(, $k:expr => $v:expr)* $(,)?) => {{
        use $crate::metrics::_pred::*;
        $crate::metrics::with_client(|client| {
            client.time_with_tags(&$crate::metrics::TimerMetric::name(&$id), $value)
                $(.with_tag($k, $v))*
                .send();
        })
    }};

}

#[cfg(test)]
mod tests {
    use super::*;

    enum MyTimer {
        TheA,
        TheB,
    }

    impl TimerMetric for MyTimer {
        fn name(&self) -> Cow<'_, str> {
            match self {
                Self::TheA => Cow::Borrowed("theA"),
                Self::TheB => Cow::Borrowed("theB"),
            }
        }
    }

    // should compile, do not mark this function as test
    #[allow(dead_code)]
    fn timer_works() {
        let t = std::time::Instant::now();
        metric!(timer(MyTimer::TheA) = t.elapsed());
    }

    enum MySet {
        Set1,
        Set2(i32),
    }

    impl SetMetric for MySet {
        fn name(&self) -> Cow<'_, str> {
            match self {
                Self::Set1 => Cow::Borrowed("Set1"),
                Self::Set2(val) => Cow::Owned(format!("Set2_{}", val)),
            }
        }
    }

    // should compile, do not mark this function as test
    #[allow(dead_code)]
    fn set_works() {
        metric!(set(MySet::Set1) = 43);
        metric!(set(MySet::Set2(23)) = 55);
    }
}
