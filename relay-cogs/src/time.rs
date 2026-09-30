//! Time management for COGS measurements.
//!
//! COGS are derived from measured amount of work, which usually is done by taking a timed measure.
//! To allow different types of time measures other than wall time, this module exposes a [`Clock`]
//! trait.

use std::cell::Cell;
use std::sync::LazyLock;
pub use std::time::Duration;

/// A clock for taking COGS measurements.
pub trait Clock {
    /// Returns a current instant in in time.
    ///
    /// This value must be monotonically increasing but doesn't need to be strictly monotonically
    /// increasing.
    fn now(&self) -> Instant;
}

/// An instant in time.
///
/// This instant can be produced by [`Clock`]. Mixing instants from different clocks is not
/// supported and leads to undefined results but not undefined behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Instant(Duration);

impl Instant {
    /// Creates a new [`Instant`] from a duration.
    ///
    /// The meaning of the duration is up to the caller, usually a specific [`Clock`].
    /// For example a system clock may take offsets from a specific [`std::time::Instant`],
    /// a CPU time measurement may use a duration representing the amount of nano seconds
    /// passed since the start of the task.
    pub fn from_duration(duration: Duration) -> Self {
        Self(duration)
    }

    /// Returns the amount of time elapsed from an earlier instant to this one, or a zero duration
    /// if that instant is later than this one.
    pub fn since(self, earlier: Self) -> Duration {
        self.0.saturating_sub(earlier.0)
    }
}

/// A system [`Clock`] which uses the monotonically increasing [`Instant`] as its source, measuring
/// wall time.
#[derive(Copy, Clone, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        /// A static point in time as a [`std::time::Instant`] which can be used as a common reference
        /// point for all [`std::time::Instant`] instances created.
        ///
        /// This allows a consistent conversion from a [`std::time::Instant`] to [`Duration`].
        static REFERENCE_INSTANT: LazyLock<std::time::Instant> =
            LazyLock::new(std::time::Instant::now);

        let reference = *REFERENCE_INSTANT;
        let now = std::time::Instant::now();
        Instant::from_duration(
            now.checked_duration_since(reference)
                .unwrap_or(Duration::ZERO),
        )
    }
}

/// A [`Clock`] which can be set to any value.
#[derive(Clone, Debug, Default)]
pub struct StaticClock(Cell<Duration>);

impl StaticClock {
    /// Creates a new [`StaticClock`] initialized to `start`.
    ///
    /// # Example:
    ///
    /// ```
    /// # use relay_cogs::time::{Clock, StaticClock, Duration, Instant};
    /// let clock = StaticClock::new(Duration::from_millis(123));
    /// assert_eq!(clock.now(), Instant::from_duration(Duration::from_millis(123)));
    /// ```
    pub fn new(start: Duration) -> Self {
        Self(Cell::new(start))
    }

    /// Advances the clock by `duration`.
    ///
    /// # Example:
    ///
    /// ```
    /// # use relay_cogs::time::{Clock, StaticClock, Duration, Instant};
    /// let clock = StaticClock::default();
    /// assert_eq!(clock.now(), Instant::from_duration(Duration::ZERO));
    /// clock.advance(Duration::from_millis(50));
    /// assert_eq!(clock.now(), Instant::from_duration(Duration::from_millis(50)));
    /// ```
    pub fn advance(&self, duration: Duration) {
        self.0.set(self.0.get() + duration);
    }

    /// Advances the clock by `millis` milliseconds.
    ///
    /// This is a convenience method for [`Self::advance`].
    ///
    /// # Example:
    ///
    /// ```
    /// # use relay_cogs::time::{Clock, StaticClock, Duration, Instant};
    /// let clock = StaticClock::default();
    /// assert_eq!(clock.now(), Instant::from_duration(Duration::ZERO));
    /// clock.advance_millis(50);
    /// assert_eq!(clock.now(), Instant::from_duration(Duration::from_millis(50)));
    /// ```
    #[cfg(test)]
    pub fn advance_millis(&self, millis: u64) {
        self.advance(Duration::from_millis(millis))
    }
}

impl Clock for StaticClock {
    fn now(&self) -> Instant {
        (&self).now()
    }
}

impl Clock for &StaticClock {
    fn now(&self) -> Instant {
        Instant::from_duration(self.0.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_clock_increasing() {
        // System clocks are globally monotonically increasing.
        let clock1 = SystemClock;
        let clock2 = SystemClock;

        let a = clock1.now();
        let b = clock1.now();
        let c = clock2.now();
        let d = clock1.now();

        assert!(a <= b);
        assert!(b <= c);
        assert!(c <= d);
        assert!(a <= d);
    }
}
