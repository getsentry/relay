use crate::statsd::RelayGauges;
use arc_swap::ArcSwap;
use relay_config::Config;
use relay_statsd::metric;
use std::fmt;
use std::fmt::Formatter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use sysinfo::{MemoryRefreshKind, System};

/// Count after which the [`MemoryStat`] data will be refreshed.
const UPDATE_TIME_THRESHOLD_MS: u64 = 100;

/// The representation of the current memory state.
#[derive(Clone, Copy, Debug)]
pub struct Memory {
    /// Used memory.
    ///
    /// This measure of used memory represents the Resident Set Size (RSS) which represents the
    /// amount of physical memory that a process has in the main memory that does not correspond
    /// to anything on disk.
    pub used: u64,
    /// Total memory.
    pub total: u64,
}

impl Memory {
    /// Returns the percentage amount of used memory in the interval [0.0, 1.0].
    ///
    /// The percentage measurement will return 1.0 in the following edge cases:
    /// - When total is 0
    /// - When used / total produces a NaN
    pub fn used_percent(&self) -> f32 {
        let used_percent = self.used as f32 / self.total as f32;
        if used_percent.is_nan() {
            return 1.0;
        };

        used_percent.clamp(0.0, 1.0)
    }
}

/// Inner struct that holds the latest [`Memory`] state which is polled at least every 100ms.
///
/// The goal of this implementation is to offer lock-free reading to any arbitrary number of threads
/// while at the same time, reducing to the minimum the need for locking when memory stats need to
/// be updated.
///
/// Because of how the implementation is designed, there is a very small chance that multiple
/// threads are waiting on the lock that guards [`System`]. The only case in which there might be
/// multiple threads waiting on the lock, is if a thread holds the lock for more than
/// [`UPDATE_TIME_THRESHOLD_MS`] and a new thread comes and updates the `last_update` and tries
/// to acquire the lock to perform another memory reading. Since the reading of [`System`] is much
/// faster than [`UPDATE_TIME_THRESHOLD_MS`] this should not happen in the real world.
struct Inner {
    memory: ArcSwap<Memory>,
    last_update: AtomicU64,
    reference_time: Instant,
    system: Mutex<System>,
}

/// Wrapper around [`Inner`] which hides the [`Arc`] and exposes utils method to make working with
/// [`MemoryStat`] as opaque as possible.
#[derive(Clone)]
pub struct MemoryStat(Arc<Inner>);

impl MemoryStat {
    /// Creates an instance of [`MemoryStat`] and obtains the current memory readings from
    /// [`System`].
    pub fn new() -> Self {
        // sysinfo docs suggest to use a single instance of `System` across the program.
        let mut system = System::new();
        Self(Arc::new(Inner {
            memory: ArcSwap::from(Arc::new(Self::refresh_memory(&mut system))),
            last_update: AtomicU64::new(0),
            reference_time: Instant::now(),
            system: Mutex::new(system),
        }))
    }

    /// Returns a copy of the most up to date [`Memory`] data.
    pub fn memory(&self) -> Memory {
        self.try_update();
        **self.0.memory.load()
    }

    /// Builds a [`MemoryChecker`] which holds a reference to the supplied [`Config`] and the
    /// current [`MemoryStat`].
    pub fn init_checker(&self, config: Arc<Config>) -> MemoryChecker {
        MemoryChecker {
            memory_stat: self.clone(),
            config,
        }
    }

    /// Refreshes the memory readings.
    fn refresh_memory(system: &mut System) -> Memory {
        system.refresh_memory_specifics(MemoryRefreshKind::new().with_ram());
        let memory = match system.cgroup_limits() {
            Some(cgroup) => Memory {
                used: cgroup.rss,
                total: cgroup.total_memory,
            },
            None => Memory {
                used: system.used_memory(),
                total: system.total_memory(),
            },
        };

        metric!(gauge(RelayGauges::SystemMemoryUsed) = memory.used);
        metric!(gauge(RelayGauges::SystemMemoryTotal) = memory.total);

        memory
    }

    /// Updates the memory readings if at least [`UPDATE_TIME_THRESHOLD_MS`] has passed.
    fn try_update(&self) {
        let last_update = self.0.last_update.load(Ordering::Relaxed);
        let elapsed_time = self.0.reference_time.elapsed().as_millis() as u64;

        if elapsed_time - last_update < UPDATE_TIME_THRESHOLD_MS {
            return;
        }

        if self.0.last_update.compare_exchange_weak(
            last_update,
            elapsed_time,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ).is_err() {
            return;
        }

        let mut system = self
            .0
            .system
            .lock()
            .unwrap_or_else(|system| system.into_inner());

        let updated_memory = Self::refresh_memory(&mut system);
        self.0.memory.store(Arc::new(updated_memory));
    }
}

impl fmt::Debug for MemoryStat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MemoryStat")
    }
}

impl Default for MemoryStat {
    fn default() -> Self {
        Self::new()
    }
}

/// Enum representing the two different states of a memory check.
pub enum MemoryCheck {
    /// The memory usage is below the specified thresholds.
    Below(Memory),
    /// The memory usage exceeds the specified thresholds.
    Exceeded(Memory),
}

impl MemoryCheck {
    /// Returns `true` if [`MemoryCheck`] is of variant [`MemoryCheck::Below`].
    pub fn is_below(&self) -> bool {
        matches!(self, Self::Below(_))
    }

    /// Returns `true` if [`MemoryCheck`] is of variant [`MemoryCheck::Exceeded`].
    pub fn is_exceeded(&self) -> bool {
        !self.is_below()
    }
}

/// Struct that composes a [`Config`] and [`MemoryStat`] and provides utility methods to validate
/// whether memory is within limits.
///
/// The rationale behind such struct, is to be able to share across Relay the same logic for dealing
/// with memory readings. It's decoupled from [`MemoryStat`] because it's just a layer on top that
/// decides how memory readings are interpreted.
#[derive(Clone, Debug)]
pub struct MemoryChecker {
    pub memory_stat: MemoryStat,
    config: Arc<Config>,
}

impl MemoryChecker {
    /// Checks if the used percentage of memory is below the specified threshold.
    pub fn check_memory_percent(&self) -> MemoryCheck {
        let memory = self.memory_stat.memory();
        if memory.used_percent() < self.config.health_max_memory_watermark_percent() {
            return MemoryCheck::Below(memory);
        }

        MemoryCheck::Exceeded(memory)
    }

    /// Checks if the used memory (in bytes) is below the specified threshold.
    pub fn check_memory_bytes(&self) -> MemoryCheck {
        let memory = self.memory_stat.memory();
        if memory.used < self.config.health_max_memory_watermark_bytes() {
            return MemoryCheck::Below(memory);
        }

        MemoryCheck::Exceeded(memory)
    }

    /// Checks if the used memory is below both percentage and bytes thresholds.
    ///
    /// This is the function that should be mainly used for checking whether of not Relay has
    /// enough memory.
    pub fn check_memory(&self) -> MemoryCheck {
        let memory = self.memory_stat.memory();
        if memory.used_percent() < self.config.health_max_memory_watermark_percent()
            && memory.used < self.config.health_max_memory_watermark_bytes()
        {
            return MemoryCheck::Below(memory);
        }

        MemoryCheck::Exceeded(memory)
    }
}

#[cfg(test)]
mod tests {
    use relay_config::Config;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use crate::utils::memory::UPDATE_TIME_THRESHOLD_MS;
    use crate::utils::{Memory, MemoryStat};

    #[test]
    fn test_memory_used_percent_both_0() {
        let memory = Memory { used: 0, total: 0 };
        assert_eq!(memory.used_percent(), 1.0);
    }

    #[test]
    fn test_memory_used_percent_total_0() {
        let memory = Memory {
            used: 100,
            total: 0,
        };
        assert_eq!(memory.used_percent(), 1.0);
    }

    #[test]
    fn test_memory_used_percent_zero() {
        let memory = Memory {
            used: 0,
            total: 100,
        };
        assert_eq!(memory.used_percent(), 0.0);
    }

    #[test]
    fn test_memory_used_percent_half() {
        let memory = Memory {
            used: 50,
            total: 100,
        };
        assert_eq!(memory.used_percent(), 0.5);
    }

    #[test]
    fn test_memory_checker() {
        let config = Config::from_json_value(serde_json::json!({
            "health": {
                "max_memory_percent": 1.0
            }
        }))
        .unwrap();
        let memory_checker = MemoryStat::new().init_checker(Arc::new(config));
        assert!(memory_checker.check_memory().is_below());

        let config = Config::from_json_value(serde_json::json!({
            "health": {
                "max_memory_percent": 0.0
            }
        }))
        .unwrap();
        let memory_checker = MemoryStat::new().init_checker(Arc::new(config));
        assert!(memory_checker.check_memory().is_exceeded());
    }

    #[test]
    fn test_last_update_is_updated() {
        let memory = MemoryStat::new();
        let first_update = memory.0.last_update.load(Ordering::Relaxed);

        sleep(Duration::from_millis(UPDATE_TIME_THRESHOLD_MS + 10));

        memory.memory();
        let second_update = memory.0.last_update.load(Ordering::Relaxed);

        assert!(first_update <= second_update);
    }
}
