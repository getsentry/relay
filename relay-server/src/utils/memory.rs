use crate::statsd::RelayGauges;
use arc_swap::ArcSwap;
use relay_statsd::metric;
use std::fmt;
use std::fmt::Formatter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use sysinfo::{MemoryRefreshKind, System};

/// Count after which the [`MemoryStat`] data will be refreshed.
const UPDATE_TIME_THRESHOLD_MS: u64 = 100;

#[derive(Clone, Copy)]
pub struct Memory {
    pub used: u64,
    pub total: u64,
}

impl Memory {
    pub fn used_percent(&self) -> f32 {
        if self.total == 0 {
            return 0.0;
        }

        (self.used as f32 / self.total as f32).clamp(0.0, 1.0)
    }
}

struct Inner {
    memory: ArcSwap<Memory>,
    last_update: AtomicU64,
    reference_time: Instant,
    max_percent_threshold: f32,
    system: Mutex<System>,
}

#[derive(Clone)]
pub struct MemoryStat(Arc<Inner>);

impl MemoryStat {
    pub fn new(max_percent_threshold: f32) -> Self {
        // sysinfo docs suggest to use a single instance of `System` across the program.
        let mut system = System::new();
        Self(Arc::new(Inner {
            memory: ArcSwap::from(Arc::new(Self::refresh_memory(&mut system))),
            last_update: AtomicU64::new(0),
            reference_time: Instant::now(),
            max_percent_threshold,
            system: Mutex::new(system),
        }))
    }

    pub fn memory(&self) -> Memory {
        self.try_update();
        **self.0.memory.load()
    }

    pub fn has_enough_memory(&self) -> bool {
        self.memory().used_percent() < self.0.max_percent_threshold
    }

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

    fn try_update(&self) {
        let last_update = self.0.last_update.load(Ordering::Relaxed);
        let elapsed_time = self.0.reference_time.elapsed().as_millis() as u64;

        if elapsed_time - last_update < UPDATE_TIME_THRESHOLD_MS {
            return;
        }

        let Ok(mut system) = self.0.system.lock() else {
            return;
        };

        let Ok(_) = self.0.last_update.compare_exchange_weak(
            last_update,
            elapsed_time,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) else {
            return;
        };

        let updated_memory = Self::refresh_memory(&mut system);
        self.0.memory.store(Arc::new(updated_memory));
    }
}

impl fmt::Debug for MemoryStat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MemoryStat")
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::{Memory, MemoryStat};

    #[test]
    fn test_has_enough_memory() {
        let memory_stat = MemoryStat::new(1.0);
        assert!(memory_stat.has_enough_memory());
    }

    #[test]
    fn test_has_not_enough_memory() {
        let memory_stat = MemoryStat::new(0.0);
        assert!(!memory_stat.has_enough_memory());
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
}
