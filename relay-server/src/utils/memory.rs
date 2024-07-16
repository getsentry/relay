use std::fmt;
use std::fmt::Formatter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use sysinfo::{MemoryRefreshKind, System};

/// Count after which the [`MemoryStat`] data will be refreshed.
const UPDATE_TIME_THRESHOLD_SECONDS: f64 = 0.1;

#[derive(Clone, Copy)]
struct Memory {
    pub used: u64,
    pub total: u64,
}

impl Memory {
    fn used_percent(&self) -> f32 {
        if self.total == 0 {
            return 0.0;
        }

        (self.used as f32 / self.total as f32).clamp(0.0, 1.0)
    }
}

struct Inner {
    memory: RwLock<Memory>,
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
            memory: RwLock::new(Self::build_data(&mut system)),
            last_update: AtomicU64::new(0),
            reference_time: Instant::now(),
            max_percent_threshold,
            system: Mutex::new(system),
        }))
    }

    pub fn has_enough_memory(&self) -> bool {
        // If we succeeded in updating the memory readings, we just return a copy of the newly read
        // limits to avoid acquiring the read lock in the subsequent code.
        if let Some(memory) = self.try_update() {
            return memory.used_percent() < self.0.max_percent_threshold;
        };

        let Ok(memory_lock) = self.0.memory.read() else {
            return false;
        };

        memory_lock.used_percent() < self.0.max_percent_threshold
    }

    fn build_data(system: &mut System) -> Memory {
        system.refresh_memory_specifics(MemoryRefreshKind::new().with_ram());
        match system.cgroup_limits() {
            Some(cgroup) => Memory {
                used: cgroup.rss,
                total: cgroup.total_memory,
            },
            None => Memory {
                used: system.used_memory(),
                total: system.total_memory(),
            },
        }
    }

    fn try_update(&self) -> Option<Memory> {
        let last_update = self.0.last_update.load(Ordering::Relaxed);
        let elapsed_time = self.0.reference_time.elapsed().as_secs_f64();

        if elapsed_time - (last_update as f64) < UPDATE_TIME_THRESHOLD_SECONDS {
            return None;
        }

        let (Ok(mut memory), Ok(mut system)) = (self.0.memory.write(), self.0.system.lock()) else {
            return None;
        };

        let Ok(_) = self.0.last_update.compare_exchange_weak(
            last_update,
            elapsed_time as u64,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) else {
            return None;
        };

        let updated_memory = Self::build_data(&mut system);
        *memory = updated_memory;

        Some(updated_memory)
    }
}

impl fmt::Debug for MemoryStat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MemoryStat")
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::MemoryStat;

    #[test]
    fn test_has_enough_memory() {
        let memory_stat = MemoryStat::new(0.95);
        assert!(memory_stat.has_enough_memory());
    }

    #[test]
    fn test_has_not_enough_memory() {
        let memory_stat = MemoryStat::new(0.0);
        assert!(!memory_stat.has_enough_memory());
    }
}
