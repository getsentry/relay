use relay_config::Config;
use std::fmt;
use std::fmt::Formatter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use sysinfo::System;

/// Count after which the [`MemoryStat`] data will be refreshed.
const UPDATE_COUNT_THRESHOLD: u64 = 1000;

struct MemoryStatInner {
    pub used: u64,
    pub total: u64,
}

impl MemoryStatInner {
    fn new() -> Self {
        Self { used: 0, total: 0 }
    }

    fn used_percent(&self) -> f32 {
        (self.used as f32 / self.total as f32).clamp(0.0, 1.0)
    }
}

#[derive(Clone)]
pub struct MemoryStat {
    data: Arc<RwLock<MemoryStatInner>>,
    current_count: Arc<AtomicU64>,
    config: Arc<Config>,
    system: Arc<System>,
}

impl fmt::Debug for MemoryStat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MemoryStat")
    }
}

impl MemoryStat {
    pub fn new(config: Arc<Config>) -> Self {
        // sysinfo docs suggest to use a single instance of `System` across the program.
        let system = System::new();
        Self {
            data: Arc::new(RwLock::new(Self::build_data(&system))),
            current_count: Arc::new(AtomicU64::new(0)),
            config: config.clone(),
            system: Arc::new(system),
        }
    }

    pub fn has_enough_memory(&self) -> bool {
        self.try_update();

        // TODO: we could make an optimization that if we already updated the memory, we can avoid
        //  acquiring a read lock and just evaluate the bottom expression on the newly added data.
        let Ok(data) = self.data.read() else {
            return false;
        };

        data.used_percent() < self.config.health_max_memory_watermark_percent()
    }

    pub fn increment(&self) {
        self.current_count.fetch_add(1, Ordering::Relaxed);
    }

    fn build_data(system: &System) -> MemoryStatInner {
        match system.cgroup_limits() {
            Some(cgroup) => MemoryStatInner {
                used: cgroup.rss,
                total: cgroup.total_memory,
            },
            None => MemoryStatInner {
                used: system.used_memory(),
                total: system.total_memory(),
            },
        }
    }

    fn try_update(&self) {
        let current_count = self.current_count.load(Ordering::Relaxed);

        if current_count < UPDATE_COUNT_THRESHOLD {
            return;
        }

        let Ok(mut data) = self.data.write() else {
            return;
        };

        let Ok(_) = self.current_count.compare_exchange_weak(
            current_count,
            0,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) else {
            return;
        };

        *data = Self::build_data(&self.system);
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_data() {}
}
