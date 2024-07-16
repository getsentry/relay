use relay_config::Config;
use std::fmt;
use std::fmt::Formatter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use sysinfo::System;

/// Count after which the [`MemoryStat`] data will be refreshed.
const UPDATE_TIME_THRESHOLD_SECONDS: f64 = 0.1;

struct MemoryStatInner {
    pub used: u64,
    pub total: u64,
}

impl MemoryStatInner {
    fn new() -> Self {
        Self { used: 0, total: 0 }
    }

    fn used_percent(&self) -> f32 {
        if self.total == 0 {
            return 0.0;
        }

        (self.used as f32 / self.total as f32).clamp(0.0, 1.0)
    }
}

#[derive(Clone)]
pub struct MemoryStat {
    data: Arc<RwLock<MemoryStatInner>>,
    last_update: Arc<AtomicU64>,
    reference_time: Instant,
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
            last_update: Arc::new(AtomicU64::new(0)),
            reference_time: Instant::now(),
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
        let last_update = self.last_update.load(Ordering::Relaxed);
        let elapsed_time = self.reference_time.elapsed().as_secs_f64();

        if elapsed_time - (last_update as f64) < UPDATE_TIME_THRESHOLD_SECONDS {
            return;
        }

        let Ok(mut data) = self.data.write() else {
            return;
        };

        let Ok(_) = self.last_update.compare_exchange_weak(
            last_update,
            elapsed_time as u64,
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
    use crate::utils::MemoryStat;
    use relay_config::Config;
    use std::sync::Arc;

    #[test]
    fn test_has_enough_memory() {
        let config = Config::from_json_value(serde_json::json!({
            "health": {
                "max_memory_percent": 0.95
            }
        }))
        .unwrap();

        let memory_stat = MemoryStat::new(Arc::new(config));
        assert!(memory_stat.has_enough_memory());
    }

    #[test]
    fn test_has_not_enough_memory() {
        let config = Config::from_json_value(serde_json::json!({
            "health": {
                "max_memory_percent": 0.0
            }
        }))
        .unwrap();

        let memory_stat = MemoryStat::new(Arc::new(config));
        assert!(!memory_stat.has_enough_memory());
    }
}
