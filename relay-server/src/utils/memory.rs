use relay_config::Config;
use std::sync::{Arc, Mutex};

struct MemoryStatInner {
    pub lock: Mutex<()>,
    pub used: u64,
    pub total: u64,
}

impl MemoryStatInner {
    fn new() -> Self {
        Self {
            lock: Mutex::new(()),
            used: 0,
            total: 0,
        }
    }
}

#[derive(Clone)]
pub struct MemoryStat {
    inner: Arc<MemoryStatInner>,
    config: Arc<Config>,
}

impl MemoryStat {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            inner: Arc::new(MemoryStatInner::new()),
            config: config.clone(),
        }
    }
}
