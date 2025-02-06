use crate::MemoryStat;
use ahash::{HashMap, HashMapExt};
use relay_system::{AsyncResponse, FromMessage, Interface, Sender, Service};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

/// Service that tracks internal relay stats and exposes them.
pub struct RelayMetricsService {
    memory_stat: MemoryStat,
    /// A single number value only for testing
    value: AtomicU64,

    tags: HashMap<String, String>,
}

impl RelayMetricsService {
    pub fn new(memory_stat: MemoryStat) -> Self {
        Self {
            memory_stat,
            value: AtomicU64::new(3),
            tags: HashMap::new(),
        }
    }
}

impl Service for RelayMetricsService {
    type Interface = InternalMetricsMessage;

    async fn run(self, mut rx: relay_system::Receiver<Self::Interface>) {
        while let Some(message) = rx.recv().await {
            match message {
                InternalMetricsMessage::Check(sender) => {
                    let memory = self.memory_stat.memory();
                    let memory_percentage =
                        ((memory.used as f64 / memory.total as f64) * 100.0) as u64;
                    sender.send(KedaMetricsData::new(
                        memory_percentage,
                        self.value.load(Ordering::Relaxed),
                    ));
                }
                InternalMetricsMessage::Add => {
                    self.value.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }
    }
}

/// Kind of message that is supported by the internal metrics service
/// One can either query all stored data or modify it by passing messages.
pub enum KedaMetricsMessageKind {
    Check,

    Add,

    InflightRequestsAdd,

    InflightRequestsSub,
}

/// Wrapper for internal messages that will either modify the value or return all stored values.
/// Just storing value does not need to wait for a reply, we just want to store it and
/// have someone retrieve it at a later point in time.
pub enum InternalMetricsMessage {
    Check(Sender<KedaMetricsData>),

    Add,
}

impl Interface for InternalMetricsMessage {}

impl FromMessage<KedaMetricsMessageKind> for InternalMetricsMessage {
    type Response = AsyncResponse<KedaMetricsData>;

    fn from_message(message: KedaMetricsMessageKind, sender: Sender<KedaMetricsData>) -> Self {
        match message {
            KedaMetricsMessageKind::Check => InternalMetricsMessage::Check(sender),
            KedaMetricsMessageKind::Add => InternalMetricsMessage::Add,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct KedaMetricsData {
    /// Memory usage percentage as integer. e.g. 72
    memory_usage_percentage: u64,

    value: u64,
}

impl KedaMetricsData {
    pub fn new(memory_usage_percentage: u64, value: u64) -> Self {
        Self {
            memory_usage_percentage,
            value,
        }
    }

    pub fn safe_for_shutdown(&self) -> bool {
        self.value == 0
    }
}
