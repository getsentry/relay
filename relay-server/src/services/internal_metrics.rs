use crate::MemoryStat;
use ahash::{HashMap, HashMapExt};
use relay_system::{AsyncResponse, FromMessage, Interface, Sender, Service};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Service that tracks internal relay stats and exposes them.
pub struct RelayMetricsService {
    memory_stat: MemoryStat,
    /// A single number value only for testing
    spooled_envelopes: AtomicU64,

    busy_time: u64,

    processor_utilization: f64,

    last_utilization_check: Instant,

    tags: HashMap<String, String>,
}

impl RelayMetricsService {
    pub fn new(memory_stat: MemoryStat) -> Self {
        Self {
            memory_stat,
            spooled_envelopes: AtomicU64::new(0),
            busy_time: 0,
            processor_utilization: 0.0,
            last_utilization_check: Instant::now(),
            tags: HashMap::new(),
        }
    }
}

impl Service for RelayMetricsService {
    type Interface = InternalMetricsMessage;

    async fn run(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        while let Some(message) = rx.recv().await {
            match message {
                InternalMetricsMessage::Check(sender) => {
                    let memory = self.memory_stat.memory();
                    let memory_percentage =
                        ((memory.used as f64 / memory.total as f64) * 100.0) as u64;
                    sender.send(KedaMetricsData::new(
                        memory_percentage,
                        self.spooled_envelopes.load(Ordering::Acquire),
                        self.processor_utilization,
                    ));
                }
                InternalMetricsMessage::EnvelopePush => {
                    dbg!("PUSH received");
                    self.spooled_envelopes.fetch_add(1, Ordering::Relaxed);
                }
                InternalMetricsMessage::EnvelopePop => {
                    dbg!("POP received");
                    self.spooled_envelopes.fetch_sub(1, Ordering::Relaxed);
                }
                InternalMetricsMessage::ProcessorBusyTime(busy_time) => {
                    self.busy_time += busy_time.as_micros() as u64;
                    let elapsed = self.last_utilization_check.elapsed();
                    // if combined time is greater than 5 seconds then we reset it and calculate
                    // the utilization
                    if elapsed.as_secs() >= 5 {
                        dbg!(self.busy_time);
                        dbg!(elapsed.as_micros());
                        self.processor_utilization =
                            self.busy_time as f64 / elapsed.as_micros() as f64;
                        dbg!(self.processor_utilization);
                        self.busy_time = 0;
                        self.last_utilization_check = Instant::now();
                    }
                }
            }
        }
    }
}

/// Kind of message that is supported by the internal metrics service
/// One can either query all stored data or modify it by passing messages.
pub enum KedaMetricsMessageKind {
    Check,

    EnvelopePush,

    EnvelopePop,

    ProcessorBusyTime(Duration),
}

/// Wrapper for internal messages that will either modify the value or return all stored values.
/// Just storing value does not need to wait for a reply, we just want to store it and
/// have someone retrieve it at a later point in time.
pub enum InternalMetricsMessage {
    Check(Sender<KedaMetricsData>),

    EnvelopePush,

    EnvelopePop,

    ProcessorBusyTime(Duration),
}

impl Interface for InternalMetricsMessage {}

impl FromMessage<KedaMetricsMessageKind> for InternalMetricsMessage {
    type Response = AsyncResponse<KedaMetricsData>;

    fn from_message(message: KedaMetricsMessageKind, sender: Sender<KedaMetricsData>) -> Self {
        match message {
            KedaMetricsMessageKind::Check => InternalMetricsMessage::Check(sender),
            KedaMetricsMessageKind::EnvelopePop => InternalMetricsMessage::EnvelopePop,
            KedaMetricsMessageKind::EnvelopePush => InternalMetricsMessage::EnvelopePush,
            KedaMetricsMessageKind::ProcessorBusyTime(busy_time) => {
                InternalMetricsMessage::ProcessorBusyTime(busy_time)
            }
        }
    }
}

#[derive(Debug, Serialize)]
pub struct KedaMetricsData {
    /// Memory usage percentage as integer. e.g. 72
    memory_usage_percentage: u64,

    spooled_envelopes: u64,

    processor_utilization: f64,
}

impl KedaMetricsData {
    pub fn new(memory_usage_percentage: u64, value: u64, processor_utilization: f64) -> Self {
        Self {
            memory_usage_percentage,
            spooled_envelopes: value,
            processor_utilization,
        }
    }
}
