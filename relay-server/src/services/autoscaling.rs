use crate::MemoryStat;
use relay_system::{AsyncResponse, Controller, FromMessage, Interface, Sender, Service};
use serde::Serialize;

/// Service that tracks internal relay metrics so that they can be exposed.
pub struct AutoscalingMetricService {
    memory_stat: MemoryStat,
    up: u8,
}

impl AutoscalingMetricService {
    pub fn new(memory_stat: MemoryStat) -> Self {
        Self { memory_stat, up: 1 }
    }
}

impl Service for AutoscalingMetricService {
    type Interface = AutoscalingMetrics;

    async fn run(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        let mut shutdown = Controller::shutdown_handle();
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    self.up = 0;
                },
                Some(message) = rx.recv() => {
                    match message {
                        AutoscalingMetrics::Check(sender) => {
                            let memory_usage = self.memory_stat.memory();
                            sender.send(AutoscalingData::new(memory_usage.used_percent(), self.up));
                        }
                    }
                }
            }
        }
    }
}

/// Supported operations within the internal metrics service.
pub enum AutoscalingMessageKind {
    /// Requests the current data from the service.
    Check,
}

/// This mirrors the same messages as [`AutoscalingMessageKind`] but it can be augmented
/// with additional data necessary for the service framework, for example a Sender.
pub enum AutoscalingMetrics {
    Check(Sender<AutoscalingData>),
}

impl Interface for AutoscalingMetrics {}

impl FromMessage<AutoscalingMessageKind> for AutoscalingMetrics {
    type Response = AsyncResponse<AutoscalingData>;

    fn from_message(message: AutoscalingMessageKind, sender: Sender<AutoscalingData>) -> Self {
        match message {
            AutoscalingMessageKind::Check => AutoscalingMetrics::Check(sender),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct AutoscalingData {
    pub memory_usage: f32,
    pub up: u8,
}

impl AutoscalingData {
    pub fn new(memory_usage: f32, up: u8) -> Self {
        Self { memory_usage, up }
    }
}
