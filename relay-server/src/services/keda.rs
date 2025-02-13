use crate::MemoryStat;
use relay_system::{AsyncResponse, Controller, FromMessage, Interface, Sender, Service};
use serde::Serialize;

/// Service that tracks internal relay metrics so that they can be exposed.
pub struct KedaService {
    memory_stat: MemoryStat,
    up: u8,
}

impl KedaService {
    pub fn new(memory_stat: MemoryStat) -> Self {
        Self { memory_stat, up: 1 }
    }
}

impl Service for KedaService {
    type Interface = KedaMetrics;

    async fn run(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        let mut shutdown = Controller::shutdown_handle();
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    self.up = 0;
                },
                Some(message) = rx.recv() => {
                    match message {
                        KedaMetrics::Check(sender) => {
                            let memory_usage = self.memory_stat.memory();
                            sender.send(KedaData::new(memory_usage.used_percent(), self.up));
                        }
                    }
                }
            }
        }
    }
}

/// Supported operations within the internal metrics service.
pub enum KedaMessageKind {
    /// Requests the current data from the service.
    Check,
}

/// This mirrors the same messages as [`KedaMessageKind`] but it can be augmented
/// with additional data necessary for the service framework, for example a Sender.
pub enum KedaMetrics {
    Check(Sender<KedaData>),
}

impl Interface for KedaMetrics {}

impl FromMessage<KedaMessageKind> for KedaMetrics {
    type Response = AsyncResponse<KedaData>;

    fn from_message(message: KedaMessageKind, sender: Sender<KedaData>) -> Self {
        match message {
            KedaMessageKind::Check => KedaMetrics::Check(sender),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct KedaData {
    memory_usage: f32,
    up: u8,
}

impl KedaData {
    pub fn new(memory_usage: f32, up: u8) -> Self {
        Self { memory_usage, up }
    }
}
