use relay_system::{AsyncResponse, FromMessage, Interface, Sender, Service};
use serde::Serialize;
use std::time::{Duration, Instant};

const UTILIZATION_WINDOW_SECONDS: u64 = 1;

/// Service that tracks internal relay metrics so that they can be exposed.
pub struct RelayMetricsService {
    /// The total busy time within a utilization window.
    busy_time: u64,
    /// The last calculated utilization as percentage.
    processor_utilization: f64,
    /// The moment the last utilization calculation happened.
    last_utilization_check: Instant,
}

impl RelayMetricsService {
    pub fn new() -> Self {
        Self {
            busy_time: 0,
            processor_utilization: 0.0,
            last_utilization_check: Instant::now(),
        }
    }
}

impl Service for RelayMetricsService {
    type Interface = InternalMetrics;

    async fn run(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        while let Some(message) = rx.recv().await {
            match message {
                InternalMetrics::Check(sender) => {
                    sender.send(InternalMetricsData::new(self.processor_utilization));
                }
                InternalMetrics::ProcessorBusyTime(busy_time) => {
                    self.busy_time += busy_time.as_micros() as u64;
                    let elapsed = self.last_utilization_check.elapsed();
                    if elapsed.as_secs() >= UTILIZATION_WINDOW_SECONDS {
                        self.processor_utilization =
                            self.busy_time as f64 / elapsed.as_micros() as f64;
                        self.busy_time = 0;
                        self.last_utilization_check = Instant::now();
                    }
                }
            }
        }
    }
}

/// Supported operations within the internal metrics service.
pub enum InternalMetricsMessageKind {
    /// Requests the current data from the service.
    Check,
    /// Reports the busy time of the processor.
    ProcessorBusyTime(Duration),
}

/// This mirrors the same messages as [`InternalMetricsMessageKind`] but it can be augmented
/// with additional data necessary for the service framework, for example a Sender.
pub enum InternalMetrics {
    Check(Sender<InternalMetricsData>),

    ProcessorBusyTime(Duration),
}

impl Interface for InternalMetrics {}

impl FromMessage<InternalMetricsMessageKind> for InternalMetrics {
    type Response = AsyncResponse<InternalMetricsData>;

    fn from_message(
        message: InternalMetricsMessageKind,
        sender: Sender<InternalMetricsData>,
    ) -> Self {
        match message {
            InternalMetricsMessageKind::Check => InternalMetrics::Check(sender),
            InternalMetricsMessageKind::ProcessorBusyTime(busy_time) => {
                InternalMetrics::ProcessorBusyTime(busy_time)
            }
        }
    }
}

#[derive(Debug, Serialize)]
pub struct InternalMetricsData {
    processor_utilization: f64,
}

impl InternalMetricsData {
    pub fn new(processor_utilization: f64) -> Self {
        Self {
            processor_utilization,
        }
    }
}
