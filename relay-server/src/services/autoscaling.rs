use crate::services::buffer::PartitionedEnvelopeBuffer;
use crate::services::processor::EnvelopeProcessorServicePool;
use crate::MemoryStat;
use relay_system::{
    AsyncResponse, Controller, FromMessage, Handle, Interface, RuntimeMetrics, Sender, Service,
};
use tokio::time::Instant;

/// Service that tracks internal relay metrics so that they can be exposed.
pub struct AutoscalingMetricService {
    /// For exposing internal memory usage of relay.
    memory_stat: MemoryStat,
    /// Reference to the spooler to get item count and total used size.
    envelope_buffer: PartitionedEnvelopeBuffer,
    /// Runtime handle to expose service utilization metrics.
    handle: Handle,
    /// Gives access to runtime metrics.
    runtime_metrics: RuntimeMetrics,
    /// The last time the runtime utilization was checked.
    last_runtime_check: Instant,
    /// This will always report `1` unless the instance is shutting down.
    up: u8,
    /// Gives access to AsyncPool metrics.
    async_pool: EnvelopeProcessorServicePool,
}

impl AutoscalingMetricService {
    pub fn new(
        memory_stat: MemoryStat,
        envelope_buffer: PartitionedEnvelopeBuffer,
        handle: Handle,
        async_pool: EnvelopeProcessorServicePool,
    ) -> Self {
        let runtime_metrics = handle.metrics();
        Self {
            memory_stat,
            envelope_buffer,
            handle,
            runtime_metrics,
            last_runtime_check: Instant::now(),
            async_pool,
            up: 1,
        }
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
                            let metrics = self.handle
                                .current_services_metrics()
                                .iter()
                                .map(|(id, metric)| ServiceUtilization(id.name(), metric.utilization))
                                .collect();
                            let worker_pool_utilization = self.async_pool.metrics().utilization() as u8;
                            let runtime_utilization = self.runtime_utilization();

                            sender.send(AutoscalingData {
                                memory_usage: memory_usage.used_percent(),
                                up: self.up,
                                total_size: self.envelope_buffer.total_storage_size(),
                                item_count: self.envelope_buffer.item_count(),
                                services_metrics: metrics,
                                worker_pool_utilization,
                                runtime_utilization
                            });
                        }
                    }
                }
            }
        }
    }
}

impl AutoscalingMetricService {
    fn runtime_utilization(&mut self) -> u8 {
        let last_checked = self.last_runtime_check.elapsed().as_secs_f64();
        // Prevent division by 0 in case it's checked in rapid succession.
        if last_checked < 0.001 {
            return 0;
        }
        let avg_utilization = (0..self.runtime_metrics.num_workers())
            .into_iter()
            .map(|worker_id| self.runtime_metrics.worker_total_busy_duration(worker_id))
            .map(|busy| busy.as_secs_f64())
            .sum::<f64>()
            / last_checked
            / (self.runtime_metrics.num_workers() as f64);

        self.last_runtime_check = Instant::now();

        (avg_utilization * 100.0).min(100.0) as u8
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

pub struct AutoscalingData {
    pub memory_usage: f32,
    pub up: u8,
    pub total_size: u64,
    pub item_count: u64,
    pub worker_pool_utilization: u8,
    pub services_metrics: Vec<ServiceUtilization>,
    pub runtime_utilization: u8,
}

pub struct ServiceUtilization(pub &'static str, pub u8);
