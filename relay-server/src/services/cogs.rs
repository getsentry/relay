use std::sync::atomic::{AtomicBool, Ordering};

use relay_cogs::{CogsMeasurement, CogsRecorder, ResourceId};
use relay_config::Config;
use relay_system::{Addr, Controller, FromMessage, Interface, Service};
use sentry_usage_accountant::{Producer, UsageAccountant, UsageUnit};

use crate::services::store::{Store, StoreCogs};

pub struct CogsReport(CogsMeasurement);

impl Interface for CogsReport {}

impl FromMessage<CogsMeasurement> for CogsReport {
    type Response = relay_system::NoResponse;

    fn from_message(message: CogsMeasurement, _: ()) -> Self {
        Self(message)
    }
}

/// Relay [accountant producer](Producer) which produces to a dynamic
/// upstram.
enum RelayProducer {
    /// Produces to Kafka via the store service.
    #[cfg(feature = "processing")]
    Store(Addr<Store>),
    /// Discards all measurements.
    Noop,
}

impl Producer for RelayProducer {
    type Error = std::convert::Infallible;

    fn send(&mut self, message: sentry_usage_accountant::Message) -> Result<(), Self::Error> {
        match self {
            #[cfg(feature = "processing")]
            Self::Store(addr) => addr.send(StoreCogs(message)),
            Self::Noop => {}
        }

        Ok(())
    }
}

/// Service implementing the [`CogsReport`] interface.
pub struct CogsService {
    relay_resource_id: String,
    usage_accountant: UsageAccountant<RelayProducer>,
}

impl CogsService {
    pub fn new(config: &Config, #[cfg(feature = "processing")] store: Option<Addr<Store>>) -> Self {
        #[cfg(feature = "processing")]
        let producer = store
            .map(RelayProducer::Store)
            .unwrap_or(RelayProducer::Noop);

        #[cfg(not(feature = "processing"))]
        let producer = RelayProducer::Noop;

        Self {
            relay_resource_id: config.cogs_relay_resource_id().to_owned(),
            usage_accountant: UsageAccountant::new(producer, None),
        }
    }

    fn handle_report(&mut self, CogsReport(measurement): CogsReport) {
        relay_log::trace!("recording measurement: {measurement:?}");

        let resource_id = match measurement.resource {
            ResourceId::Relay => &self.relay_resource_id,
        };

        let (amount, unit) = match measurement.value {
            relay_cogs::Value::Time(duration) => (
                duration.as_millis().try_into().unwrap_or(u64::MAX),
                UsageUnit::Milliseconds,
            ),
        };

        let result =
            self.usage_accountant
                .record(resource_id, measurement.feature.as_str(), amount, unit);

        // Usage accountant flushes debounced -> we won't be spammed with Sentry errors and error logs.
        if let Err(error) = result {
            relay_log::error!(
                error = &error as &dyn std::error::Error,
                "failed to record COGS data"
            );
        }
    }

    fn handle_shutdown(&mut self) {
        if let Err(error) = self.usage_accountant.flush() {
            relay_log::error!(
                error = &error as &dyn std::error::Error,
                "failed to flush COGS usage accountant"
            );
        }
    }
}

impl Service for CogsService {
    type Interface = CogsReport;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut shutdown = Controller::shutdown_handle();

            loop {
                tokio::select! {
                    biased;

                    Some(message) = rx.recv() => self.handle_report(message),
                    _ = shutdown.notified() => self.handle_shutdown(),

                    else => break,
                }
            }
        });
    }
}

/// COGS measurement recorder.
///
/// The recorder forwards the measurements to the [cogs service](`CogsService`).
pub struct CogsServiceRecorder {
    addr: Addr<CogsReport>,
    max_size: u64,
    has_errored: AtomicBool,
}

impl CogsServiceRecorder {
    /// Creates a new recorder forwarding messages to [`CogsService`].
    pub fn new(addr: Addr<CogsReport>, config: &Config) -> Self {
        Self {
            addr,
            max_size: config.cogs_max_queue_size(),
            has_errored: AtomicBool::new(false),
        }
    }
}

impl CogsRecorder for CogsServiceRecorder {
    fn record(&self, measurement: CogsMeasurement) {
        // Make sure we don't have an ever growing backlog of COGS measurements,
        // an error in the service should not have a visible impact in production.
        if self.addr.len() > self.max_size {
            if !self.has_errored.swap(true, Ordering::Relaxed) {
                relay_log::error!("COGS measurements backlogged");
            }

            return;
        }

        self.addr.send(measurement);
    }
}
