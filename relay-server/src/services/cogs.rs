use std::sync::atomic::{AtomicBool, Ordering};

use sentry_usage_accountant::{Producer, UsageAccountant, UsageUnit};

use relay_cogs::{CogsMeasurement, CogsRecorder, ResourceId};
use relay_config::Config;
use relay_system::{Addr, Controller, FromMessage, Interface, Service};

#[cfg(feature = "processing")]
use crate::services::store::{Store, StoreCogs};
use crate::statsd::RelayCounters;

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

    fn send(&mut self, _message: Vec<u8>) -> Result<(), Self::Error> {
        match self {
            #[cfg(feature = "processing")]
            Self::Store(addr) => addr.send(StoreCogs(_message)),
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
            .filter(|_| config.cogs_enabled())
            .unwrap_or(RelayProducer::Noop);

        #[cfg(not(feature = "processing"))]
        let producer = RelayProducer::Noop;

        let granularity = chrono::Duration::from_std(config.cogs_granularity()).ok();

        Self {
            relay_resource_id: config.cogs_relay_resource_id().to_owned(),
            usage_accountant: UsageAccountant::new(producer, granularity),
        }
    }

    fn handle_report(&mut self, CogsReport(measurement): CogsReport) {
        relay_log::trace!("recording measurement: {measurement:?}");

        let resource_id = match measurement.resource {
            ResourceId::Relay => &self.relay_resource_id,
        };

        let (amount, unit) = match measurement.value {
            relay_cogs::Value::Time(duration) => (
                duration.as_micros().try_into().unwrap_or(u64::MAX),
                UsageUnit::Milliseconds,
            ),
        };

        relay_statsd::metric!(
            counter(RelayCounters::CogsUsage) += amount as i64,
            resource_id = resource_id,
            app_feature = measurement.feature.as_str()
        );

        self.usage_accountant
            .record(resource_id, measurement.feature.as_str(), amount, unit)
            .unwrap_or_else(|err| match err {});
    }

    fn handle_shutdown(&mut self) {
        self.usage_accountant
            .flush()
            .unwrap_or_else(|err| match err {});
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
    pub fn new(config: &Config, addr: Addr<CogsReport>) -> Self {
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
        if self.addr.len() >= self.max_size {
            if !self.has_errored.swap(true, Ordering::Relaxed) {
                relay_log::error!("COGS measurements backlogged");
            }

            return;
        }

        self.addr.send(measurement);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_cogs_service_recorder_limit() {
        let addr = Addr::dummy();
        let config = Config::from_json_value(serde_json::json!({
            "cogs": {
                "max_queue_size": 2
            }
        }))
        .unwrap();
        let recorder = CogsServiceRecorder::new(&config, addr.clone());

        for _ in 0..5 {
            recorder.record(CogsMeasurement {
                resource: ResourceId::Relay,
                feature: relay_cogs::AppFeature::Spans,
                value: relay_cogs::Value::Time(Duration::from_secs(1)),
            });
        }

        assert_eq!(addr.len(), 2);
        assert!(recorder.has_errored.load(Ordering::Relaxed));
    }
}
