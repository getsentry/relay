use std::sync::atomic::{AtomicBool, Ordering};

use relay_cogs::{CogsMeasurement, CogsRecorder, ResourceId};
use relay_config::Config;
use relay_system::{Addr, FromMessage, Interface, Service};

use crate::statsd::RelayCounters;

pub struct CogsReport(CogsMeasurement);

impl Interface for CogsReport {}

impl FromMessage<CogsMeasurement> for CogsReport {
    type Response = relay_system::NoResponse;

    fn from_message(message: CogsMeasurement, _: ()) -> Self {
        Self(message)
    }
}

/// Service implementing the [`CogsReport`] interface.
pub struct CogsService {
    relay_resource_id: String,
}

impl CogsService {
    pub fn new(config: &Config) -> Self {
        Self {
            relay_resource_id: config.cogs_relay_resource_id().to_owned(),
        }
    }

    fn handle_report(&mut self, CogsReport(measurement): CogsReport) {
        relay_log::trace!("recording measurement: {measurement:?}");

        let resource_id = match measurement.resource {
            ResourceId::Relay => &self.relay_resource_id,
        };

        let amount = match measurement.value {
            relay_cogs::Value::Time(duration) => {
                duration.as_micros().try_into().unwrap_or(i64::MAX)
            }
        };

        relay_statsd::metric!(
            counter(RelayCounters::CogsUsage) += amount,
            resource_id = resource_id,
            app_feature = measurement.feature.as_str()
        );
    }
}

impl Service for CogsService {
    type Interface = CogsReport;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                self.handle_report(message);
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
