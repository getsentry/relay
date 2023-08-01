use std::sync::Arc;
use std::time::Duration;

use relay_dynamic_config::GlobalConfig;
use relay_system::{Addr, AsyncResponse, FromMessage, Interface, Sender, Service};

use crate::actors::processor::EnvelopeProcessor;
use crate::actors::project_upstream::GetProjectStates;
use crate::actors::upstream::{SendQuery, UpstreamRelay};

/// Service implementing the [`GlobalConfiguration`] interface.
///
/// The service is responsible for fetching the global config and
/// forwarding it to the services that require it.
#[derive(Debug)]
pub struct GlobalConfigurationService {
    global_config: Arc<GlobalConfig>,
    envelope_processor: Addr<EnvelopeProcessor>,
    upstream: Addr<UpstreamRelay>,
}

/// Service interface for the [`GetGlobalConfig`] message.
pub enum GlobalConfiguration {
    GetGlobalConfig(Sender<Arc<GlobalConfig>>),
}

impl Interface for GlobalConfiguration {}

pub struct GetGlobalConfig;

impl FromMessage<GetGlobalConfig> for GlobalConfiguration {
    type Response = AsyncResponse<Arc<GlobalConfig>>;

    fn from_message(_: GetGlobalConfig, sender: Sender<Arc<GlobalConfig>>) -> Self {
        Self::GetGlobalConfig(sender)
    }
}

impl GlobalConfigurationService {
    pub fn new(project_cache: Addr<EnvelopeProcessor>, upstream: Addr<UpstreamRelay>) -> Self {
        let global_config = Arc::new(GlobalConfig::default());
        Self {
            global_config,
            envelope_processor: project_cache,
            upstream,
        }
    }

    fn handle_message(&self, message: GlobalConfiguration) {
        match message {
            GlobalConfiguration::GetGlobalConfig(global_config) => {
                global_config.send(self.global_config.clone());
            }
        }
    }

    /// Forwards the given global config to the services that require it.
    async fn update_global_config(&mut self) {
        let upstream_relay: Addr<UpstreamRelay> = self.upstream.clone();
        let project_cache = self.envelope_processor.clone();

        let query = GetProjectStates {
            public_keys: vec![],
            full_config: false,
            no_cache: false,
            global_config: true,
        };

        match upstream_relay.send(SendQuery(query)).await {
            Ok(Ok(response)) => match response.global_config {
                Some(global_config) => {
                    let global_config = Arc::new(global_config);
                    self.global_config = global_config.clone();
                    project_cache.send::<Arc<GlobalConfig>>(global_config);
                }
                None => relay_log::error!("Upstream response didn't include a global config"),
            },
            Err(e) => {
                relay_log::error!("failed to send global config request: {}", e);
            }
            Ok(Err(e)) => {
                relay_log::error!("failed to fetch global config request: {}", e);
            }
        };
    }
}

impl Service for GlobalConfigurationService {
    type Interface = GlobalConfiguration;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(10));
            relay_log::info!("global configuration service started");

            loop {
                tokio::select! {
                    biased;
                    _ = ticker.tick() => self.update_global_config().await,
                    Some(message) = rx.recv() => self.handle_message(message),
                    else => break,
                }
            }
            relay_log::info!("global configuration service stopped");
        });
    }
}
