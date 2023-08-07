use std::sync::Arc;
use std::time::Duration;

use relay_dynamic_config::GlobalConfig;
use relay_system::{Addr, AsyncResponse, FromMessage, Interface, Sender, Service};
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::actors::processor::EnvelopeProcessor;
use crate::actors::project_upstream::GetProjectStates;
use crate::actors::upstream::{SendQuery, UpstreamRelay};

/// Service implementing the [`GlobalConfiguration`] interface.
///
/// The service is responsible for fetching the global config and
/// forwarding it to the services that require it, and for serving downstream relays.
#[derive(Debug)]
pub struct GlobalConfigurationService {
    enabled: bool,
    global_config: Arc<GlobalConfig>,
    envelope_processor: Addr<EnvelopeProcessor>,
    upstream: Addr<UpstreamRelay>,
}

/// Service interface for the [`GetGlobalConfig`] message.
pub enum GlobalConfiguration {
    /// Used to receive the most recently fetched global config.
    GetGlobalConfig(Sender<Arc<GlobalConfig>>),
}

impl Interface for GlobalConfiguration {}

/// The message for requesting the most recent global config from [`GlobalConfigurationService`].
pub struct GetGlobalConfig;

impl FromMessage<GetGlobalConfig> for GlobalConfiguration {
    type Response = AsyncResponse<Arc<GlobalConfig>>;

    fn from_message(_: GetGlobalConfig, sender: Sender<Arc<GlobalConfig>>) -> Self {
        Self::GetGlobalConfig(sender)
    }
}

impl GlobalConfigurationService {
    /// Creates a new [`GlobalConfigurationService`].
    pub fn new(
        enabled: bool,
        envelope_processor: Addr<EnvelopeProcessor>,
        upstream: Addr<UpstreamRelay>,
    ) -> Self {
        let global_config = Arc::new(GlobalConfig::default());
        Self {
            enabled,
            global_config,
            envelope_processor,
            upstream,
        }
    }

    fn handle_message(&self, message: GlobalConfiguration) {
        match message {
            GlobalConfiguration::GetGlobalConfig(sender) => {
                sender.send(Arc::clone(&self.global_config));
            }
        }
    }

    /// Forwards the given global config to the services that require it.
    fn update_global_config(&mut self, global_tx: UnboundedSender<Arc<GlobalConfig>>) {
        let upstream_relay: Addr<UpstreamRelay> = self.upstream.clone();
        let envelope_processor = self.envelope_processor.clone();

        tokio::spawn(async move {
            let query = GetProjectStates {
                public_keys: vec![],
                full_config: false,
                no_cache: false,
                global: true,
            };

            match upstream_relay.send(SendQuery(query)).await {
                Ok(Ok(response)) => match response.global {
                    Some(global_config) => {
                        let global_config = Arc::new(global_config);
                        if let Err(e) = global_tx.send(global_config.clone()) {
                            relay_log::error!(
                                "failed to send global config to GlobalConfigurationService: {}",
                                e
                            );
                        };
                        relay_log::info!("global config received :D");
                        envelope_processor.send::<Arc<GlobalConfig>>(global_config);
                    }
                    None => relay_log::error!("global config is missing in upstream response"),
                },
                Ok(Err(e)) => {
                    relay_log::error!("failed to fetch global config request: {}", e);
                }
                Err(e) => {
                    relay_log::error!("failed to send global config request: {}", e);
                }
            };
        });
    }
}

impl Service for GlobalConfigurationService {
    type Interface = GlobalConfiguration;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        if !self.enabled {
            relay_log::info!(
                "global configuration service not starting due to missing credentials"
            );
            return;
        }

        tokio::spawn(async move {
            let ticker_duration = Duration::from_secs(10);

            let mut ticker = tokio::time::interval(ticker_duration);
            relay_log::info!("global configuration service started");

            // Channel for async global config responses back into the GlobalConfigurationService.
            let (global_tx, mut global_rx) = mpsc::unbounded_channel();

            loop {
                tokio::select! {
                    biased;
                    _ = ticker.tick() => self.update_global_config(global_tx.clone()),
                    Some(global_config) = global_rx.recv() => self.global_config = global_config,
                    Some(message) = rx.recv() => self.handle_message(message),
                    else => break,
                }
            }
            relay_log::info!("global configuration service stopped");
        });
    }
}
