use std::sync::Arc;
use std::time::Duration;

use relay_dynamic_config::GlobalConfig;
use relay_system::{Addr, AsyncResponse, FromMessage, Interface, Sender, Service};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::watch;

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
    sender: watch::Sender<Arc<GlobalConfig>>,
    upstream: Addr<UpstreamRelay>,
}

/// Service interface for the [`GetGlobalConfig`] message.
pub enum GlobalConfiguration {
    /// Used to receive the most recently fetched global config.
    GetGlobalConfig(Sender<Arc<GlobalConfig>>), // TODO: rename to Get
    Subscribe(Sender<watch::Receiver<Arc<GlobalConfig>>>),
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

pub struct Subscribe;

impl FromMessage<Subscribe> for GlobalConfiguration {
    type Response = AsyncResponse<watch::Receiver<Arc<GlobalConfig>>>;

    fn from_message(_: Subscribe, sender: Sender<watch::Receiver<Arc<GlobalConfig>>>) -> Self {
        Self::Subscribe(sender)
    }
}

impl GlobalConfigurationService {
    /// Creates a new [`GlobalConfigurationService`].
    pub fn new(enabled: bool, upstream: Addr<UpstreamRelay>) -> Self {
        let (sender, _) = watch::channel(Arc::new(GlobalConfig::default()));
        Self {
            enabled,
            sender,
            upstream,
        }
    }

    fn handle_message(&self, message: GlobalConfiguration) {
        match message {
            GlobalConfiguration::GetGlobalConfig(sender) => {
                // sender.send(Arc::clone(&self.global_config));
                self.sender.borrow().clone()
            }
            Subscribe(sender) => sender.send(self.sender.subscribe()),
        }
    }

    /// Forwards the given global config to the services that require it.
    fn update_global_config(&mut self) {
        let upstream_relay: Addr<UpstreamRelay> = self.upstream.clone();

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
                        self.sender.send(global_config).ok();
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

            loop {
                tokio::select! {
                    biased;
                    _ = ticker.tick() => self.update_global_config(),
                    Some(message) = rx.recv() => self.handle_message(message),
                    else => break,
                }
            }
            relay_log::info!("global configuration service stopped");
        });
    }
}
