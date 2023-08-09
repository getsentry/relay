use std::sync::Arc;
use std::time::Duration;

use relay_dynamic_config::GlobalConfig;
use relay_system::{Addr, AsyncResponse, FromMessage, Interface, Sender, Service};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::watch;

use crate::actors::project_upstream::{GetProjectStates, GetProjectStatesResponse};
use crate::actors::upstream::{SendQuery, UpstreamRelay, UpstreamRequestError};

/// Service implementing the [`GlobalConfiguration`] interface.
///
/// The service is responsible for fetching the global config and
/// forwarding it to the services that require it, and for serving downstream relays.
#[derive(Debug)]
pub struct GlobalConfigurationService {
    sender: watch::Sender<Arc<GlobalConfig>>,
    upstream: Addr<UpstreamRelay>,
    /// Number of seconds to wait before making another request.
    fetch_interval: u64,
}

/// Global Config service interface
pub enum GlobalConfigInterface {
    /// Used to receive the most recently fetched global config.
    Get(Sender<Arc<GlobalConfig>>),
    /// Used to receive a watch that will notify about new global configs.
    Subscribe(Sender<watch::Receiver<Arc<GlobalConfig>>>),
}

impl Interface for GlobalConfigInterface {}

/// The message for requesting the most recent global config from [`GlobalConfigurationService`].
pub struct Get;

impl FromMessage<Get> for GlobalConfigInterface {
    type Response = AsyncResponse<Arc<GlobalConfig>>;

    fn from_message(_: Get, sender: Sender<Arc<GlobalConfig>>) -> Self {
        Self::Get(sender)
    }
}

/// The message for receiving a watch that subscribes to the [`GlobalConfigurationService`].
pub struct Subscribe;

impl FromMessage<Subscribe> for GlobalConfigInterface {
    type Response = AsyncResponse<watch::Receiver<Arc<GlobalConfig>>>;

    fn from_message(_: Subscribe, sender: Sender<watch::Receiver<Arc<GlobalConfig>>>) -> Self {
        Self::Subscribe(sender)
    }
}

impl GlobalConfigurationService {
    /// Creates a new [`GlobalConfigurationService`].
    pub fn new(upstream: Addr<UpstreamRelay>) -> Self {
        let (sender, _) = watch::channel(Arc::new(GlobalConfig::default()));
        Self {
            sender,
            upstream,
            fetch_interval: 10,
        }
    }

    fn handle_message(&self, message: GlobalConfigInterface) {
        match message {
            GlobalConfigInterface::Get(sender) => {
                sender.send(self.sender.borrow().clone());
            }
            GlobalConfigInterface::Subscribe(sender) => {
                sender.send(self.sender.subscribe());
            }
        }
    }

    /// Requests a new global config from upstream.
    ///
    /// This function does not check any authentication since the upstream
    /// service deals with it (including observability):
    /// - If Relay is authenticated, requests will complete as expected.
    /// - If Relay isn't authenticated yet, the following request to successfull
    /// authentication will happen.
    /// - If Relay is permanently blocked, the upstream service will error on
    /// requests.
    fn update_global_config(&self, global_tx: UnboundedSender<Arc<GlobalConfig>>) {
        let upstream_relay: Addr<UpstreamRelay> = self.upstream.clone();

        tokio::spawn(async move {
            let query = GetProjectStates {
                public_keys: vec![],
                full_config: false,
                no_cache: false,
                global: true,
            };

            match upstream_relay.send(SendQuery(query)).await {
                Ok(res) => Self::handle_upstream_response(res, global_tx),
                Err(_) => relay_log::error!(
                    "GlobalConfigService failed to send request to UpstreamService"
                ),
            };
        });
    }

    fn handle_upstream_response(
        response: Result<GetProjectStatesResponse, UpstreamRequestError>,
        global_tx: UnboundedSender<Arc<GlobalConfig>>,
    ) {
        if let Ok(config) = response {
            match config.global {
                Some(global_config) => _ = global_tx.send(Arc::new(global_config)),
                None => relay_log::error!("Global config missing in upstream response"),
            };
        }
    }
}

impl Service for GlobalConfigurationService {
    type Interface = GlobalConfigInterface;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            relay_log::info!("global configuration service starting");

            let mut ticker = tokio::time::interval(Duration::from_secs(self.fetch_interval));
            // Channel for sending new global configs from upstream to the spawn loop, so that we may
            // update the tokio::watch.
            let (global_tx, mut global_rx) = mpsc::unbounded_channel();

            loop {
                tokio::select! {
                    biased;

                    Some(global_config) = global_rx.recv() => {
                        if let Err(e) = self.sender.send(global_config) {
                            relay_log::error!("failed to update global config watch: {}", e);
                        };
                    },
                    _ = ticker.tick() => self.update_global_config(global_tx.clone()),
                    Some(message) = rx.recv() => self.handle_message(message),

                    else => break,
                }
            }
            relay_log::info!("global configuration service stopped");
        });
    }
}
