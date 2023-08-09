use std::sync::Arc;
use std::time::Duration;

use relay_dynamic_config::GlobalConfig;
use relay_system::{Addr, AsyncResponse, FromMessage, Interface, Sender, Service};
use tokio::sync::watch;

use crate::actors::project_upstream::{GetProjectStates, GetProjectStatesResponse};
use crate::actors::upstream::{SendQuery, UpstreamRelay, UpstreamRequestError};

/// Service implementing the [`GlobalConfigInterface`] interface.
///
/// The service is responsible for fetching the global config and
/// forwarding it to the services that require it, and for serving downstream relays.
#[derive(Debug)]
pub struct GlobalConfigService {
    /// Sender of the [`watch`] channel for the subscribers of the service.
    // NOTE(iker): Placing the sender behind an Arc is a workaround to be able
    // to send updates through this channel from invoked tasks where the global
    // config request is made from upstream, since a watch sender cannot be
    // cloned. To keep the latest config in the channel, it's assumed responses
    // from the upstream are resolved faster than the fetch interval of this
    // service. An alternative is to create a channel internal to the service
    // and handle the updates through them.
    sender: Arc<watch::Sender<Arc<GlobalConfig>>>,
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

/// The message for requesting the most recent global config from [`GlobalConfigService`].
pub struct Get;

impl FromMessage<Get> for GlobalConfigInterface {
    type Response = AsyncResponse<Arc<GlobalConfig>>;

    fn from_message(_: Get, sender: Sender<Arc<GlobalConfig>>) -> Self {
        Self::Get(sender)
    }
}

/// The message for receiving a watch that subscribes to the [`GlobalConfigService`].
pub struct Subscribe;

impl FromMessage<Subscribe> for GlobalConfigInterface {
    type Response = AsyncResponse<watch::Receiver<Arc<GlobalConfig>>>;

    fn from_message(_: Subscribe, sender: Sender<watch::Receiver<Arc<GlobalConfig>>>) -> Self {
        Self::Subscribe(sender)
    }
}

impl GlobalConfigService {
    /// Creates a new [`GlobalConfigService`].
    pub fn new(upstream: Addr<UpstreamRelay>) -> Self {
        let (sender, _) = watch::channel(Arc::new(GlobalConfig::default()));
        Self {
            sender: Arc::new(sender),
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
    fn update_global_config(&self) {
        let upstream_relay: Addr<UpstreamRelay> = self.upstream.clone();
        let sender = Arc::clone(&self.sender);

        tokio::spawn(async move {
            let query = GetProjectStates {
                public_keys: vec![],
                full_config: false,
                no_cache: false,
                global: true,
            };

            match upstream_relay.send(SendQuery(query)).await {
                Ok(res) => Self::handle_upstream_response(res, sender),
                Err(_) => relay_log::error!(
                    "GlobalConfigService failed to send request to UpstreamService"
                ),
            };
        });
    }

    fn handle_upstream_response(
        response: Result<GetProjectStatesResponse, UpstreamRequestError>,
        sender: Arc<watch::Sender<Arc<GlobalConfig>>>,
    ) {
        if let Ok(config) = response {
            match config.global {
                Some(global_config) => _ = sender.send(Arc::new(global_config)),
                None => relay_log::error!("Global config missing in upstream response"),
            };
        }
    }
}

impl Service for GlobalConfigService {
    type Interface = GlobalConfigInterface;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            relay_log::info!("global config service starting");

            let mut ticker = tokio::time::interval(Duration::from_secs(self.fetch_interval));

            loop {
                tokio::select! {
                    biased;

                    _ = ticker.tick() => self.update_global_config(),
                    Some(message) = rx.recv() => self.handle_message(message),

                    else => break,
                }
            }
            relay_log::info!("global config service stopped");
        });
    }
}
