use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use relay_config::Config;
use relay_dynamic_config::GlobalConfig;
use relay_system::{Addr, AsyncResponse, FromMessage, Interface, Sender, Service};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, watch};

use crate::actors::upstream::{
    RequestPriority, SendQuery, UpstreamQuery, UpstreamRelay, UpstreamRequestError,
};
use crate::utils::SleepHandle;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetGlobalConfigResponse {
    #[serde(default)]
    global: Option<GlobalConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetGlobalConfig {
    pub global: bool,
    // Upstream expects a list of public keys.
    public_keys: Vec<()>,
}

impl GetGlobalConfig {
    pub fn query() -> GetGlobalConfig {
        GetGlobalConfig {
            global: true,
            public_keys: vec![],
        }
    }
}

impl UpstreamQuery for GetGlobalConfig {
    type Response = GetGlobalConfigResponse;

    fn method(&self) -> reqwest::Method {
        Method::POST
    }

    fn path(&self) -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("/api/0/relays/projectconfigs/?version=4")
    }

    fn retry() -> bool {
        false
    }

    fn priority() -> super::upstream::RequestPriority {
        RequestPriority::High
    }

    fn route(&self) -> &'static str {
        "global_config"
    }
}

/// The message for requesting the most recent global config from [`GlobalConfigService`].
pub struct Get;

/// The message for receiving a watch that subscribes to the [`GlobalConfigService`].
pub struct Subscribe;

/// A way to get updates of the global config.
pub enum GlobalConfigManager {
    /// Returns the most recent global config.
    Get(Sender<Arc<GlobalConfig>>),
    /// Returns a [`watch::Receiver`] where global config updates will be sent to.
    Subscribe(Sender<watch::Receiver<Arc<GlobalConfig>>>),
}

impl Interface for GlobalConfigManager {}

impl FromMessage<Get> for GlobalConfigManager {
    type Response = AsyncResponse<Arc<GlobalConfig>>;

    fn from_message(_: Get, sender: Sender<Arc<GlobalConfig>>) -> Self {
        Self::Get(sender)
    }
}

impl FromMessage<Subscribe> for GlobalConfigManager {
    type Response = AsyncResponse<watch::Receiver<Arc<GlobalConfig>>>;

    fn from_message(_: Subscribe, sender: Sender<watch::Receiver<Arc<GlobalConfig>>>) -> Self {
        Self::Subscribe(sender)
    }
}

/// Service implementing the [`GlobalConfigManager`] interface.
///
/// The service offers two alternatives to fetch the [`GlobalConfig`]:
/// responding to a [`Get`] message with the config for one-off requests, or
/// subscribing to updates with [`Subscribe`] to keep up-to-date.
#[derive(Debug)]
pub struct GlobalConfigService {
    /// Sender of the [`watch`] channel for the subscribers of the service.
    sender: watch::Sender<Arc<GlobalConfig>>,
    /// Sender of the internal channel to forward global configs from upstream.
    internal_tx: UnboundedSender<Result<GetGlobalConfigResponse, UpstreamRequestError>>,
    /// Receiver of the internal channel to forward global configs from upstream.
    internal_rx: UnboundedReceiver<Result<GetGlobalConfigResponse, UpstreamRequestError>>,
    /// Upstream service to request global configs from.
    upstream: Addr<UpstreamRelay>,
    /// The duration to wait before fetching global configs from upstream.
    fetch_interval: Duration,
    /// Handle to avoid multiple outgoing requests.
    fetch_handle: SleepHandle,
}

impl GlobalConfigService {
    /// Creates a new [`GlobalConfigService`].
    pub fn new(config: Arc<Config>, upstream: Addr<UpstreamRelay>) -> Self {
        let (sender, _) = watch::channel(Arc::default());
        let (internal_tx, internal_rx) = mpsc::unbounded_channel();
        Self {
            sender,
            internal_tx,
            internal_rx,
            upstream,
            fetch_interval: config.global_config_fetch_interval(),
            fetch_handle: SleepHandle::idle(),
        }
    }

    /// Handles messages from external services.
    fn handle_message(&self, message: GlobalConfigManager) {
        match message {
            GlobalConfigManager::Get(sender) => {
                sender.send(self.sender.borrow().clone());
            }
            GlobalConfigManager::Subscribe(sender) => {
                sender.send(self.sender.subscribe());
            }
        }
    }

    /// Schedules the next global config request.
    fn schedule_fetch(&mut self) {
        if self.fetch_handle.is_idle() {
            // XXX(iker): set a backoff mechanism?
            self.fetch_handle.set(self.fetch_interval);
        }
    }

    /// Requests a new global config from upstream.
    ///
    /// As the upstream service is responsible for authentication, the global
    /// config service doesn't deal with it.
    fn update_global_config(&mut self) {
        self.fetch_handle.reset();

        let upstream_relay: Addr<UpstreamRelay> = self.upstream.clone();
        let internal_tx = self.internal_tx.clone();

        tokio::spawn(async move {
            let query = GetGlobalConfig::query();
            match upstream_relay.send(SendQuery(query)).await {
                // TODO(iker): add observability on the request.
                Ok(res) => {
                    if internal_tx.send(res).is_err() {
                        relay_log::error!("failed to forward internally the upstream response");
                    }
                }
                // FIXME(iker): if the request to upstream fails, we should
                // reschedule another fetch anyway.
                Err(_) => relay_log::error!("failed to send request to upstream"),
            };
        });
    }

    /// Handles the global config response from upstream, forwarding the global
    /// config through the internal channel if succesfully received.
    fn handle_upstream_response(
        &mut self,
        response: Result<GetGlobalConfigResponse, UpstreamRequestError>,
    ) {
        match response {
            Ok(config) => match config.global {
                Some(global_config) => {
                    if self.sender.send(Arc::new(global_config)).is_err() {
                        relay_log::error!("failed to forward the global config internally");
                    }
                }
                None => relay_log::error!("global config missing in upstream response"),
            },
            Err(e) => relay_log::error!(
                error = &e as &dyn std::error::Error,
                "failed to fetch global config from upstream"
            ),
        };
        self.schedule_fetch();
    }
}

impl Service for GlobalConfigService {
    type Interface = GlobalConfigManager;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            relay_log::info!("global config service starting");

            // NOTE(iker): if this first request fails it's possible the default
            // global config is forwarded. This is not ideal, but we accept it
            // for now.
            self.update_global_config();

            loop {
                tokio::select! {
                    biased;

                    () = &mut self.fetch_handle => self.update_global_config(),
                    Some(global_config) = self.internal_rx.recv() => self.handle_upstream_response(global_config),
                    Some(message) = rx.recv() => self.handle_message(message),

                    else => break,
                }
            }
            relay_log::info!("global config service stopped");
        });
    }
}
