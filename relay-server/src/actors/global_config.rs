//! This module implements the Global Config service.
//!
//! The global config service is a Relay service to manage [`GlobalConfig`]s,
//! from fetching to forwarding. Once the service is started, it requests
//! recurrently the configs from upstream in a timely manner to provide it to
//! the rest of Relay.
//!
//! There are two ways to interact with this service: requesting a single global
//! config update or subscribing for updates; see [`GlobalConfigManager`] for
//! more details.

use std::borrow::Cow;
use std::sync::Arc;

use relay_config::Config;
use relay_dynamic_config::GlobalConfig;
use relay_statsd::metric;
use relay_system::{Addr, AsyncResponse, Controller, FromMessage, Interface, Service};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use crate::actors::upstream::{
    RequestPriority, SendQuery, UpstreamQuery, UpstreamRelay, UpstreamRequestError,
};
use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::SleepHandle;

/// The result of sending a global config query to upstream.
/// It can fail both in sending it, and in the response.
type UpstreamQueryResult =
    Result<Result<GetGlobalConfigResponse, UpstreamRequestError>, relay_system::SendError>;

/// The response of a fetch of a global config from upstream.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GetGlobalConfigResponse {
    #[serde(default)]
    global: Option<GlobalConfig>,
}

/// The request to fetch a global config from upstream.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GetGlobalConfig {
    global: bool,
    // Dummy variable - upstream expects a list of public keys.
    public_keys: Vec<()>,
}

impl GetGlobalConfig {
    fn new() -> GetGlobalConfig {
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
///
/// On Relay shutdown, the sender of the channel is dropped and receiving new
/// updates errors.
pub struct Subscribe;

/// An interface to get [`GlobalConfig`]s through [`GlobalConfigService`].
///
/// For a one-off update, [`GlobalConfigService`] responds to
/// [`GlobalConfigManager::Get`] messages with the latest instance of the
/// [`GlobalConfig`]. For continued updates, you can subscribe with
/// [`GlobalConfigManager::Subscribe`] to get a receiver back where up-to-date
/// instances will be sent to, while [`GlobalConfigService`] manages the update
/// frequency from upstream.
pub enum GlobalConfigManager {
    /// Returns the most recent global config.
    Get(relay_system::Sender<Arc<GlobalConfig>>),
    /// Returns a [`watch::Receiver`] where global config updates will be sent to.
    Subscribe(relay_system::Sender<watch::Receiver<Arc<GlobalConfig>>>),
}

impl Interface for GlobalConfigManager {}

impl FromMessage<Get> for GlobalConfigManager {
    type Response = AsyncResponse<Arc<GlobalConfig>>;

    fn from_message(_: Get, sender: relay_system::Sender<Arc<GlobalConfig>>) -> Self {
        Self::Get(sender)
    }
}

impl FromMessage<Subscribe> for GlobalConfigManager {
    type Response = AsyncResponse<watch::Receiver<Arc<GlobalConfig>>>;

    fn from_message(
        _: Subscribe,
        sender: relay_system::Sender<watch::Receiver<Arc<GlobalConfig>>>,
    ) -> Self {
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
    config: Arc<Config>,
    /// Sender of the [`watch`] channel for the subscribers of the service.
    global_config_watch: watch::Sender<Arc<GlobalConfig>>,
    /// Sender of the internal channel to forward global configs from upstream.
    internal_tx: mpsc::Sender<UpstreamQueryResult>,
    /// Receiver of the internal channel to forward global configs from upstream.
    internal_rx: mpsc::Receiver<UpstreamQueryResult>,
    /// Upstream service to request global configs from.
    upstream: Addr<UpstreamRelay>,
    /// Handle to avoid multiple outgoing requests.
    fetch_handle: SleepHandle,
}

impl GlobalConfigService {
    /// Creates a new [`GlobalConfigService`].
    pub fn new(config: Arc<Config>, upstream: Addr<UpstreamRelay>) -> Self {
        let (global_config_watch, _) = watch::channel(Arc::default());
        let (internal_tx, internal_rx) = mpsc::channel(1);
        Self {
            config,
            global_config_watch,
            internal_tx,
            internal_rx,
            upstream,
            fetch_handle: SleepHandle::idle(),
        }
    }

    /// Handles messages from external services.
    fn handle_message(&self, message: GlobalConfigManager) {
        match message {
            GlobalConfigManager::Get(sender) => {
                sender.send(self.global_config_watch.borrow().clone());
            }
            GlobalConfigManager::Subscribe(sender) => {
                sender.send(self.global_config_watch.subscribe());
            }
        }
    }

    /// Schedules the next global config request.
    fn schedule_fetch(&mut self) {
        if self.fetch_handle.is_idle() {
            self.fetch_handle
                .set(self.config.global_config_fetch_interval());
        }
    }

    /// Requests a new global config from upstream.
    ///
    /// We check if we have credentials before sending,
    /// otherwise we would log an [`UpstreamRequestError::NoCredentials`] error.
    fn update_global_config(&mut self) {
        self.fetch_handle.reset();

        let upstream_relay = self.upstream.clone();
        let internal_tx = self.internal_tx.clone();

        tokio::spawn(async move {
            metric!(timer(RelayTimers::GlobalConfigRequestDuration), {
                let query = GetGlobalConfig::new();
                let res = upstream_relay.send(SendQuery(query)).await;
                // Internal forwarding should only fail when the internal
                // receiver is closed.
                internal_tx.send(res).await.ok();
            });
        });
    }

    /// Handles the response of an attempt to fetch the global config from
    /// upstream.
    ///
    /// This function checks two levels of results:
    /// 1. Whether the request to the upstream was successful.
    /// 2. If the request was successful, it then checks whether the returned
    /// global config is valid and contains the expected data.
    fn handle_result(&mut self, result: UpstreamQueryResult) {
        match result {
            Ok(Ok(config)) => {
                let mut success = false;
                match config.global {
                    Some(global_config) => {
                        // Notifying subscribers only fails when there are no
                        // subscribers.
                        self.global_config_watch.send(Arc::new(global_config)).ok();
                        success = true;
                    }
                    None => relay_log::error!("global config missing in upstream response"),
                }
                metric!(
                    counter(RelayCounters::GlobalConfigFetched) += 1,
                    success = if success { "true" } else { "false" },
                );
            }
            Ok(Err(e)) => relay_log::error!(
                error = &e as &dyn std::error::Error,
                "failed to fetch global config from upstream"
            ),
            Err(e) => relay_log::error!(
                error = &e as &dyn std::error::Error,
                "failed to send request to upstream"
            ),
        }

        self.schedule_fetch();
    }
}

impl Service for GlobalConfigService {
    type Interface = GlobalConfigManager;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut shutdown = Controller::shutdown_handle();

            relay_log::info!("global config service starting");

            if self.config.has_credentials() {
                // NOTE(iker): if this first request fails it's possible the default
                // global config is forwarded. This is not ideal, but we accept it
                // for now.
                self.update_global_config();
            } else {
                // NOTE(iker): not making a request results in the sleep handler
                // not being reset, so no new requests are made.
                relay_log::info!("fetching global configs disabled: no credentials configured");
            }

            loop {
                tokio::select! {
                    biased;

                    () = &mut self.fetch_handle => self.update_global_config(),
                    Some(result) = self.internal_rx.recv() => self.handle_result(result),
                    Some(message) = rx.recv() => self.handle_message(message),
                    _ = shutdown.notified() => break,

                    else => break,
                }
            }
            relay_log::info!("global config service stopped");
        });
    }
}

#[cfg(test)]
mod tests {
    use relay_system::{Controller, Service, ShutdownMode};
    use relay_test::mock_service;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::actors::global_config::{Get, GlobalConfigService};

    /// Tests whether the service shuts down gracefully when the signal is
    /// received, without requesting more global configs.
    #[tokio::test(start_paused = true)]
    async fn test_service_shutdown() {
        for mode in &[ShutdownMode::Graceful, ShutdownMode::Immediate] {
            shutdown_service_with_mode(*mode).await;
        }
    }

    async fn shutdown_service_with_mode(mode: ShutdownMode) {
        let (upstream, _) = mock_service("upstream", (), |&mut (), _| {});

        Controller::start(Duration::from_secs(1));

        let service = GlobalConfigService::new(Arc::default(), upstream).start();

        assert!(service.send(Get).await.is_ok());

        Controller::shutdown(mode);
        tokio::time::advance(Duration::from_millis(200)).await;

        assert!(service.send(Get).await.is_err());
    }
}
