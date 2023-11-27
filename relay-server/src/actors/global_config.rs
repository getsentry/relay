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
use std::time::Duration;

use relay_config::Config;
use relay_config::RelayMode;
use relay_dynamic_config::GlobalConfig;
use relay_statsd::metric;
use relay_system::{Addr, AsyncResponse, Controller, FromMessage, Interface, Service};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

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
    global_status: Option<StatusResponse>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StatusResponse {
    Ready,
    Pending,
}

impl StatusResponse {
    pub fn is_ready(&self) -> bool {
        matches!(self, &Self::Ready)
    }
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
        Cow::Borrowed("/api/0/relays/projectconfigs/?version=3")
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
/// The global config service must be up and running, else the subscription
/// fails. Subscribers should use the initial value when they get the watch
/// rather than only waiting for the watch to update, in case a global config
///  is only updated once, such as is the case with the static config file.
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
    Get(relay_system::Sender<Status>),
    /// Returns a [`watch::Receiver`] where global config updates will be sent to.
    Subscribe(relay_system::Sender<watch::Receiver<Status>>),
}

impl Interface for GlobalConfigManager {}

impl FromMessage<Get> for GlobalConfigManager {
    type Response = AsyncResponse<Status>;

    fn from_message(_: Get, sender: relay_system::Sender<Status>) -> Self {
        Self::Get(sender)
    }
}

impl FromMessage<Subscribe> for GlobalConfigManager {
    type Response = AsyncResponse<watch::Receiver<Status>>;

    fn from_message(_: Subscribe, sender: relay_system::Sender<watch::Receiver<Status>>) -> Self {
        Self::Subscribe(sender)
    }
}

/// Describes the current fetching status of the [`GlobalConfig`] from the upstream.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum Status {
    /// Global config ready to be used by other services.
    ///
    /// This variant implies different things in different circumstances. In managed mode, it means
    /// that we have received a config from upstream. In other modes the config is either
    /// from a file or the default global config.
    Ready(Arc<GlobalConfig>),
    /// The global config is requested from the upstream but it has not arrived yet.
    ///
    /// This variant should never be sent after the first `Ready` has occured.
    #[default]
    Pending,
}

impl Status {
    fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
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
    global_config_watch: watch::Sender<Status>,
    /// Sender of the internal channel to forward global configs from upstream.
    internal_tx: mpsc::Sender<UpstreamQueryResult>,
    /// Receiver of the internal channel to forward global configs from upstream.
    internal_rx: mpsc::Receiver<UpstreamQueryResult>,
    /// Upstream service to request global configs from.
    upstream: Addr<UpstreamRelay>,
    /// Handle to avoid multiple outgoing requests.
    fetch_handle: SleepHandle,
    /// Last instant the global config was successfully fetched in.
    last_fetched: Instant,
    /// Interval of upstream fetching failures before reporting such errors.
    upstream_failure_interval: Duration,
    /// Disables the upstream fetch loop.
    shutdown: bool,
}

impl GlobalConfigService {
    /// Creates a new [`GlobalConfigService`].
    pub fn new(config: Arc<Config>, upstream: Addr<UpstreamRelay>) -> Self {
        let (internal_tx, internal_rx) = mpsc::channel(1);
        let (global_config_watch, _) = watch::channel(Status::Pending);

        Self {
            config,
            global_config_watch,
            internal_tx,
            internal_rx,
            upstream,
            fetch_handle: SleepHandle::idle(),
            last_fetched: Instant::now(),
            upstream_failure_interval: Duration::from_secs(35),
            shutdown: false,
        }
    }

    /// Handles messages from external services.
    fn handle_message(&mut self, message: GlobalConfigManager) {
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
        if !self.shutdown && self.fetch_handle.is_idle() {
            self.fetch_handle
                .set(self.config.global_config_fetch_interval());
        }
    }

    /// Requests a new global config from upstream.
    ///
    /// We check if we have credentials before sending,
    /// otherwise we would log an [`UpstreamRequestError::NoCredentials`] error.
    fn request_global_config(&mut self) {
        // Disable upstream requests timer until we receive result of query.
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
                let is_ready = config.global_status.is_some_and(|stat| stat.is_ready());

                match config.global {
                    Some(global_config) if is_ready => {
                        // Log the first time we receive a global config from upstream.
                        if !self.global_config_watch.borrow().is_ready() {
                            relay_log::info!("received global config from upstream");
                        }
                        self.global_config_watch
                            .send_replace(Status::Ready(global_config.into()));
                        success = true;
                        self.last_fetched = Instant::now();
                    }
                    Some(_) => {
                        relay_log::info!("global config from upstream is not yet ready");
                    }
                    None => relay_log::error!("global config missing in upstream response"),
                }
                metric!(
                    counter(RelayCounters::GlobalConfigFetched) += 1,
                    success = if success { "true" } else { "false" },
                );
            }
            Ok(Err(e)) => {
                if self.last_fetched.elapsed() >= self.upstream_failure_interval {
                    relay_log::error!(
                        error = &e as &dyn std::error::Error,
                        "failed to fetch global config from upstream"
                    );
                }
            }
            Err(e) => relay_log::error!(
                error = &e as &dyn std::error::Error,
                "failed to send request to upstream"
            ),
        }

        // Enable upstream requests timer for global configs.
        self.schedule_fetch();
    }

    fn handle_shutdown(&mut self) {
        self.shutdown = true;
        self.fetch_handle.reset();
    }
}

impl Service for GlobalConfigService {
    type Interface = GlobalConfigManager;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut shutdown_handle = Controller::shutdown_handle();

            relay_log::info!("global config service starting");
            if self.config.relay_mode() == RelayMode::Managed {
                relay_log::info!("requesting global config from upstream");
                self.request_global_config();
            } else {
                match GlobalConfig::load(self.config.path()) {
                    Ok(Some(from_file)) => {
                        relay_log::info!("serving static global config loaded from file");
                        self.global_config_watch
                            .send_replace(Status::Ready(Arc::new(from_file)));
                    }
                    Ok(None) => {
                        relay_log::info!(
                                "serving default global configs due to lacking static global config file"
                            );
                        self.global_config_watch
                            .send_replace(Status::Ready(Arc::default()));
                    }
                    Err(e) => {
                        relay_log::error!("failed to load global config from file: {}", e);
                        relay_log::info!(
                                "serving default global configs due to failure to load global config from file"
                            );
                        self.global_config_watch
                            .send_replace(Status::Ready(Arc::default()));
                    }
                }
            };

            loop {
                tokio::select! {
                    biased;

                    () = &mut self.fetch_handle => self.request_global_config(),
                    Some(result) = self.internal_rx.recv() => self.handle_result(result),
                    Some(message) = rx.recv() => self.handle_message(message),
                    _ = shutdown_handle.notified() => self.handle_shutdown(),

                    else => break,
                }
            }
            relay_log::info!("global config service stopped");
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use relay_config::{Config, RelayMode};
    use relay_system::{Controller, Service, ShutdownMode};
    use relay_test::mock_service;

    use crate::actors::global_config::{Get, GlobalConfigService};

    /// Tests that the service can still handle requests after sending a
    /// shutdown signal.
    #[tokio::test]
    async fn shutdown_service() {
        relay_test::setup();
        tokio::time::pause();

        let (upstream, _) = mock_service("upstream", 0, |state, _| {
            *state += 1;

            if *state > 1 {
                panic!("should not receive requests after shutdown");
            }
        });

        Controller::start(Duration::from_secs(1));
        let mut config = Config::default();
        config.regenerate_credentials(false).unwrap();
        let fetch_interval = config.global_config_fetch_interval();

        let service = GlobalConfigService::new(Arc::new(config), upstream).start();

        assert!(service.send(Get).await.is_ok());

        Controller::shutdown(ShutdownMode::Immediate);
        tokio::time::sleep(fetch_interval * 2).await;

        assert!(service.send(Get).await.is_ok());
    }

    #[tokio::test]
    #[should_panic]
    async fn managed_relay_makes_upstream_request() {
        relay_test::setup();
        tokio::time::pause();

        let (upstream, handle) = mock_service("upstream", (), |(), _| {
            panic!();
        });

        let mut config = Config::from_json_value(serde_json::json!({
            "relay": {
                "mode":  RelayMode::Managed
            }
        }))
        .unwrap();
        config.regenerate_credentials(false).unwrap();

        let fetch_interval = config.global_config_fetch_interval();
        let service = GlobalConfigService::new(Arc::new(config), upstream).start();
        service.send(Get).await.unwrap();

        tokio::time::sleep(fetch_interval * 2).await;
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn proxy_relay_does_not_make_upstream_request() {
        relay_test::setup();
        tokio::time::pause();

        let (upstream, _) = mock_service("upstream", (), |(), _| {
            panic!("upstream should not be called outside of managed mode");
        });

        let config = Config::from_json_value(serde_json::json!({
            "relay": {
                "mode":  RelayMode::Proxy
            }
        }))
        .unwrap();

        let fetch_interval = config.global_config_fetch_interval();

        let service = GlobalConfigService::new(Arc::new(config), upstream).start();
        service.send(Get).await.unwrap();

        tokio::time::sleep(fetch_interval * 2).await;
    }
}
