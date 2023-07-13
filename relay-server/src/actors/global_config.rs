use std::borrow::Cow;
use std::sync::Arc;

use relay_dynamic_config::GlobalConfig;
use relay_system::{Addr, Interface, Service};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;

use crate::actors::project_cache::ProjectCache;
use crate::actors::upstream::{RequestPriority, SendQuery, UpstreamQuery, UpstreamRelay};

/// Represents the global config request made to upstream.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetGlobalConfig {
    global: bool,
}

impl GetGlobalConfig {
    /// Returns the global config query.
    ///
    /// Since this actor aims to deal with global configs, `global` is always `true`.
    fn query() -> Self {
        GetGlobalConfig { global: true }
    }
}

/// Represents the global config response returned from upstream.
///
/// This actor only requests global config entries and ignores the rest of
/// entries belonging to project config.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetGlobalConfigResponse {
    global: GlobalConfig,
}

impl UpstreamQuery for GetGlobalConfig {
    type Response = GetGlobalConfigResponse;

    fn method(&self) -> Method {
        Method::POST
    }

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/api/0/relays/projectconfigs/?version=4")
    }

    fn priority() -> super::upstream::RequestPriority {
        RequestPriority::High
    }

    fn retry() -> bool {
        true
    }

    fn route(&self) -> &'static str {
        "global_config"
    }
}

// No messages are accepted for now.
#[derive(Debug)]
pub enum GlobalConfigMessage {}

impl Interface for GlobalConfigMessage {}

/// Helper type to forward global config updates to other actors.
pub struct UpdateGlobalConfig {
    global_config: Arc<GlobalConfig>,
}

impl From<GetGlobalConfigResponse> for UpdateGlobalConfig {
    fn from(value: GetGlobalConfigResponse) -> Self {
        UpdateGlobalConfig {
            global_config: Arc::new(value.global),
        }
    }
}

/// Service implementing the [`GlobalConfig`] interface.
///
/// The service is responsible to fetch the global config appropriately and
/// forward it to the services that require it.
#[derive(Debug)]
pub struct GlobalConfigService {
    project_cache: Addr<ProjectCache>,
    upstream_relay: Addr<UpstreamRelay>,
    config_update_tx: mpsc::UnboundedSender<UpdateGlobalConfig>,
    config_update_rx: mpsc::UnboundedReceiver<UpdateGlobalConfig>,
}

impl GlobalConfigService {
    pub fn new(project_cache: Addr<ProjectCache>, upstream_relay: Addr<UpstreamRelay>) -> Self {
        let (config_tx, config_rx) = mpsc::unbounded_channel();
        GlobalConfigService {
            project_cache,
            upstream_relay,
            config_update_tx: config_tx,
            config_update_rx: config_rx,
        }
    }

    /// Forwards the given global config to the services that require it.
    fn update_global_config(&mut self, new_config: UpdateGlobalConfig) {
        self.project_cache.send(new_config.global_config);
    }

    /// Requests Relay's global config to upstream.
    ///
    /// Forwards the deserialized response through the internal config channel.
    fn request_global_config(&self) {
        let tx = self.config_update_tx.clone();
        let upstream_relay = self.upstream_relay.clone();

        tokio::spawn(async move {
            let query = GetGlobalConfig::query();
            match upstream_relay.send(SendQuery(query)).await {
                Ok(request_response) => match request_response {
                    Ok(config_response) => {
                        if tx.send(config_response.into()).is_err() {
                            relay_log::error!("Global config forwarding error");
                        }
                    }
                    Err(_) => {
                        relay_log::warn!("Global config server response errored");
                    }
                },
                Err(_) => {
                    relay_log::error!("Global config service errored requesting global config");
                }
            };
        });
    }
}

impl Service for GlobalConfigService {
    type Interface = GlobalConfigMessage;

    fn spawn_handler(mut self, _rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            // TODO(iker): make this value configurable from the configuration file.
            let mut ticker = tokio::time::interval(Duration::from_secs(10));

            relay_log::info!("global config started");

            loop {
                tokio::select! {
                    biased;

                    Some(message) = self.config_update_rx.recv() => self.update_global_config(message),
                    _ = ticker.tick() => self.request_global_config(),

                    else => break,
                }
            }
            relay_log::info!("global config stopped");
        });
    }
}
