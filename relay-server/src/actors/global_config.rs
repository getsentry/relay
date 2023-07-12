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

/// Helper type to forward global config updates.
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
}

impl GlobalConfigService {
    pub fn new(project_cache: Addr<ProjectCache>, upstream_relay: Addr<UpstreamRelay>) -> Self {
        GlobalConfigService {
            project_cache,
            upstream_relay,
        }
    }

    /// Forwards the given global config to the services that require it.
    fn update_global_config(&mut self, new_config: UpdateGlobalConfig) {
        self.project_cache.send(new_config.global_config);
    }

    fn request_global_config(&self, config_tx: mpsc::UnboundedSender<UpdateGlobalConfig>) {
        tokio::spawn(async move {
            let query = GetGlobalConfig::query();
            match self.upstream_relay.send(SendQuery(query)).await {
                Ok(response) => {
                    // TODO(iker): store the result in the service
                    match response {
                        Ok(config_response) => config_tx.send(config_response.into()),
                        Err(request_error) => {
                            todo!()
                        }
                    }
                }
                Err(send_error) => {
                    todo!()
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

            let (config_tx, mut config_rx) = mpsc::unbounded_channel();
            loop {
                tokio::select! {
                    biased;

                    Some(message) = config_rx.recv() => self.update_global_config(message),
                    _ = ticker.tick() => self.request_global_config(config_tx.clone()),
                    else => break,
                }
            }
            relay_log::info!("global config stopped");
        });
    }
}
