use std::borrow::Cow;
use std::sync::Arc;

use relay_dynamic_config::GlobalConfig;
use relay_system::{Addr, Interface, Service};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::time::Duration;

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

    /// Requests Relay's global config to upstream.
    ///
    /// Forwards the global config in the response to the services that require
    /// it.
    fn request_global_config(&self) {
        let project_cache = self.project_cache.clone();
        let upstream_relay = self.upstream_relay.clone();

        tokio::spawn(async move {
            let query = GetGlobalConfig::query();
            match upstream_relay.send(SendQuery(query)).await {
                Ok(request_response) => match request_response {
                    Ok(config_response) => {
                        project_cache.send(Arc::new(config_response.global));
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

    fn spawn_handler(self, _rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            // TODO(iker): make this value configurable from the configuration file.
            let mut ticker = tokio::time::interval(Duration::from_secs(10));

            relay_log::info!("global config started");

            loop {
                tokio::select! {
                    biased;

                    _ = ticker.tick() => self.request_global_config(),

                    else => break,
                }
            }
            relay_log::info!("global config stopped");
        });
    }
}
