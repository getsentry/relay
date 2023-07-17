use std::borrow::Cow;
use std::time::Duration;

use relay_dynamic_config::GlobalConfig;
use relay_system::{Addr, Interface, Service};
use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::actors::project_cache::ProjectCache;
use crate::actors::upstream::{RequestPriority, SendQuery, UpstreamQuery, UpstreamRelay};

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
    upstream: Addr<UpstreamRelay>,
}

impl Interface for GetGlobalConfig {}

impl GlobalConfigService {
    pub fn new(project_cache: Addr<ProjectCache>, upstream: Addr<UpstreamRelay>) -> Self {
        Self {
            project_cache,
            upstream,
        }
    }

    /// Forwards the given global config to the services that require it.
    fn update_global_config(&mut self) {
        let upstream_relay: Addr<UpstreamRelay> = self.upstream.clone();
        let cache = self.project_cache.clone();
        tokio::spawn(async move {
            let query = GetGlobalConfig;

            if let Ok(Ok(response)) = upstream_relay.send(SendQuery(query)).await {
                let global_config = response.global;
                cache.send::<GlobalConfig>(global_config);
            };
        });
    }
}

impl Service for GlobalConfigService {
    type Interface = GlobalConfigMessage;

    fn spawn_handler(mut self, _rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(10));
            relay_log::info!("global config started");

            loop {
                tokio::select! {
                    biased;
                    _ = ticker.tick() => self.update_global_config(),
                    else => break,
                }
            }
            relay_log::info!("global config stopped");
        });
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetGlobalConfig;
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetGlobalConfigResponse {
    global: GlobalConfig,
}

impl UpstreamQuery for GetGlobalConfig {
    type Response = GetGlobalConfigResponse;

    fn method(&self) -> Method {
        Method::POST
    }

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/api/0/relays/projectconfigs/?version=4&global_config=true")
    }

    fn priority() -> RequestPriority {
        RequestPriority::High
    }

    fn retry() -> bool {
        false
    }

    fn route(&self) -> &'static str {
        "project_configs"
    }
}
