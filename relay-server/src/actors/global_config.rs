use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use relay_dynamic_config::GlobalConfig;
use relay_system::{Addr, Service};
use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::actors::processor::EnvelopeProcessor;
use crate::actors::project_cache::ProjectCache;
use crate::actors::upstream::{RequestPriority, SendQuery, UpstreamQuery, UpstreamRelay};

/// Service implementing the [`GlobalConfig`] interface.
///
/// The service is responsible for fetching the global config and
/// forwarding it to the services that require it.
#[derive(Debug)]
pub struct GlobalConfigService {
    project_cache: Addr<EnvelopeProcessor>,
    upstream: Addr<UpstreamRelay>,
}

impl GlobalConfigService {
    pub fn new(project_cache: Addr<EnvelopeProcessor>, upstream: Addr<UpstreamRelay>) -> Self {
        Self {
            project_cache,
            upstream,
        }
    }

    /// Forwards the given global config to the services that require it.
    fn update_global_config(&mut self) {
        let upstream_relay: Addr<UpstreamRelay> = self.upstream.clone();
        let project_cache = self.project_cache.clone();
        tokio::spawn(async move {
            let query = GetGlobalConfig {
                global_config: true,
            };

            match upstream_relay.send(SendQuery(query)).await {
                Ok(Ok(response)) => {
                    //self.global_config = response.global.clone();
                    project_cache.send::<Arc<GlobalConfig>>(response.global);
                }
                Err(e) => {
                    relay_log::error!("failed to send global config request: {}", e);
                }
                Ok(Err(e)) => {
                    relay_log::error!("failed to fetch global config request: {}", e);
                }
            };
        });
    }
}

impl Service for GlobalConfigService {
    type Interface = ();

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
pub struct GetGlobalConfig {
    global_config: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetGlobalConfigResponse {
    global: Arc<GlobalConfig>,
}

impl UpstreamQuery for GetGlobalConfig {
    type Response = GetGlobalConfigResponse;

    fn method(&self) -> Method {
        Method::POST
    }

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/api/0/relays/projectconfigs/")
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
