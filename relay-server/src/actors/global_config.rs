use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use hashbrown::HashMap;
use relay_common::ProjectKey;
use relay_dynamic_config::GlobalConfig;
use relay_system::{Addr, Interface, Service};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::actors::project_cache::ProjectCache;
use crate::actors::project_redis::RedisProjectSource;
use crate::actors::project_upstream::UpstreamResponse;
use crate::actors::upstream::{RequestPriority, SendQuery, UpstreamQuery, UpstreamRelayService};

// No messages are accepted for now.
#[derive(Debug)]
pub enum GlobalConfigMessage {}

impl Interface for GlobalConfigMessage {}

/// Helper type to forward global config updates.
pub struct UpdateGlobalConfig {
    global_config: Arc<GlobalConfig>,
}

/// Service implementing the [`GlobalConfig`] interface.
///
/// The service is responsible to fetch the global config appropriately and
/// forward it to the services that require it.
#[derive(Debug)]
pub struct GlobalConfigService {
    project_cache: Addr<ProjectCache>,
    upstream: Addr<UpstreamRelayService>,
}

impl Interface for GetGlobalConfig {}

impl GlobalConfigService {
    const PATH: &str = "sentry-api-0-relay-projectconfigs?version=4&global=true";

    pub fn new(project_cache: Addr<ProjectCache>) -> Self {
        GlobalConfigService { project_cache }
    }

    /// Forwards the given global config to the services that require it.
    fn update_global_config(&mut self) {
        let upstream_relay: Addr<UpstreamRelayService> = self.upstream.clone();
        tokio::spawn(async {
            let query = GetGlobalConfig;
            match upstream_relay.send(SendQuery(query)).await {
                Ok(response) => {}
                // If sending of the request to upstream fails:
                // - drop the current batch of the channels
                // - report the error, since this is the case we should not have in proper
                //   workflow
                // - return `None` to signal that we do not have any response from the Upstream
                //   and we should ignore this.
                Err(_err) => {
                    relay_log::error!("failed to send the request to upstream: channel full");
                    None
                }
            };
        });
    }
}

/// A query to retrieve a batch of project states from upstream.
///
/// This query does not implement `Deserialize`. To parse the query, use a wrapper that skips
/// invalid project keys instead of failing the entire batch.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetGlobalConfig;

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

/// The response of the projects states requests.
///
/// A [`ProjectKey`] is either pending or has a result, it can not appear in both and doing
/// so is undefined.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetGlobalConfigResponse {
    global_config: GlobalConfig,
}

impl UpstreamQuery for GetGlobalConfigResponse {
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
