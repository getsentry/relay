use std::sync::Arc;
use std::time::Duration;

use relay_dynamic_config::GlobalConfig;
use relay_system::{Addr, AsyncResponse, FromMessage, Interface, Sender, Service};

use crate::actors::processor::EnvelopeProcessor;
use crate::actors::project_upstream::GetProjectStates;
use crate::actors::upstream::{SendQuery, UpstreamRelay};

/// Service implementing the [`GlobalConfig`] interface.
///
/// The service is responsible for fetching the global config and
/// forwarding it to the services that require it.
#[derive(Debug)]
pub struct GlobalConfigService {
    global_config: Arc<GlobalConfig>,
    project_cache: Addr<EnvelopeProcessor>,
    upstream: Addr<UpstreamRelay>,
}

/// Service interface for the [`IsHealthy`] message.
pub struct GlobalConfigResponse(Sender<Arc<GlobalConfig>>);
pub struct GetGlobalConfig;

impl FromMessage<GetGlobalConfig> for GlobalConfigResponse {
    type Response = AsyncResponse<Arc<GlobalConfig>>;

    fn from_message(_message: GetGlobalConfig, sender: Sender<Arc<GlobalConfig>>) -> Self {
        Self(sender)
    }
}

impl Interface for GlobalConfigResponse {}

impl GlobalConfigService {
    pub fn new(project_cache: Addr<EnvelopeProcessor>, upstream: Addr<UpstreamRelay>) -> Self {
        let global_config = Arc::new(GlobalConfig::default());
        Self {
            global_config,
            project_cache,
            upstream,
        }
    }

    /// Forwards the given global config to the services that require it.
    async fn update_global_config(&mut self) {
        let upstream_relay: Addr<UpstreamRelay> = self.upstream.clone();
        let project_cache = self.project_cache.clone();

        let query = GetProjectStates {
            public_keys: vec![],
            full_config: false,
            no_cache: false,
            global_config: true,
        };

        match upstream_relay.send(SendQuery(query)).await {
            Ok(Ok(response)) => {
                self.global_config = response.global.clone();
                project_cache.send::<Arc<GlobalConfig>>(response.global);
            }
            Err(e) => {
                relay_log::error!("failed to send global config request: {}", e);
            }
            Ok(Err(e)) => {
                relay_log::error!("failed to fetch global config request: {}", e);
            }
        };
    }
}

impl Service for GlobalConfigService {
    type Interface = GlobalConfigResponse;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(10));
            relay_log::info!("global config started");

            loop {
                tokio::select! {
                    biased;
                    _ = ticker.tick() => self.update_global_config().await,
                    message = rx.recv() => {
                       let x =  message.unwrap();
                       x.0.send(self.global_config.clone());
                    },
                    else => break,
                }
            }
            relay_log::info!("global config stopped");
        });
    }
}
