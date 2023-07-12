use std::sync::Arc;

use relay_dynamic_config::GlobalConfig;
use relay_system::{Addr, Interface, Service};
use std::time::Duration;
use tokio::sync::mpsc;

use crate::actors::project_cache::ProjectCache;

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
}

impl GlobalConfigService {
    pub fn new(project_cache: Addr<ProjectCache>) -> Self {
        GlobalConfigService { project_cache }
    }

    /// Forwards the given global config to the services that require it.
    fn update_global_config(&mut self, new_config: UpdateGlobalConfig) {
        self.project_cache.send(new_config.global_config);
    }

    fn request_global_config(&self, config_tx: mpsc::UnboundedSender<UpdateGlobalConfig>) {
        //
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
