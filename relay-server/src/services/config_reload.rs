use std::sync::Arc;

use relay_config::Config;
use relay_system::{Receiver, Service};

use crate::statsd::RelayCounters;

/// Service which watches for configuration changes and reloads the config.
pub struct ConfigReloadService {
    config: Arc<Config>,
}

impl ConfigReloadService {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }

    async fn handle_reload(&mut self) {
        let config = Arc::clone(&self.config);
        let reload = tokio::task::spawn_blocking(move || config.reload()).await;

        match reload {
            Err(err) => {
                relay_log::warn!(
                    error = &err as &dyn std::error::Error,
                    "failed to reload the configuration"
                );
            }
            Ok(Err(err)) => {
                relay_log::warn!(
                    error = err.as_ref() as &dyn std::error::Error,
                    "failed to reload the configuration"
                );
            }
            Ok(Ok(true)) => {
                relay_statsd::metric!(counter(RelayCounters::ConfigReload) += 1);
                relay_log::info!("configuration reloaded!")
            }
            Ok(Ok(false)) => (),
        }
    }
}

impl Service for ConfigReloadService {
    type Interface = ();

    async fn run(mut self, _rx: Receiver<Self::Interface>) {
        #[cfg(not(test))]
        let mut shutdown_handle = relay_system::Controller::shutdown_handle();

        let Some(interval) = self.config.current().config_reload_interval() else {
            // No interval -> nothing to do.
            return;
        };

        relay_log::info!("watching for configuration changes every {interval:?}");

        loop {
            // Other tests may set the shutdown handle and never reset it.
            #[cfg(not(test))]
            if tokio::time::timeout(interval, shutdown_handle.notified())
                .await
                .is_ok()
            {
                // Shutdown initiated, we can just exit here.
                break;
            }
            #[cfg(test)]
            tokio::time::sleep(interval).await;

            self.handle_reload().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use super::*;

    /// Writes a new config with the specified `max_memory_bytes` and atomically replaces
    /// the old config with the new one using a rename.
    fn write_config_atomic(dir: &Path, max_memory_bytes: &str) {
        let tmp = dir.join("config.yml.tmp");
        let config = format!(
            r#"
relay:
  config_reload_interval: 0
health:
  max_memory_bytes: {max_memory_bytes}
"#
        );
        std::fs::write(&tmp, config).unwrap();
        std::fs::rename(&tmp, dir.join("config.yml")).unwrap();
    }

    async fn wait_for_reload(config: &Config, expected: u64) -> bool {
        for _ in 0..100 {
            if config.current().health_max_memory_watermark_bytes() == expected {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        config.current().health_max_memory_watermark_bytes() == expected
    }

    #[tokio::test]
    async fn test_reload_atomic_replace() {
        relay_test::setup();

        let dir = tempfile::tempdir().unwrap();
        write_config_atomic(dir.path(), "1000");

        let config = Arc::new(Config::from_path(dir.path()).unwrap());

        let service = ConfigReloadService::new(config.clone());
        service.start_detached();

        write_config_atomic(dir.path(), "2000");
        assert!(wait_for_reload(&config, 2000).await);

        write_config_atomic(dir.path(), "3000");
        assert!(wait_for_reload(&config, 3000).await);
    }

    #[tokio::test]
    async fn test_reload_file_reference_changes() {
        relay_test::setup();

        let dir = tempfile::tempdir().unwrap();
        let ref_file = dir.path().join("ref.value");
        write_config_atomic(dir.path(), &format!("${{file:{}}}", ref_file.display()));
        std::fs::write(&ref_file, b"1000").unwrap();

        let config = Arc::new(Config::from_path(dir.path()).unwrap());

        let service = ConfigReloadService::new(config.clone());
        service.start_detached();

        std::fs::write(&ref_file, b"2000").unwrap();
        assert!(wait_for_reload(&config, 2000).await);

        std::fs::write(&ref_file, b"3000").unwrap();
        assert!(wait_for_reload(&config, 3000).await);
    }
}
