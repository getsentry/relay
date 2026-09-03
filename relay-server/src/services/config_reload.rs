use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use notify_debouncer_mini::DebounceEventResult;
use notify_debouncer_mini::notify::{RecommendedWatcher, RecursiveMode};
use relay_config::Config;
use relay_system::{Controller, Receiver, Service};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::statsd::RelayCounters;

/// Duration over which config file changes are debounced.
const CONFIG_DEBOUNCE: Duration = Duration::from_secs(1);

/// Service which watches for configuration changes and reloads the config.
pub struct ConfigReloadService {
    config: Arc<Config>,
    watcher: notify_debouncer_mini::Debouncer<RecommendedWatcher>,
    events: UnboundedReceiver<DebounceEventResult>,
    currently_watched: BTreeSet<PathBuf>,
}

impl ConfigReloadService {
    pub fn new(config: Arc<Config>) -> anyhow::Result<Self> {
        let (tx, events) = tokio::sync::mpsc::unbounded_channel();

        let watcher = notify_debouncer_mini::new_debouncer(CONFIG_DEBOUNCE, TokioEventAdapter(tx))?;

        Ok(Self {
            config,
            watcher,
            events,
            currently_watched: Default::default(),
        })
    }

    fn update_watch(&mut self) {
        let config = self.config.current();
        let watcher = self.watcher.watcher();

        for to_remove in self.currently_watched.difference(config.source_files()) {
            relay_log::trace!("no longer watching {}", to_remove.display());
            let _ = watcher.unwatch(to_remove);
        }
        for to_add in config.source_files().difference(&self.currently_watched) {
            // No need for recursive, we're only watching files.
            match watcher.watch(to_add, RecursiveMode::NonRecursive) {
                Ok(()) => relay_log::info!("watching configuration file: {}", to_add.display()),
                Err(err) => {
                    relay_log::warn!(
                        error = &err as &dyn std::error::Error,
                        "failed to watch configuration file: {}",
                        to_add.display()
                    )
                }
            }
        }

        self.currently_watched = config.source_files().clone();
    }

    fn handle_watch_event(&mut self, event: DebounceEventResult) {
        let event = match event {
            Ok(event) => event,
            Err(err) => {
                relay_log::warn!(
                    error = &err as &dyn std::error::Error,
                    "config watch encountered an error"
                );
                return;
            }
        };

        for ev in event {
            relay_log::debug!("config changed: {}", ev.path.display());
        }

        relay_statsd::metric!(counter(RelayCounters::ConfigReload) += 1);

        // The reload does read from the file system sync, which may end up blocking
        // the Tokio thread for a bit. Since this is a very rare occasion and the runtime
        // is multi threaded, we accept this for now. Especially since the fs reads are expected
        // to be quite fast.
        match self.config.reload() {
            Ok(()) => relay_log::info!("configuration reloaded!"),
            Err(err) => {
                relay_log::warn!(
                    error = err.as_ref() as &dyn std::error::Error,
                    "failed to reload the configuration"
                );
            }
        }
        self.update_watch();
    }
}

impl Service for ConfigReloadService {
    type Interface = ();

    async fn run(mut self, _rx: Receiver<Self::Interface>) {
        let mut shutdown_handle = Controller::shutdown_handle();

        self.update_watch();

        loop {
            tokio::select! {
                Some(event) = self.events.recv() => self.handle_watch_event(event),
                _ = shutdown_handle.notified() => break,

                else => break,
            }
        }

        relay_log::info!("config reload service stopped");
    }
}

struct TokioEventAdapter(UnboundedSender<DebounceEventResult>);

impl notify_debouncer_mini::DebounceEventHandler for TokioEventAdapter {
    fn handle_event(&mut self, event: DebounceEventResult) {
        let _ = self.0.send(event);
    }
}
