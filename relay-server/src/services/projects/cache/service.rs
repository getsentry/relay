use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_system::Service;
use tokio::sync::{broadcast, mpsc};

use crate::services::projects::cache::handle::ProjectCacheHandle;
use crate::services::projects::cache::state::{CompletedFetch, Fetch, ProjectStore};
use crate::services::projects::project::ProjectState;
use crate::services::projects::source::ProjectSource;

/// Size of the broadcast channel for project events.
///
/// This is set to a value which theoretically should never be reachable,
/// the number of events is approximately bounded by the amount of projects
/// receiving events.
///
/// It is set to such a large amount because receivers of events currently
/// do not deal with lags in the channel gracefuly.
const PROJECT_EVENTS_CHANNEL_SIZE: usize = 512_000;

/// A cache for projects, which allows concurrent access to the cached projects.
#[derive(Debug)]
pub enum ProjectCache {
    /// Schedules another fetch or update for the specified project.
    ///
    /// A project which is not fetched will eventually expire and be evicted
    /// from the cache. Fetches for an already cached project ensure the project
    /// is always up to date and not evicted.
    Fetch(ProjectKey),
}

impl relay_system::Interface for ProjectCache {}

impl relay_system::FromMessage<Self> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: Self, _: ()) -> Self {
        message
    }
}

#[derive(Debug, Copy, Clone)]
pub enum ProjectEvent {
    Ready(ProjectKey),
    Evicted(ProjectKey),
}

/// A service implementing the [`ProjectCache`] interface.
pub struct ProjectCacheService {
    store: ProjectStore,
    source: ProjectSource,
    config: Arc<Config>,

    project_update_rx: mpsc::UnboundedReceiver<CompletedFetch>,
    project_update_tx: mpsc::UnboundedSender<CompletedFetch>,

    project_events_tx: broadcast::Sender<ProjectEvent>,
}

impl ProjectCacheService {
    /// Creates a new [`ProjectCacheService`].
    pub fn new(config: Arc<Config>, source: ProjectSource) -> Self {
        let (project_update_tx, project_update_rx) = mpsc::unbounded_channel();
        let project_events_tx = broadcast::channel(PROJECT_EVENTS_CHANNEL_SIZE).0;

        Self {
            store: ProjectStore::default(),
            source,
            config,
            project_update_rx,
            project_update_tx,
            project_events_tx,
        }
    }

    /// Consumes and starts a [`ProjectCacheService`].
    ///
    /// Returns a [`relay_system::Addr`] to communicate with the cache and a [`ProjectCacheHandle`]
    /// to access the cache concurrently.
    pub fn start(self) -> (relay_system::Addr<ProjectCache>, ProjectCacheHandle) {
        let (addr, addr_rx) = relay_system::channel(Self::name());

        let handle = ProjectCacheHandle {
            shared: self.store.shared(),
            config: Arc::clone(&self.config),
            service: addr.clone(),
            project_events: self.project_events_tx.clone(),
        };

        self.spawn_handler(addr_rx);

        (addr, handle)
    }

    /// Schedules a new [`Fetch`] and delivers the result to the [`Self::project_update_tx`] channel.
    fn schedule_fetch(&self, fetch: Fetch) {
        let source = self.source.clone();
        let project_updates = self.project_update_tx.clone();

        tokio::spawn(async move {
            tokio::time::sleep_until(fetch.when().into()).await;

            // TODO: cached state for delta fetches, maybe this should just be a revision?
            let state = match source
                .fetch(fetch.project_key(), false, ProjectState::Pending)
                .await
            {
                // TODO: verify if the sanitized here is correct
                Ok(state) => state.sanitized().into(),
                Err(err) => {
                    relay_log::error!(
                        error = &err as &dyn std::error::Error,
                        "failed to fetch project state for {fetch:?}"
                    );
                    ProjectState::Pending
                }
            };

            let _ = project_updates.send(fetch.complete(state));
        });
    }
}

/// All [`ProjectCacheService`] message handlers.
impl ProjectCacheService {
    fn handle_fetch(&mut self, project_key: ProjectKey) {
        if let Some(fetch) = self.store.try_begin_fetch(project_key, &self.config) {
            self.schedule_fetch(fetch);
        }
    }

    fn handle_project_update(&mut self, fetch: CompletedFetch) {
        let project_key = fetch.project_key();

        if let Some(fetch) = self.store.complete_fetch(fetch, &self.config) {
            relay_log::trace!(
                project_key = fetch.project_key().as_str(),
                "re-scheduling project fetch: {fetch:?}"
            );
            self.schedule_fetch(fetch);
            return;
        }

        // TODO: no-ops from revision checks should not end up here
        let _ = self
            .project_events_tx
            .send(ProjectEvent::Ready(project_key));
    }

    fn handle_evict_stale_projects(&mut self) {
        let on_evict = |project_key| {
            let _ = self
                .project_events_tx
                .send(ProjectEvent::Evicted(project_key));
        };

        self.store.evict_stale_projects(&self.config, on_evict);
    }

    fn handle_message(&mut self, message: ProjectCache) {
        match message {
            ProjectCache::Fetch(project_key) => self.handle_fetch(project_key),
        }
    }
}

impl relay_system::Service for ProjectCacheService {
    type Interface = ProjectCache;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut eviction_ticker = tokio::time::interval(self.config.cache_eviction_interval());

            loop {
                tokio::select! {
                    biased;

                    Some(update) = self.project_update_rx.recv() => {
                        self.handle_project_update(update)
                    },
                    Some(message) = rx.recv() => {
                        self.handle_message(message);
                    },
                    _ = eviction_ticker.tick() => {
                        self.handle_evict_stale_projects()
                    }
                }
            }
        });
    }
}
