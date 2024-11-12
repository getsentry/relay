use std::sync::Arc;

use futures::future::BoxFuture;
use futures::StreamExt as _;
use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_statsd::metric;
use relay_system::Service;
use tokio::sync::broadcast;

use crate::services::projects::cache::handle::ProjectCacheHandle;
use crate::services::projects::cache::state::{CompletedFetch, Fetch, ProjectStore};
use crate::services::projects::project::ProjectState;
use crate::services::projects::source::ProjectSource;
use crate::statsd::{RelayGauges, RelayTimers};
use crate::utils::FuturesScheduled;

/// Size of the broadcast channel for project events.
///
/// This is set to a value which theoretically should never be reachable,
/// the number of events is approximately bounded by the amount of projects
/// receiving events.
///
/// It is set to such a large amount because receivers of events currently
/// do not deal with lags in the channel gracefully.
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

impl ProjectCache {
    fn variant(&self) -> &'static str {
        match self {
            Self::Fetch(_) => "fetch",
        }
    }
}

impl relay_system::Interface for ProjectCache {}

impl relay_system::FromMessage<Self> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: Self, _: ()) -> Self {
        message
    }
}

/// Project life-cycle changes produced by the project cache.
#[derive(Debug, Copy, Clone)]
pub enum ProjectChange {
    /// A project was successfully fetched and is now ready to use.
    Ready(ProjectKey),
    /// A project expired from the cache and was evicted.
    Evicted(ProjectKey),
}

/// A service implementing the [`ProjectCache`] interface.
pub struct ProjectCacheService {
    store: ProjectStore,
    source: ProjectSource,
    config: Arc<Config>,

    scheduled_fetches: FuturesScheduled<BoxFuture<'static, CompletedFetch>>,

    project_events_tx: broadcast::Sender<ProjectChange>,
}

impl ProjectCacheService {
    /// Creates a new [`ProjectCacheService`].
    pub fn new(config: Arc<Config>, source: ProjectSource) -> Self {
        let project_events_tx = broadcast::channel(PROJECT_EVENTS_CHANNEL_SIZE).0;

        Self {
            store: ProjectStore::default(),
            source,
            config,
            scheduled_fetches: FuturesScheduled::default(),
            project_events_tx,
        }
    }

    /// Consumes and starts a [`ProjectCacheService`].
    ///
    /// Returns a [`ProjectCacheHandle`] to access the cache concurrently.
    pub fn start(self) -> ProjectCacheHandle {
        let (addr, addr_rx) = relay_system::channel(Self::name());

        let handle = ProjectCacheHandle {
            shared: self.store.shared(),
            config: Arc::clone(&self.config),
            service: addr,
            project_changes: self.project_events_tx.clone(),
        };

        self.spawn_handler(addr_rx);

        handle
    }

    /// Schedules a new [`Fetch`] in [`Self::scheduled_fetches`].
    fn schedule_fetch(&mut self, fetch: Fetch) {
        let source = self.source.clone();

        let when = fetch.when();
        let task = async move {
            let state = match source
                .fetch(fetch.project_key(), false, fetch.revision())
                .await
            {
                Ok(result) => result,
                Err(err) => {
                    relay_log::error!(
                        tags.project_key = fetch.project_key().as_str(),
                        error = &err as &dyn std::error::Error,
                        "failed to fetch project from source: {fetch:?}"
                    );

                    // TODO: change this to ProjectState::Pending once we consider it safe to do so.
                    // see https://github.com/getsentry/relay/pull/4140.
                    ProjectState::Disabled.into()
                }
            };

            fetch.complete(state)
        };
        self.scheduled_fetches.schedule(when, Box::pin(task));

        metric!(
            gauge(RelayGauges::ProjectCacheScheduledFetches) = self.scheduled_fetches.len() as u64
        );
    }
}

/// All [`ProjectCacheService`] message handlers.
impl ProjectCacheService {
    fn handle_fetch(&mut self, project_key: ProjectKey) {
        if let Some(fetch) = self.store.try_begin_fetch(project_key, &self.config) {
            self.schedule_fetch(fetch);
        }
    }

    fn handle_completed_fetch(&mut self, fetch: CompletedFetch) {
        let project_key = fetch.project_key();

        if let Some(fetch) = self.store.complete_fetch(fetch, &self.config) {
            relay_log::trace!(
                project_key = fetch.project_key().as_str(),
                "re-scheduling project fetch: {fetch:?}"
            );
            self.schedule_fetch(fetch);
            return;
        }

        let _ = self
            .project_events_tx
            .send(ProjectChange::Ready(project_key));

        metric!(
            gauge(RelayGauges::ProjectCacheNotificationChannel) =
                self.project_events_tx.len() as u64
        );
    }

    fn handle_evict_stale_projects(&mut self) {
        let on_evict = |project_key| {
            let _ = self
                .project_events_tx
                .send(ProjectChange::Evicted(project_key));
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
        macro_rules! timed {
            ($task:expr, $body:expr) => {{
                let task_name = $task;
                metric!(
                    timer(RelayTimers::ProjectCacheTaskDuration),
                    task = task_name,
                    { $body }
                )
            }};
        }

        tokio::spawn(async move {
            let mut eviction_ticker = tokio::time::interval(self.config.cache_eviction_interval());
            eviction_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;

                    Some(fetch) = self.scheduled_fetches.next() => timed!(
                        "completed_fetch",
                        {
                            self.handle_completed_fetch(fetch);
                            tokio::task::yield_now().await;
                        }
                    ),
                    Some(message) = rx.recv() => timed!(
                        message.variant(),
                        self.handle_message(message)
                    ),
                    _ = eviction_ticker.tick() => timed!(
                        "evict_stale_projects",
                        {
                            self.handle_evict_stale_projects();
                            tokio::task::yield_now().await;
                        }
                    ),
                }
            }
        });
    }
}
