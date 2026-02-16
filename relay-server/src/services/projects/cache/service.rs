use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt as _;
use futures::future::BoxFuture;
use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_statsd::metric;
use relay_system::{Sender, Service, ServiceSpawn, ServiceSpawnExt as _};
use tokio::sync::broadcast;

use crate::services::projects::cache::handle::ProjectCacheHandle;
use crate::services::projects::cache::state::{
    self, CompletedFetch, Eviction, Fetch, ProjectStore, Refresh,
};
use crate::services::projects::project::ProjectState;
use crate::services::projects::source::{ProjectSource, ProjectSourceError, upstream};
use crate::statsd::{RelayCounters, RelayGauges, RelayTimers};
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

pub struct FetchRequest(pub ProjectKey);

/// A cache for projects, which allows concurrent access to the cached projects.
#[derive(Debug)]
pub enum ProjectCache {
    /// Schedules another fetch or update for the specified project.
    ///
    /// A project which is not fetched will eventually expire and be evicted
    /// from the cache. Fetches for an already cached project ensure the project
    /// is always up to date and not evicted.
    Fetch(ProjectKey, relay_system::Sender<()>),
}

impl ProjectCache {
    fn variant(&self) -> &'static str {
        match self {
            Self::Fetch(_, _) => "fetch",
        }
    }
}

impl relay_system::Interface for ProjectCache {}

impl relay_system::FromMessage<FetchRequest> for ProjectCache {
    type Response = relay_system::AsyncResponse<()>;

    fn from_message(
        FetchRequest(project_key): FetchRequest,
        sender: relay_system::Sender<()>,
    ) -> Self {
        Self::Fetch(project_key, sender)
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
    observers: Observers,
}

impl ProjectCacheService {
    /// Creates a new [`ProjectCacheService`].
    pub fn new(config: Arc<Config>, source: ProjectSource) -> Self {
        Self {
            store: ProjectStore::new(&config),
            source,
            config,
            scheduled_fetches: FuturesScheduled::default(),
            observers: Observers::new(),
        }
    }

    /// Consumes and starts a [`ProjectCacheService`].
    ///
    /// Returns a [`ProjectCacheHandle`] to access the cache concurrently.
    pub fn start_in(self, services: &dyn ServiceSpawn) -> ProjectCacheHandle {
        let (addr, addr_rx) = relay_system::channel(Self::name());

        let handle = ProjectCacheHandle {
            shared: self.store.shared(),
            config: Arc::clone(&self.config),
            service: addr,
            project_changes: self.observers.broadcast(),
        };

        services.start_with(self, addr_rx);

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
                Err(ProjectSourceError::Upstream(upstream::Error::DeadlineExceeded)) => {
                    // Somewhat of an expected error which is already logged on the upstream side.
                    //
                    // -> we can just go into our usual pending handling.
                    ProjectState::Pending.into()
                }
                Err(err @ ProjectSourceError::FatalUpstream) => {
                    relay_log::error!(
                        tags.project_key = fetch.project_key().as_str(),
                        tags.has_revision = fetch.revision().as_str().is_some(),
                        error = &err as &dyn std::error::Error,
                        "failed to fetch project from source: {fetch:?}"
                    );

                    ProjectState::Pending.into()
                }
            };

            fetch.complete(state)
        };
        self.scheduled_fetches.schedule(when, Box::pin(task));

        metric!(counter(RelayCounters::ProjectCacheSchedule) += 1);
        metric!(
            gauge(RelayGauges::ProjectCacheScheduledFetches) = self.scheduled_fetches.len() as u64
        );
    }
}

/// All [`ProjectCacheService`] message handlers.
impl ProjectCacheService {
    fn handle_fetch(&mut self, project_key: ProjectKey, sender: relay_system::Sender<()>) {
        self.observers
            .oneshot
            .entry(project_key)
            .or_default()
            .push(sender);
        if let Some(fetch) = self.store.try_begin_fetch(project_key) {
            self.schedule_fetch(fetch);
        }
    }

    fn handle_completed_fetch(&mut self, fetch: CompletedFetch) {
        let project_key = fetch.project_key();

        if let Some(fetch) = self.store.complete_fetch(fetch) {
            relay_log::trace!(
                project_key = fetch.project_key().as_str(),
                "re-scheduling project fetch: {fetch:?}"
            );
            self.schedule_fetch(fetch);
            return;
        }

        self.observers.notify(ProjectChange::Ready(project_key));

        metric!(
            gauge(RelayGauges::ProjectCacheObserversBroadcast) =
                self.observers.broadcast.len() as u64
        );
        metric!(
            gauge(RelayGauges::ProjectCacheObserversOneshot) = self.observers.oneshot.len() as u64
        );
    }

    fn handle_eviction(&mut self, eviction: Eviction) {
        let project_key = eviction.project_key();

        self.store.evict(eviction);

        self.observers.notify(ProjectChange::Evicted(project_key));

        relay_log::trace!(tags.project_key = project_key.as_str(), "project evicted");
        metric!(counter(RelayCounters::EvictingStaleProjectCaches) += 1);
    }

    fn handle_refresh(&mut self, refresh: Refresh) {
        let project_key = refresh.project_key();

        if let Some(fetch) = self.store.refresh(refresh) {
            self.schedule_fetch(fetch);
        }

        relay_log::trace!(tags.project_key = project_key.as_str(), "project refreshed");
        metric!(counter(RelayCounters::RefreshStaleProjectCaches) += 1);
    }

    fn handle_message(&mut self, message: ProjectCache) {
        match message {
            ProjectCache::Fetch(project_key, sender) => self.handle_fetch(project_key, sender),
        }
    }
}

impl relay_system::Service for ProjectCacheService {
    type Interface = ProjectCache;

    async fn run(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
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

        loop {
            tokio::select! {
                biased;

                Some(fetch) = self.scheduled_fetches.next() => timed!(
                    "completed_fetch",
                    self.handle_completed_fetch(fetch)
                ),
                Some(message) = rx.recv() => timed!(
                    message.variant(),
                    self.handle_message(message)
                ),
                Some(action) = self.store.poll() => match action {
                    state::Action::Eviction(eviction) => timed!(
                        "eviction",
                        self.handle_eviction(eviction)
                    ),
                    state::Action::Refresh(refresh) => timed!(
                        "refresh",
                        self.handle_refresh(refresh)
                    ),
                }
            }
        }
    }
}

struct Observers {
    broadcast: broadcast::Sender<ProjectChange>,
    oneshot: HashMap<ProjectKey, Vec<Sender<()>>>,
}

impl Observers {
    fn new() -> Self {
        Self {
            broadcast: broadcast::channel(PROJECT_EVENTS_CHANNEL_SIZE).0,
            oneshot: HashMap::new(),
        }
    }

    fn broadcast(&self) -> broadcast::Sender<ProjectChange> {
        self.broadcast.clone()
    }

    fn notify(&mut self, change: ProjectChange) {
        let Self { broadcast, oneshot } = self;

        let _ = broadcast.send(change);

        if let ProjectChange::Ready(key) = change {
            for channel in oneshot.remove(&key).unwrap_or_default() {
                let _ = channel.send(());
            }
        }
    }
}
