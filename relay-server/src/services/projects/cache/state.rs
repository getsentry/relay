use futures::StreamExt;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

use arc_swap::ArcSwap;
use relay_base_schema::project::ProjectKey;
use relay_quotas::CachedRateLimits;
use relay_sampling::evaluation::ReservoirCounters;
use relay_statsd::metric;

use crate::services::projects::project::{ProjectState, Revision};
use crate::services::projects::source::SourceProjectState;
use crate::statsd::{RelayDistributions, RelayTimers};
use crate::utils::{RetryBackoff, UniqueScheduledQueue};

/// The backing storage for a project cache.
///
/// Exposes the only interface to delete from [`Shared`], guaranteed by
/// requiring exclusive/mutable access to [`ProjectStore`].
///
/// [`Shared`] can be extended through [`Shared::get_or_create`], in which case
/// the private state is missing. Users of [`Shared::get_or_create`] *must* trigger
/// a fetch to create the private state and keep it updated.
/// This guarantees that eventually the project state is populated, but for a undetermined,
/// time it is possible that shared state exists without the respective private state.
pub struct ProjectStore {
    config: Config,
    /// The shared state, which can be accessed concurrently.
    shared: Arc<Shared>,
    /// The private, mutably exclusive state, used to maintain the project state.
    private: hashbrown::HashMap<ProjectKey, PrivateProjectState>,
    /// Scheduled queue tracking all evictions.
    evictions: UniqueScheduledQueue<ProjectKey>,
    /// Scheduled queue tracking all refreshes.
    refreshes: UniqueScheduledQueue<ProjectKey>,
}

impl ProjectStore {
    pub fn new(config: &relay_config::Config) -> Self {
        Self {
            config: Config::new(config),
            shared: Default::default(),
            private: Default::default(),
            evictions: Default::default(),
            refreshes: Default::default(),
        }
    }

    /// Retrieves a [`Shared`] handle which can be freely shared with multiple consumers.
    pub fn shared(&self) -> Arc<Shared> {
        Arc::clone(&self.shared)
    }

    /// Tries to begin a new fetch for the passed `project_key`.
    ///
    /// Returns `None` if no fetch is necessary or there is already a fetch ongoing.
    /// A returned [`Fetch`] must be scheduled and completed with [`Fetch::complete`] and
    /// [`Self::complete_fetch`].
    pub fn try_begin_fetch(&mut self, project_key: ProjectKey) -> Option<Fetch> {
        self.do_try_begin_fetch(project_key, false)
    }

    /// Completes a [`CompletedFetch`] started with [`Self::try_begin_fetch`].
    ///
    /// Returns a new [`Fetch`] if another fetch must be scheduled. This happens when the fetched
    /// [`ProjectState`] is still pending or already deemed expired.
    #[must_use = "an incomplete fetch must be retried"]
    pub fn complete_fetch(&mut self, fetch: CompletedFetch) -> Option<Fetch> {
        let project_key = fetch.project_key();

        // Eviction is not possible for projects which are currently being fetched.
        // Hence if there was a started fetch, the project state must always exist at this stage.
        debug_assert!(self.shared.projects.pin().get(&project_key).is_some());
        debug_assert!(self.private.get(&project_key).is_some());

        let mut project = self.get_or_create(project_key);
        // Schedule another fetch if necessary, usually should only happen if
        // the completed fetch is pending.
        let new_fetch = match project.complete_fetch(fetch) {
            FetchResult::ReSchedule { refresh } => project.try_begin_fetch(refresh),
            FetchResult::Done { expiry, refresh } => {
                self.evictions.schedule(expiry.0, project_key);
                if let Some(RefreshTime(refresh)) = refresh {
                    self.refreshes.schedule(refresh, project_key);
                }
                None
            }
        };

        metric!(
            distribution(RelayDistributions::ProjectStateCacheSize) =
                self.shared.projects.len() as u64,
            storage = "shared"
        );
        metric!(
            distribution(RelayDistributions::ProjectStateCacheSize) = self.private.len() as u64,
            storage = "private"
        );

        new_fetch
    }

    /// Waits for the next scheduled action.
    ///
    /// The returned [`Action`] must be immediately turned in using the corresponding handlers,
    /// [`Self::evict`] or [`Self::refresh`].
    ///
    /// The returned future is cancellation safe.
    pub async fn poll(&mut self) -> Option<Action> {
        let eviction = self.evictions.next();
        let refresh = self.refreshes.next();

        tokio::select! {
            biased;

            Some(e) = eviction => Some(Action::Eviction(Eviction(e))),
            Some(r) = refresh => Some(Action::Refresh(Refresh(r))),
            else => None,
        }
    }

    /// Refreshes a project using an [`Refresh`] token returned from [`Self::poll`].
    ///
    /// Like [`Self::try_begin_fetch`], this returns a [`Fetch`], if there was no fetch
    /// already started in the meantime.
    ///
    /// A returned [`Fetch`] must be scheduled and completed with [`Fetch::complete`] and
    /// [`Self::complete_fetch`].
    pub fn refresh(&mut self, Refresh(project_key): Refresh) -> Option<Fetch> {
        self.do_try_begin_fetch(project_key, true)
    }

    /// Evicts a project using an [`Eviction`] token returned from [`Self::poll`].
    pub fn evict(&mut self, Eviction(project_key): Eviction) {
        // Remove the private part.
        let Some(private) = self.private.remove(&project_key) else {
            // Not possible if all invariants are upheld.
            debug_assert!(false, "no private state for eviction");
            return;
        };

        debug_assert!(
            matches!(private.state, FetchState::Complete { .. }),
            "private state must be completed"
        );

        // Remove the shared part.
        let shared = self.shared.projects.pin();
        let _removed = shared.remove(&project_key);
        debug_assert!(
            _removed.is_some(),
            "an expired project must exist in the shared state"
        );

        // Cancel next refresh, while not necessary (trying to refresh a project which does not
        // exist, will do nothing), but we can also spare us the extra work.
        self.refreshes.remove(&project_key);
    }

    /// Internal handler to begin a new fetch for the passed `project_key`, which can also handle
    /// refreshes.
    fn do_try_begin_fetch(&mut self, project_key: ProjectKey, is_refresh: bool) -> Option<Fetch> {
        let fetch = match is_refresh {
            // A rogue refresh does not need to trigger an actual fetch.
            // In practice this should never happen, as the refresh time is validated against
            // the eviction time.
            // But it may happen due to a race of the eviction and refresh (e.g. when setting them
            // to close to the same value), in which case we don't want to re-populate the cache.
            true => self.get(project_key)?,
            false => self.get_or_create(project_key),
        }
        .try_begin_fetch(is_refresh);

        // If there is a new fetch, remove the pending eviction, it will be re-scheduled once the
        // fetch is completed.
        if fetch.is_some() {
            self.evictions.remove(&project_key);
            // There is no need to clear the refresh here, if it triggers while a fetch is ongoing,
            // it is simply discarded.
        }

        fetch
    }

    /// Get a reference to the current project or create a new project.
    ///
    /// For internal use only, a created project must always be fetched immediately.
    fn get(&mut self, project_key: ProjectKey) -> Option<ProjectRef<'_>> {
        let private = self.private.get_mut(&project_key)?;

        // Same invariant as in `get_or_create`, we have exclusive access to the private
        // project here, there must be a shared project if there is a private project.
        debug_assert!(self.shared.projects.pin().contains_key(&project_key));

        let shared = self
            .shared
            .projects
            .pin()
            .get_or_insert_with(project_key, Default::default)
            .clone();

        Some(ProjectRef {
            private,
            shared,
            config: &self.config,
        })
    }

    /// Get a reference to the current project or create a new project.
    ///
    /// For internal use only, a created project must always be fetched immediately.
    fn get_or_create(&mut self, project_key: ProjectKey) -> ProjectRef<'_> {
        #[cfg(debug_assertions)]
        if self.private.contains_key(&project_key) {
            // We have exclusive access to the private part, there are no concurrent deletions
            // hence if we have a private state there must always be a shared state as well.
            //
            // The opposite is not true, the shared state may have been created concurrently
            // through the shared access.
            debug_assert!(self.shared.projects.pin().contains_key(&project_key));
        }

        let private = self
            .private
            .entry(project_key)
            .or_insert_with(|| PrivateProjectState::new(project_key, &self.config));

        let shared = self
            .shared
            .projects
            .pin()
            .get_or_insert_with(project_key, Default::default)
            .clone();

        ProjectRef {
            private,
            shared,
            config: &self.config,
        }
    }
}

/// Configuration for a [`ProjectStore`].
struct Config {
    /// Expiry timeout for individual project configs.
    ///
    /// Note: the total expiry is the sum of the expiry and grace period.
    expiry: Duration,
    /// Grace period for a project config.
    ///
    /// A project config is considered stale and will be updated asynchronously,
    /// after reaching the grace period.
    grace_period: Duration,
    /// Refresh interval for a single project.
    ///
    /// A project will be asynchronously refreshed repeatedly using this interval.
    ///
    /// The refresh interval is validated to be between expiration and grace period. An invalid refresh
    /// time is ignored.
    refresh_interval: Option<Duration>,
    /// Maximum backoff for continuously failing project updates.
    max_retry_backoff: Duration,
}

impl Config {
    fn new(config: &relay_config::Config) -> Self {
        let expiry = config.project_cache_expiry();
        let grace_period = config.project_grace_period();

        // Make sure the refresh time is:
        // - at least the expiration, refreshing a non-stale project makes no sense.
        // - at most the end of the grace period, refreshing an expired project also makes no sense.
        let refresh_interval = config
            .project_refresh_interval()
            .filter(|rt| *rt < (expiry + grace_period))
            .filter(|rt| *rt > expiry);

        Self {
            expiry: config.project_cache_expiry(),
            grace_period: config.project_grace_period(),
            refresh_interval,
            max_retry_backoff: config.http_max_retry_interval(),
        }
    }
}

/// The shared and concurrently accessible handle to the project cache.
#[derive(Default)]
pub struct Shared {
    projects: papaya::HashMap<ProjectKey, SharedProjectState, ahash::RandomState>,
}

impl Shared {
    /// Returns the existing project state or creates a new one.
    ///
    /// The caller must ensure that the project cache is instructed to
    /// [`super::ProjectCache::Fetch`] the retrieved project.
    pub fn get_or_create(&self, project_key: ProjectKey) -> SharedProject {
        self.get_or_create_inner(project_key).to_shared_project()
    }

    /// Awaits until the contained project state becomes ready (enabled or disabled).
    ///
    /// Returns an empty [`Err`] if the project config cannot be resolved in the given time.
    pub async fn get_ready(
        &self,
        project_key: ProjectKey,
        timeout: Duration,
    ) -> Result<SharedProject, ()> {
        let shared = self.get_or_create_inner(project_key);
        shared.ready_project(timeout).await
    }

    fn get_or_create_inner(&self, project_key: ProjectKey) -> SharedProjectState {
        // The fast path, we expect the project to exist.
        let projects = self.projects.pin();
        if let Some(project) = projects.get(&project_key) {
            return project.clone();
        }

        // The slow path, try to attempt to insert, somebody else may have been faster, but that's okay.
        match projects.try_insert(project_key, Default::default()) {
            Ok(inserted) => inserted.clone(),
            Err(occupied) => occupied.current.clone(),
        }
    }
}

/// TEST ONLY bypass to make the project cache mockable.
#[cfg(test)]
impl Shared {
    /// Updates the project state for a project.
    ///
    /// TEST ONLY!
    pub fn test_set_project_state(&self, project_key: ProjectKey, state: ProjectState) {
        self.projects
            .pin()
            .get_or_insert_with(project_key, Default::default)
            .set_project_state(state);
    }

    /// Returns `true` if there exists a shared state for the passed `project_key`.
    pub fn test_has_project_created(&self, project_key: ProjectKey) -> bool {
        self.projects.pin().contains_key(&project_key)
    }
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("num_projects", &self.projects.len())
            .finish()
    }
}

/// A single project from the [`Shared`] project cache.
#[derive(Debug)]
pub struct SharedProject(Arc<SharedProjectStateInner>);

impl SharedProject {
    /// Returns a reference to the contained [`ProjectState`].
    pub fn project_state(&self) -> &ProjectState {
        &self.0.state
    }

    /// Returns a reference to the contained [`CachedRateLimits`].
    pub fn cached_rate_limits(&self) -> &CachedRateLimits {
        // Exposing cached rate limits may be a bad idea, this allows mutation
        // and caching of rate limits for pending projects, which may or may not be fine.
        // Although, for now this is fine.
        //
        // Read only access is easily achievable if we return only the current rate limits.
        &self.0.rate_limits
    }

    /// Returns a reference to the contained [`ReservoirCounters`].
    pub fn reservoir_counters(&self) -> &ReservoirCounters {
        &self.0.reservoir_counters
    }
}

/// TEST ONLY bypass to make the project cache mockable.
#[cfg(test)]
impl SharedProject {
    /// Creates a new [`SharedProject`] for testing only.
    pub fn for_test(state: ProjectState) -> Self {
        Self(Arc::new(SharedProjectStateInner {
            state,
            ..Default::default()
        }))
    }
}

/// Reference to a full project wrapping shared and private state.
struct ProjectRef<'a> {
    shared: SharedProjectState,
    private: &'a mut PrivateProjectState,
    config: &'a Config,
}

impl ProjectRef<'_> {
    fn try_begin_fetch(&mut self, is_refresh: bool) -> Option<Fetch> {
        let now = Instant::now();
        self.private
            .try_begin_fetch(now, is_refresh, self.config)
            .map(|fetch| fetch.with_revision(self.shared.revision()))
    }

    fn complete_fetch(&mut self, fetch: CompletedFetch) -> FetchResult {
        let now = Instant::now();

        if let Some(latency) = fetch.latency() {
            let delay = match fetch.delay() {
                Some(delay) if delay.as_secs() <= 15 => "lte15s",
                Some(delay) if delay.as_secs() <= 30 => "lte30s",
                Some(delay) if delay.as_secs() <= 60 => "lte60s",
                Some(delay) if delay.as_secs() <= 120 => "lte120",
                Some(delay) if delay.as_secs() <= 300 => "lte300s",
                Some(delay) if delay.as_secs() <= 600 => "lte600s",
                Some(delay) if delay.as_secs() <= 1800 => "lte1800s",
                Some(delay) if delay.as_secs() <= 3600 => "lte3600s",
                Some(_) => "gt3600s",
                None => "none",
            };
            metric!(
                timer(RelayTimers::ProjectCacheUpdateLatency) = latency,
                delay = delay
            );
        }

        if !fetch.is_pending() {
            let state = match fetch.state {
                SourceProjectState::New(_) => "new",
                SourceProjectState::NotModified => "not_modified",
            };

            metric!(
                timer(RelayTimers::ProjectCacheFetchDuration) = fetch.duration(now),
                state = state
            );
        }

        // Update private and shared state with the new data.
        let result = self.private.complete_fetch(&fetch, now, self.config);
        match fetch.state {
            // Keep the old state around if the current fetch is pending.
            // It may still be useful to callers.
            SourceProjectState::New(state) if !state.is_pending() => {
                self.shared.set_project_state(state);
            }
            _ => {}
        }

        result
    }
}

pub enum Action {
    Eviction(Eviction),
    Refresh(Refresh),
}

/// A [`Refresh`] token.
///
/// The token must be turned in using [`ProjectStore::refresh`].
#[derive(Debug)]
#[must_use = "a refresh must be used"]
pub struct Refresh(ProjectKey);

impl Refresh {
    /// Returns the [`ProjectKey`] of the project that needs to be refreshed.
    pub fn project_key(&self) -> ProjectKey {
        self.0
    }
}

/// A [`Eviction`] token.
///
/// The token must be turned in using [`ProjectStore::evict`].
#[derive(Debug)]
#[must_use = "an eviction must be used"]
pub struct Eviction(ProjectKey);

impl Eviction {
    /// Returns the [`ProjectKey`] of the project that needs to be evicted.
    pub fn project_key(&self) -> ProjectKey {
        self.0
    }
}

/// A [`Fetch`] token.
///
/// When returned it must be executed and completed using [`Self::complete`].
#[must_use = "a fetch must be executed"]
#[derive(Debug)]
pub struct Fetch {
    project_key: ProjectKey,
    previous_fetch: Option<Instant>,
    initiated: Instant,
    when: Option<Instant>,
    revision: Revision,
}

impl Fetch {
    /// Returns the [`ProjectKey`] of the project to fetch.
    pub fn project_key(&self) -> ProjectKey {
        self.project_key
    }

    /// Returns when the fetch for the project should be scheduled.
    ///
    /// This can be now (as soon as possible, indicated by `None`) or a later point in time,
    /// if the project is currently in a backoff.
    pub fn when(&self) -> Option<Instant> {
        self.when
    }

    /// Returns the revisions of the currently cached project.
    ///
    /// If the upstream indicates it does not have a different version of this project
    /// we do not need to update the local state.
    pub fn revision(&self) -> Revision {
        self.revision.clone()
    }

    /// Completes the fetch with a result and returns a [`CompletedFetch`].
    pub fn complete(self, state: SourceProjectState) -> CompletedFetch {
        CompletedFetch { fetch: self, state }
    }

    fn with_revision(mut self, revision: Revision) -> Self {
        self.revision = revision;
        self
    }
}

/// The result of an executed [`Fetch`].
#[must_use = "a completed fetch must be acted upon"]
#[derive(Debug)]
pub struct CompletedFetch {
    fetch: Fetch,
    state: SourceProjectState,
}

impl CompletedFetch {
    /// Returns the [`ProjectKey`] of the project which was fetched.
    pub fn project_key(&self) -> ProjectKey {
        self.fetch.project_key()
    }

    /// Returns the amount of time passed between the last successful fetch for this project and the start of this fetch.
    ///
    /// `None` if this is the first fetch.
    fn delay(&self) -> Option<Duration> {
        self.fetch
            .previous_fetch
            .map(|pf| self.fetch.initiated.duration_since(pf))
    }

    /// Returns the duration between first initiating the fetch and `now`.
    fn duration(&self, now: Instant) -> Duration {
        now.duration_since(self.fetch.initiated)
    }

    /// Returns the update latency of the fetched project config from the upstream.
    ///
    /// Is `None`, when no project config could be fetched, or if this was the first
    /// fetch of a project config.
    ///
    /// Note: this latency is computed on access, it does not use the time when the [`Fetch`]
    /// was marked as (completed)[`Fetch::complete`].
    fn latency(&self) -> Option<Duration> {
        // We're not interested in initial fetches. The latency on the first fetch
        // has no meaning about how long it takes for an updated project config to be
        // propagated to a Relay.
        let is_first_fetch = self.fetch.revision().as_str().is_none();
        if is_first_fetch {
            return None;
        }

        let project_info = match &self.state {
            SourceProjectState::New(ProjectState::Enabled(project_info)) => project_info,
            // Not modified or deleted/disabled -> no latency to track.
            //
            // Currently we discard the last changed timestamp for disabled projects,
            // it would be possible to do so and then also expose a latency for disabled projects.
            _ => return None,
        };

        // A matching revision is not an update.
        if project_info.rev == self.fetch.revision {
            return None;
        }

        let elapsed = chrono::Utc::now() - project_info.last_change?;
        elapsed.to_std().ok()
    }

    /// Returns `true` if the fetch completed with a pending status.
    fn is_pending(&self) -> bool {
        match &self.state {
            SourceProjectState::New(state) => state.is_pending(),
            SourceProjectState::NotModified => false,
        }
    }
}

/// The state of a project contained in the [`Shared`] project cache.
///
/// This state is interior mutable and allows updates to the project.
#[derive(Debug, Default, Clone)]
struct SharedProjectState(Arc<ArcSwap<SharedProjectStateInner>>);

impl SharedProjectState {
    /// Updates the project state.
    fn set_project_state(&self, state: ProjectState) {
        let prev = self.0.rcu(|stored| SharedProjectStateInner {
            state: state.clone(),
            rate_limits: Arc::clone(&stored.rate_limits),
            reservoir_counters: Arc::clone(&stored.reservoir_counters),
            notify: Arc::clone(&stored.notify),
        });

        // Try clean expired reservoir counters.
        //
        // We do it after the `rcu`, to not re-run this more often than necessary.
        if let Some(state) = state.enabled() {
            let config = state.config.sampling.as_ref();
            if let Some(config) = config.and_then(|eb| eb.as_ref().ok()) {
                // We can safely use previous here, the `rcu` just replaced the state, the
                // reservoir counters did not change.
                //
                // `try_lock` to not potentially block, it's a best effort cleanup.
                if let Ok(mut counters) = prev.reservoir_counters.try_lock() {
                    counters.retain(|key, _| config.rules.iter().any(|rule| rule.id == *key));
                }
            }
        }

        // Finally, notify listeners:
        prev.notify.notify_waiters();
    }

    /// Awaits until the contained project state becomes ready (enabled or disabled).
    ///
    /// Returns an empty [`Err`] if the project config cannot be resolved in the given time.
    pub async fn ready_project(&self, timeout: Duration) -> Result<SharedProject, ()> {
        tokio::time::timeout(timeout, self.ready_project_inner())
            .await
            .map_err(|_| ())
    }

    async fn ready_project_inner(&self) -> SharedProject {
        loop {
            let inner = self.0.load_full();
            let notified = inner.notify.notified();
            if !inner.state.is_pending() {
                return SharedProject(inner);
            }
            notified.await;
        }
    }

    /// Extracts and clones the revision from the contained project state.
    fn revision(&self) -> Revision {
        self.0.as_ref().load().state.revision().clone()
    }

    /// Transforms this interior mutable handle to an immutable [`SharedProject`].
    fn to_shared_project(&self) -> SharedProject {
        SharedProject(self.0.as_ref().load_full())
    }
}

/// The data contained in a [`SharedProjectState`].
///
/// All fields must be cheap to clone and are ideally just a single `Arc`.
/// Partial updates to [`SharedProjectState`], are performed using `rcu` cloning all fields.
#[derive(Debug, Default)]
struct SharedProjectStateInner {
    state: ProjectState,
    rate_limits: Arc<CachedRateLimits>,
    reservoir_counters: ReservoirCounters,
    notify: Arc<Notify>,
}

/// Current fetch state for a project.
///
/// ─────► Pending ◄─────┐
///           │          │
///           │          │Backoff
///           ▼          │
/// ┌───► InProgress ────┘
/// │         │
/// │         │
/// │         ▼
/// └───── Complete
#[derive(Debug)]
enum FetchState {
    /// There is a fetch currently in progress.
    InProgress {
        /// Whether the current check in progress was triggered from a refresh.
        ///
        /// Triggering a non-refresh fetch while a refresh fetch is currently in progress,
        /// will overwrite this property.
        is_refresh: bool,
    },
    /// A successful fetch is pending.
    ///
    /// Projects which have not yet been fetched are in the pending state,
    /// as well as projects which have a fetch in progress but were notified
    /// from upstream that the project config is still pending.
    ///
    /// If the upstream notifies this instance about a pending config,
    /// a backoff is applied, before trying again.
    Pending {
        /// Instant when the fetch was first initiated.
        ///
        /// A state may be transitioned multiple times from [`Self::Pending`] to [`Self::InProgress`]
        /// and back to [`Self::Pending`]. This timestamp is the first time when the state
        /// was transitioned from [`Self::Complete`] to [`Self::InProgress`].
        ///
        /// Only `None` on first fetch.
        initiated: Option<Instant>,
        /// Time when the next fetch should be attempted.
        ///
        /// `None` means soon as possible.
        next_fetch_attempt: Option<Instant>,
    },
    /// There was a successful non-pending fetch.
    Complete {
        /// Time when the fetch was completed.
        when: LastFetch,
    },
}

/// Contains all mutable state necessary to maintain the project cache.
struct PrivateProjectState {
    /// Project key this state belongs to.
    project_key: ProjectKey,

    /// The current fetch state.
    state: FetchState,
    /// The current backoff used for calculating the next fetch attempt.
    ///
    /// The backoff is reset after a successful, non-pending fetch.
    backoff: RetryBackoff,

    /// The last time the state was successfully fetched.
    ///
    /// May be `None` when the state has never been successfully fetched.
    ///
    /// This is purely informational, all necessary information to make
    /// state transitions is contained in [`FetchState`].
    last_fetch: Option<Instant>,

    /// The expiry time of this project.
    ///
    /// A refresh of the project, will not push the expiration time.
    expiry: Option<Instant>,
}

impl PrivateProjectState {
    fn new(project_key: ProjectKey, config: &Config) -> Self {
        Self {
            project_key,
            state: FetchState::Pending {
                initiated: None,
                next_fetch_attempt: None,
            },
            backoff: RetryBackoff::new(config.max_retry_backoff),
            last_fetch: None,
            expiry: None,
        }
    }

    fn try_begin_fetch(
        &mut self,
        now: Instant,
        is_refresh: bool,
        config: &Config,
    ) -> Option<Fetch> {
        let (initiated, when) = match &mut self.state {
            FetchState::InProgress {
                is_refresh: refresh_in_progress,
            } => {
                relay_log::trace!(
                    tags.project_key = self.project_key.as_str(),
                    "project fetch skipped, fetch in progress"
                );
                // Upgrade the refresh status if necessary.
                *refresh_in_progress = *refresh_in_progress && is_refresh;
                return None;
            }
            FetchState::Pending {
                initiated,
                next_fetch_attempt,
            } => {
                // Schedule a new fetch, even if there is a backoff, it will just be sleeping for a while.
                (initiated.unwrap_or(now), *next_fetch_attempt)
            }
            FetchState::Complete { when } => {
                // Sanity check to make sure timestamps do not drift.
                debug_assert_eq!(Some(when.0), self.last_fetch);

                if when.check_expiry(now, config).is_fresh() {
                    // The current state is up to date, no need to start another fetch.
                    relay_log::trace!(
                        tags.project_key = self.project_key.as_str(),
                        "project fetch skipped, already up to date"
                    );
                    return None;
                }

                (now, None)
            }
        };

        // Mark a current fetch in progress.
        self.state = FetchState::InProgress { is_refresh };

        relay_log::trace!(
            tags.project_key = &self.project_key.as_str(),
            attempts = self.backoff.attempt() + 1,
            "project state {} scheduled in {:?}",
            if is_refresh { "refresh" } else { "fetch" },
            when.unwrap_or(now).saturating_duration_since(now),
        );

        Some(Fetch {
            project_key: self.project_key,
            previous_fetch: self.last_fetch,
            initiated,
            when,
            revision: Revision::default(),
        })
    }

    fn complete_fetch(
        &mut self,
        fetch: &CompletedFetch,
        now: Instant,
        config: &Config,
    ) -> FetchResult {
        let FetchState::InProgress { is_refresh } = self.state else {
            debug_assert!(
                false,
                "fetch completed while there was no current fetch registered"
            );
            // Be conservative in production.
            return FetchResult::ReSchedule { refresh: false };
        };

        if fetch.is_pending() {
            let next_backoff = self.backoff.next_backoff();
            let next_fetch_attempt = match next_backoff.is_zero() {
                false => now.checked_add(next_backoff),
                true => None,
            };
            self.state = FetchState::Pending {
                next_fetch_attempt,
                initiated: Some(fetch.fetch.initiated),
            };
            relay_log::trace!(
                tags.project_key = &self.project_key.as_str(),
                "project state {} completed but still pending",
                if is_refresh { "refresh" } else { "fetch" },
            );

            FetchResult::ReSchedule {
                refresh: is_refresh,
            }
        } else {
            relay_log::trace!(
                tags.project_key = &self.project_key.as_str(),
                "project state {} completed with non-pending config",
                if is_refresh { "refresh" } else { "fetch" },
            );

            self.backoff.reset();
            self.last_fetch = Some(now);

            let when = LastFetch(now);

            let refresh = when.refresh_time(config);
            let expiry = match self.expiry {
                Some(expiry) if is_refresh => ExpiryTime(expiry),
                // Only bump/re-compute the expiry time if the fetch was not a refresh,
                // to not keep refreshed projects forever in the cache.
                Some(_) | None => when.expiry_time(config),
            };
            self.expiry = Some(expiry.0);

            self.state = FetchState::Complete { when };
            FetchResult::Done { expiry, refresh }
        }
    }
}

/// Result returned when completing a fetch.
#[derive(Debug)]
#[must_use = "fetch result must be used"]
enum FetchResult {
    /// Another fetch must be scheduled immediately.
    ReSchedule {
        /// Whether the fetch should be re-scheduled as a refresh.
        refresh: bool,
    },
    /// The fetch is completed and should be registered for refresh and eviction.
    Done {
        /// When the project should be expired.
        expiry: ExpiryTime,
        /// When the project should be refreshed.
        refresh: Option<RefreshTime>,
    },
}

/// New type containing the last successful fetch time as an [`Instant`].
#[derive(Debug, Copy, Clone)]
struct LastFetch(Instant);

impl LastFetch {
    /// Returns the [`Expiry`] of the last fetch in relation to `now`.
    fn check_expiry(&self, now: Instant, config: &Config) -> Expiry {
        let elapsed = now.saturating_duration_since(self.0);

        if elapsed >= config.expiry + config.grace_period {
            Expiry::Expired
        } else if elapsed >= config.expiry {
            Expiry::Stale
        } else {
            Expiry::Fresh
        }
    }

    /// Returns when the project needs to be queued for a refresh.
    fn refresh_time(&self, config: &Config) -> Option<RefreshTime> {
        config
            .refresh_interval
            .map(|duration| self.0 + duration)
            .map(RefreshTime)
    }

    /// Returns when the project is based to expire based on the current [`LastFetch`].
    fn expiry_time(&self, config: &Config) -> ExpiryTime {
        ExpiryTime(self.0 + config.grace_period + config.expiry)
    }
}

/// Expiry state of a project.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum Expiry {
    /// The project state is perfectly up to date.
    Fresh,
    /// The project state is outdated but events depending on this project state can still be
    /// processed. The state should be refreshed in the background though.
    Stale,
    /// The project state is completely outdated and events need to be buffered up until the new
    /// state has been fetched.
    Expired,
}

impl Expiry {
    /// Returns `true` if the project is up-to-date and does not need to be fetched.
    fn is_fresh(&self) -> bool {
        matches!(self, Self::Fresh)
    }
}

/// Instant when a project is scheduled for refresh.
#[derive(Debug)]
#[must_use = "an refresh time must be used to schedule a refresh"]
struct RefreshTime(Instant);

/// Instant when a project is scheduled for expiry.
#[derive(Debug)]
#[must_use = "an expiry time must be used to schedule an eviction"]
struct ExpiryTime(Instant);

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    async fn collect_evicted(store: &mut ProjectStore) -> Vec<ProjectKey> {
        let mut evicted = Vec::new();
        // Small timeout to really only get what is ready to be evicted right now.
        while let Ok(Some(Action::Eviction(eviction))) =
            tokio::time::timeout(Duration::from_nanos(5), store.poll()).await
        {
            evicted.push(eviction.0);
            store.evict(eviction);
        }
        evicted
    }

    macro_rules! assert_state {
        ($store:ident, $project_key:ident, $state:pat) => {
            assert!(matches!(
                $store.shared().get_or_create($project_key).project_state(),
                $state
            ));
        };
    }

    #[tokio::test(start_paused = true)]
    async fn test_store_fetch() {
        let project_key = ProjectKey::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut store = ProjectStore::new(&Default::default());

        let fetch = store.try_begin_fetch(project_key).unwrap();
        assert_eq!(fetch.project_key(), project_key);
        assert_eq!(fetch.when(), None);
        assert_eq!(fetch.revision().as_str(), None);
        assert_state!(store, project_key, ProjectState::Pending);

        // Fetch already in progress, nothing to do.
        assert!(store.try_begin_fetch(project_key).is_none());

        // A pending fetch should trigger a new fetch immediately.
        let fetch = fetch.complete(ProjectState::Pending.into());
        let fetch = store.complete_fetch(fetch).unwrap();
        assert_eq!(fetch.project_key(), project_key);
        // First backoff is still immediately.
        assert_eq!(fetch.when(), None);
        assert_eq!(fetch.revision().as_str(), None);
        assert_state!(store, project_key, ProjectState::Pending);

        // Pending again.
        let fetch = fetch.complete(ProjectState::Pending.into());
        let fetch = store.complete_fetch(fetch).unwrap();
        assert_eq!(fetch.project_key(), project_key);
        // This time it needs to be in the future (backoff).
        assert!(fetch.when() > Some(Instant::now()));
        assert_eq!(fetch.revision().as_str(), None);
        assert_state!(store, project_key, ProjectState::Pending);

        // Now complete with disabled.
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch).is_none());
        assert_state!(store, project_key, ProjectState::Disabled);

        // A new fetch is not yet necessary.
        assert!(store.try_begin_fetch(project_key).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn test_store_fetch_pending_does_not_replace_state() {
        let project_key = ProjectKey::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut store = ProjectStore::new(
            &relay_config::Config::from_json_value(serde_json::json!({
                "cache": {
                    "project_expiry": 5,
                    "project_grace_period": 5,
                }
            }))
            .unwrap(),
        );

        let fetch = store.try_begin_fetch(project_key).unwrap();
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch).is_none());
        assert_state!(store, project_key, ProjectState::Disabled);

        tokio::time::advance(Duration::from_secs(6)).await;

        let fetch = store.try_begin_fetch(project_key).unwrap();
        let fetch = fetch.complete(ProjectState::Pending.into());
        // We're returned a new fetch, because the current one completed pending.
        let fetch = store.complete_fetch(fetch).unwrap();
        // The old cached state is still available and not replaced.
        assert_state!(store, project_key, ProjectState::Disabled);

        let fetch = fetch.complete(ProjectState::new_allowed().into());
        assert!(store.complete_fetch(fetch).is_none());
        assert_state!(store, project_key, ProjectState::Enabled(_));
    }

    #[tokio::test(start_paused = true)]
    async fn test_store_evict_projects() {
        let project_key1 = ProjectKey::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let project_key2 = ProjectKey::parse("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        let mut store = ProjectStore::new(
            &relay_config::Config::from_json_value(serde_json::json!({
                "cache": {
                    "project_expiry": 5,
                    "project_grace_period": 0,
                }
            }))
            .unwrap(),
        );

        let fetch = store.try_begin_fetch(project_key1).unwrap();
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch).is_none());

        assert_eq!(collect_evicted(&mut store).await, Vec::new());
        assert_state!(store, project_key1, ProjectState::Disabled);

        // 3 seconds is not enough to expire any project.
        tokio::time::advance(Duration::from_secs(3)).await;

        assert_eq!(collect_evicted(&mut store).await, Vec::new());
        assert_state!(store, project_key1, ProjectState::Disabled);

        let fetch = store.try_begin_fetch(project_key2).unwrap();
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch).is_none());

        // A total of 6 seconds should expire the first project.
        tokio::time::advance(Duration::from_secs(3)).await;

        assert_eq!(collect_evicted(&mut store).await, vec![project_key1]);
        assert_state!(store, project_key1, ProjectState::Pending);
        assert_state!(store, project_key2, ProjectState::Disabled);
    }

    #[tokio::test(start_paused = true)]
    async fn test_store_evict_projects_pending_not_expired() {
        let project_key1 = ProjectKey::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let project_key2 = ProjectKey::parse("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        let mut store = ProjectStore::new(
            &relay_config::Config::from_json_value(serde_json::json!({
                "cache": {
                    "project_expiry": 5,
                    "project_grace_period": 0,
                }
            }))
            .unwrap(),
        );

        let fetch = store.try_begin_fetch(project_key1).unwrap();
        // Create a new project in a pending state, but never fetch it, this should also never expire.
        store.shared().get_or_create(project_key2);

        tokio::time::advance(Duration::from_secs(6)).await;

        // No evictions, project is pending.
        assert_eq!(collect_evicted(&mut store).await, Vec::new());

        // Complete the project.
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch).is_none());

        // Still should not be evicted, because we do have 5 seconds to expire since completion.
        assert_eq!(collect_evicted(&mut store).await, Vec::new());
        tokio::time::advance(Duration::from_secs(4)).await;
        assert_eq!(collect_evicted(&mut store).await, Vec::new());
        assert_state!(store, project_key1, ProjectState::Disabled);

        // Just enough to expire the project.
        tokio::time::advance(Duration::from_millis(1001)).await;
        assert_eq!(collect_evicted(&mut store).await, vec![project_key1]);
        assert_state!(store, project_key1, ProjectState::Pending);
        assert_state!(store, project_key2, ProjectState::Pending);
    }

    #[tokio::test(start_paused = true)]
    async fn test_store_evict_projects_stale() {
        let project_key = ProjectKey::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut store = ProjectStore::new(
            &relay_config::Config::from_json_value(serde_json::json!({
                "cache": {
                    "project_expiry": 5,
                    "project_grace_period": 5,
                }
            }))
            .unwrap(),
        );

        let fetch = store.try_begin_fetch(project_key).unwrap();
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch).is_none());

        // This is in the grace period, but not yet expired.
        tokio::time::advance(Duration::from_millis(9500)).await;

        assert_eq!(collect_evicted(&mut store).await, Vec::new());
        assert_state!(store, project_key, ProjectState::Disabled);

        // Now it's expired.
        tokio::time::advance(Duration::from_secs(1)).await;

        assert_eq!(collect_evicted(&mut store).await, vec![project_key]);
        assert_state!(store, project_key, ProjectState::Pending);
    }

    #[tokio::test(start_paused = true)]
    async fn test_store_no_eviction_during_fetch() {
        let project_key = ProjectKey::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut store = ProjectStore::new(
            &relay_config::Config::from_json_value(serde_json::json!({
                "cache": {
                    "project_expiry": 5,
                    "project_grace_period": 5,
                }
            }))
            .unwrap(),
        );

        let fetch = store.try_begin_fetch(project_key).unwrap();

        // Project is expired, but there is an ongoing fetch.
        tokio::time::advance(Duration::from_millis(10500)).await;
        // No evictions, there is a fetch ongoing!
        assert_eq!(collect_evicted(&mut store).await, Vec::new());

        // Complete the project.
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch).is_none());
        // But start a new fetch asap (after grace period).
        tokio::time::advance(Duration::from_millis(5001)).await;
        let fetch = store.try_begin_fetch(project_key).unwrap();

        // Again, expire the project.
        tokio::time::advance(Duration::from_millis(10500)).await;
        // No evictions, there is a fetch ongoing!
        assert_eq!(collect_evicted(&mut store).await, Vec::new());

        // Complete the project.
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch).is_none());

        // Not quite yet expired.
        tokio::time::advance(Duration::from_millis(9500)).await;
        assert_eq!(collect_evicted(&mut store).await, Vec::new());
        // Now it's expired.
        tokio::time::advance(Duration::from_millis(501)).await;
        assert_eq!(collect_evicted(&mut store).await, vec![project_key]);
        assert_state!(store, project_key, ProjectState::Pending);
    }

    #[tokio::test(start_paused = true)]
    async fn test_store_refresh() {
        let project_key = ProjectKey::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut store = ProjectStore::new(
            &relay_config::Config::from_json_value(serde_json::json!({
                "cache": {
                    "project_expiry": 5,
                    "project_grace_period": 5,
                    "project_refresh_interval": 7,
                }
            }))
            .unwrap(),
        );

        let fetch = store.try_begin_fetch(project_key).unwrap();
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch).is_none());
        assert_state!(store, project_key, ProjectState::Disabled);

        // Wait for a refresh.
        let Some(Action::Refresh(refresh)) = store.poll().await else {
            panic!();
        };
        assert_eq!(refresh.project_key(), project_key);

        let fetch = store.refresh(refresh).unwrap();
        // Upgrade the pending refresh fetch to a non-refresh fetch.
        assert!(store.try_begin_fetch(project_key).is_none());
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch).is_none());

        // Since the previous refresh has been upgraded to a proper fetch.
        // Expiration has been rescheduled and a new refresh is planned to happen in 7 seconds from
        // now.
        let Some(Action::Refresh(refresh)) = store.poll().await else {
            panic!();
        };
        let fetch = store.refresh(refresh).unwrap();
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch).is_none());

        // At this point the refresh is through, but expiration is around the corner.
        // Because the refresh doesn't bump the expiration deadline.
        let Some(Action::Eviction(eviction)) = store.poll().await else {
            panic!();
        };
        assert_eq!(eviction.project_key(), project_key);
    }

    #[tokio::test(start_paused = true)]
    async fn test_store_refresh_overtaken_by_eviction() {
        let project_key = ProjectKey::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut store = ProjectStore::new(
            &relay_config::Config::from_json_value(serde_json::json!({
                "cache": {
                    "project_expiry": 5,
                    "project_grace_period": 5,
                    "project_refresh_interval": 7,
                }
            }))
            .unwrap(),
        );

        let fetch = store.try_begin_fetch(project_key).unwrap();
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch).is_none());
        assert_state!(store, project_key, ProjectState::Disabled);

        // Move way past the expiration time.
        tokio::time::advance(Duration::from_secs(20)).await;

        // The eviction should be prioritized, there is no reason to refresh an already evicted
        // project.
        let Some(Action::Eviction(eviction)) = store.poll().await else {
            panic!();
        };
        assert_eq!(eviction.project_key(), project_key);
        store.evict(eviction);

        // Make sure there is not another refresh queued.
        // This would not technically be necessary because refresh code must be able to handle
        // refreshes for non-fetched projects, but the current implementation should enforce this.
        assert!(
            tokio::time::timeout(Duration::from_secs(60), store.poll())
                .await
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_store_refresh_during_eviction() {
        let project_key = ProjectKey::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut store = ProjectStore::new(
            &relay_config::Config::from_json_value(serde_json::json!({
                "cache": {
                    "project_expiry": 5,
                    "project_grace_period": 5,
                    "project_refresh_interval": 7,
                }
            }))
            .unwrap(),
        );

        let fetch = store.try_begin_fetch(project_key).unwrap();
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch).is_none());
        assert_state!(store, project_key, ProjectState::Disabled);

        // Move way past the expiration time.
        tokio::time::advance(Duration::from_secs(20)).await;

        // Poll both the eviction and refresh token, while a proper implementation should prevent
        // this, it's a good way to test that a refresh for an evicted project does not fetch the
        // project.
        let Some(Action::Eviction(eviction)) = store.poll().await else {
            panic!();
        };
        let Some(Action::Refresh(refresh)) = store.poll().await else {
            panic!();
        };
        assert_eq!(eviction.project_key(), project_key);
        assert_eq!(refresh.project_key(), project_key);
        store.evict(eviction);

        assert!(store.refresh(refresh).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn test_ready_state() {
        let shared = SharedProjectState::default();
        let shared1 = shared.clone();

        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            shared1.set_project_state(ProjectState::Disabled);
        });

        // After five seconds, project state is still pending:
        let result = shared.ready_project(Duration::from_secs(5)).await;
        assert!(result.is_err());

        // After another 10 seconds, the state will have been updated:
        let result = shared.ready_project(Duration::from_secs(10)).await;
        assert!(matches!(
            result.unwrap().project_state(),
            &ProjectState::Disabled
        ));
    }
}
