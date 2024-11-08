use std::fmt;
use std::sync::Arc;
use tokio::time::Instant;

use arc_swap::ArcSwap;
use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_quotas::CachedRateLimits;
use relay_sampling::evaluation::ReservoirCounters;
use relay_statsd::metric;

use crate::services::projects::project::{ProjectState, Revision};
use crate::services::projects::source::SourceProjectState;
use crate::statsd::{RelayCounters, RelayHistograms};
use crate::utils::RetryBackoff;

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
#[derive(Default)]
pub struct ProjectStore {
    /// The shared state, which can be accessed concurrently.
    shared: Arc<Shared>,
    /// The private, mutably exclusive state, used to maintain the project state.
    private: hashbrown::HashMap<ProjectKey, PrivateProjectState>,
}

impl ProjectStore {
    /// Retrieves a [`Shared`] handle which can be freely shared with multiple consumers.
    pub fn shared(&self) -> Arc<Shared> {
        Arc::clone(&self.shared)
    }

    /// Tries to begin a new fetch for the passed `project_key`.
    ///
    /// Returns `None` if no fetch is necessary or there is already a fetch ongoing.
    /// A returned [`Fetch`] must be scheduled and completed with [`Fetch::complete`] and
    /// [`Self::complete_fetch`].
    pub fn try_begin_fetch(&mut self, project_key: ProjectKey, config: &Config) -> Option<Fetch> {
        self.get_or_create(project_key, config)
            .try_begin_fetch(config)
    }

    /// Completes a [`CompletedFetch`] started with [`Self::try_begin_fetch`].
    ///
    /// Returns a new [`Fetch`] if another fetch must be scheduled. This happens when the fetched
    /// [`ProjectState`] is still pending or already deemed expired.
    #[must_use = "an incomplete fetch must be retried"]
    pub fn complete_fetch(&mut self, fetch: CompletedFetch, config: &Config) -> Option<Fetch> {
        // Eviction is not possible for projects which are currently being fetched.
        // Hence if there was a started fetch, the project state must always exist at this stage.
        debug_assert!(self.shared.projects.pin().get(&fetch.project_key).is_some());
        debug_assert!(self.private.get(&fetch.project_key).is_some());

        let mut project = self.get_or_create(fetch.project_key, config);
        project.complete_fetch(fetch);
        // Schedule another fetch if necessary, usually should only happen if
        // the completed fetch is pending.
        project.try_begin_fetch(config)
    }

    /// Evicts all stale, expired projects from the cache.
    ///
    /// Evicted projects are passed to the `on_evict` callback. Returns the total amount of evicted
    /// projects.
    pub fn evict_stale_projects<F>(&mut self, config: &Config, mut on_evict: F) -> usize
    where
        F: FnMut(ProjectKey),
    {
        let eviction_start = Instant::now();

        let expired = self.private.extract_if(|_, private| {
            // We only evict projects which have fully expired and are not currently being fetched.
            //
            // This relies on the fact that a project should never remain in the `pending` state
            // for long and is either always being fetched or successfully fetched.
            private.last_fetch().map_or(false, |ts| {
                ts.check_expiry(eviction_start, config).is_expired()
            })
        });

        let mut evicted = 0;

        let shared = self.shared.projects.pin();
        for (project_key, _) in expired {
            let _removed = shared.remove(&project_key);
            debug_assert!(
                _removed.is_some(),
                "an expired project must exist in the shared state"
            );
            relay_log::trace!(tags.project_key = project_key.as_str(), "project evicted");

            evicted += 1;

            on_evict(project_key);
        }
        drop(shared);

        metric!(
            histogram(RelayHistograms::ProjectStateCacheSize) = self.shared.projects.len() as u64,
            storage = "shared"
        );
        metric!(
            histogram(RelayHistograms::ProjectStateCacheSize) = self.private.len() as u64,
            storage = "private"
        );
        metric!(counter(RelayCounters::EvictingStaleProjectCaches) += evicted as u64);

        evicted
    }

    /// Get a reference to the current project or create a new project.
    ///
    /// For internal use only, a created project must always be fetched immediately.
    fn get_or_create(&mut self, project_key: ProjectKey, config: &Config) -> ProjectRef<'_> {
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
            .and_modify(|_| {
                metric!(counter(RelayCounters::ProjectCacheHit) += 1);
            })
            .or_insert_with(|| {
                metric!(counter(RelayCounters::ProjectCacheMiss) += 1);
                PrivateProjectState::new(project_key, config)
            });

        let shared = self
            .shared
            .projects
            .pin()
            .get_or_insert_with(project_key, Default::default)
            .clone();

        ProjectRef { private, shared }
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
        // The fast path, we expect the project to exist.
        let projects = self.projects.pin();
        if let Some(project) = projects.get(&project_key) {
            return project.to_shared_project();
        }

        // The slow path, try to attempt to insert, somebody else may have been faster, but that's okay.
        match projects.try_insert(project_key, Default::default()) {
            Ok(inserted) => inserted.to_shared_project(),
            Err(occupied) => occupied.current.to_shared_project(),
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
}

impl ProjectRef<'_> {
    fn try_begin_fetch(&mut self, config: &Config) -> Option<Fetch> {
        let now = Instant::now();
        self.private
            .try_begin_fetch(now, config)
            .map(|fetch| fetch.with_revision(self.shared.revision()))
    }

    fn complete_fetch(&mut self, fetch: CompletedFetch) {
        let now = Instant::now();
        self.private.complete_fetch(&fetch, now);

        // Keep the old state around if the current fetch is pending.
        // It may still be useful to callers.
        match fetch.state {
            SourceProjectState::New(state) if !state.is_pending() => {
                self.shared.set_project_state(state);
            }
            _ => {}
        }
    }
}

/// A [`Fetch`] token.
///
/// When returned it must be executed and completed using [`Self::complete`].
#[must_use = "a fetch must be executed"]
#[derive(Debug)]
pub struct Fetch {
    project_key: ProjectKey,
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
    /// A return value of `None` indicates, the fetch should be scheduled as soon as possible.
    ///
    /// This can be now (as soon as possible) or a later point in time, if the project is currently
    /// in a backoff.
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
        CompletedFetch {
            project_key: self.project_key,
            state,
        }
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
    project_key: ProjectKey,
    state: SourceProjectState,
}

impl CompletedFetch {
    /// Returns the [`ProjectKey`] of the project which was fetched.
    pub fn project_key(&self) -> ProjectKey {
        self.project_key
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
}

/// Current fetch state for a project.
#[derive(Debug)]
enum FetchState {
    /// There is a fetch currently in progress.
    InProgress,
    /// A successful fetch is pending.
    ///
    /// This state is essentially only the initial state, a project
    /// for the most part should always have a fetch in progress or be
    /// in the non-pending state.
    Pending {
        /// Time when the next fetch should be attempted.
        ///
        /// `None` means soon as possible.
        next_fetch_attempt: Option<Instant>,
    },
    /// There was a successful non-pending fetch.
    Complete {
        /// Time when the fetch was completed.
        last_fetch: LastFetch,
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
}

impl PrivateProjectState {
    fn new(project_key: ProjectKey, config: &Config) -> Self {
        Self {
            project_key,
            state: FetchState::Pending {
                next_fetch_attempt: None,
            },
            backoff: RetryBackoff::new(config.http_max_retry_interval()),
        }
    }

    /// Returns the [`LastFetch`] if there is currently no fetch in progress and the project
    /// was fetched successfully before.
    fn last_fetch(&self) -> Option<LastFetch> {
        match &self.state {
            FetchState::Complete { last_fetch } => Some(*last_fetch),
            _ => None,
        }
    }

    fn try_begin_fetch(&mut self, now: Instant, config: &Config) -> Option<Fetch> {
        let when = match &self.state {
            FetchState::InProgress {} => {
                relay_log::trace!(
                    tags.project_key = self.project_key.as_str(),
                    "project fetch skipped, fetch in progress"
                );
                return None;
            }
            FetchState::Pending { next_fetch_attempt } => {
                // Schedule a new fetch, even if there is a backoff, it will just be sleeping for a while.
                *next_fetch_attempt
            }
            FetchState::Complete { last_fetch } => {
                if last_fetch.check_expiry(now, config).is_fresh() {
                    // The current state is up to date, no need to start another fetch.
                    relay_log::trace!(
                        tags.project_key = self.project_key.as_str(),
                        "project fetch skipped, already up to date"
                    );
                    return None;
                }
                None
            }
        };

        // Mark a current fetch in progress.
        self.state = FetchState::InProgress {};

        relay_log::trace!(
            tags.project_key = &self.project_key.as_str(),
            attempts = self.backoff.attempt() + 1,
            "project state fetch scheduled in {:?}",
            when.unwrap_or(now).saturating_duration_since(now),
        );

        Some(Fetch {
            project_key: self.project_key,
            when,
            revision: Revision::default(),
        })
    }

    fn complete_fetch(&mut self, fetch: &CompletedFetch, now: Instant) {
        debug_assert!(
            matches!(self.state, FetchState::InProgress),
            "fetch completed while there was no current fetch registered"
        );

        if fetch.is_pending() {
            let next_backoff = self.backoff.next_backoff();
            let next_fetch_attempt = match next_backoff.is_zero() {
                false => now.checked_add(next_backoff),
                true => None,
            };
            self.state = FetchState::Pending { next_fetch_attempt };
            relay_log::trace!(
                tags.project_key = &self.project_key.as_str(),
                "project state fetch completed but still pending"
            );
        } else {
            relay_log::trace!(
                tags.project_key = &self.project_key.as_str(),
                "project state fetch completed with non-pending config"
            );
            self.backoff.reset();
            self.state = FetchState::Complete {
                last_fetch: LastFetch(now),
            };
        }
    }
}

/// New type containing the last successful fetch time as an [`Instant`].
#[derive(Debug, Copy, Clone)]
struct LastFetch(Instant);

impl LastFetch {
    /// Returns the [`Expiry`] of the last fetch in relation to `now`.
    fn check_expiry(&self, now: Instant, config: &Config) -> Expiry {
        let expiry = config.project_cache_expiry();
        let elapsed = now.saturating_duration_since(self.0);

        if elapsed >= expiry + config.project_grace_period() {
            Expiry::Expired
        } else if elapsed >= expiry {
            Expiry::Stale
        } else {
            Expiry::Fresh
        }
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

    /// Returns `true` if the project is expired and can be evicted.
    fn is_expired(&self) -> bool {
        matches!(self, Self::Expired)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn collect_evicted(store: &mut ProjectStore, config: &Config) -> Vec<ProjectKey> {
        let mut evicted = Vec::new();
        let num_evicted = store.evict_stale_projects(config, |pk| evicted.push(pk));
        assert_eq!(evicted.len(), num_evicted);
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

    #[test]
    fn test_store_fetch() {
        let project_key = ProjectKey::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut store = ProjectStore::default();
        let config = Default::default();

        let fetch = store.try_begin_fetch(project_key, &config).unwrap();
        assert_eq!(fetch.project_key(), project_key);
        assert_eq!(fetch.when(), None);
        assert_eq!(fetch.revision().as_str(), None);
        assert_state!(store, project_key, ProjectState::Pending);

        // Fetch already in progress, nothing to do.
        assert!(store.try_begin_fetch(project_key, &config).is_none());

        // A pending fetch should trigger a new fetch immediately.
        let fetch = fetch.complete(ProjectState::Pending.into());
        let fetch = store.complete_fetch(fetch, &config).unwrap();
        assert_eq!(fetch.project_key(), project_key);
        // First backoff is still immediately.
        assert_eq!(fetch.when(), None);
        assert_eq!(fetch.revision().as_str(), None);
        assert_state!(store, project_key, ProjectState::Pending);

        // Pending again.
        let fetch = fetch.complete(ProjectState::Pending.into());
        let fetch = store.complete_fetch(fetch, &config).unwrap();
        assert_eq!(fetch.project_key(), project_key);
        // This time it needs to be in the future (backoff).
        assert!(fetch.when() > Some(Instant::now()));
        assert_eq!(fetch.revision().as_str(), None);
        assert_state!(store, project_key, ProjectState::Pending);

        // Now complete with disabled.
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch, &config).is_none());
        assert_state!(store, project_key, ProjectState::Disabled);

        // A new fetch is not yet necessary.
        assert!(store.try_begin_fetch(project_key, &config).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn test_store_fetch_pending_does_not_replace_state() {
        let project_key = ProjectKey::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut store = ProjectStore::default();
        let config = Config::from_json_value(serde_json::json!({
            "cache": {
                "project_expiry": 5,
                "project_grace_period": 5,
            }
        }))
        .unwrap();

        let fetch = store.try_begin_fetch(project_key, &config).unwrap();
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch, &config).is_none());
        assert_state!(store, project_key, ProjectState::Disabled);

        tokio::time::advance(Duration::from_secs(6)).await;

        let fetch = store.try_begin_fetch(project_key, &config).unwrap();
        let fetch = fetch.complete(ProjectState::Pending.into());
        // We're returned a new fetch, because the current one completed pending.
        let fetch = store.complete_fetch(fetch, &config).unwrap();
        // The old cached state is still available and not replaced.
        assert_state!(store, project_key, ProjectState::Disabled);

        let fetch = fetch.complete(ProjectState::new_allowed().into());
        assert!(store.complete_fetch(fetch, &config).is_none());
        assert_state!(store, project_key, ProjectState::Enabled(_));
    }

    #[tokio::test(start_paused = true)]
    async fn test_store_evict_projects() {
        let project_key1 = ProjectKey::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let project_key2 = ProjectKey::parse("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        let mut store = ProjectStore::default();
        let config = Config::from_json_value(serde_json::json!({
            "cache": {
                "project_expiry": 5,
                "project_grace_period": 0,
            }
        }))
        .unwrap();

        let fetch = store.try_begin_fetch(project_key1, &config).unwrap();
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch, &config).is_none());

        assert_eq!(collect_evicted(&mut store, &config), Vec::new());
        assert_state!(store, project_key1, ProjectState::Disabled);

        // 3 seconds is not enough to expire any project.
        tokio::time::advance(Duration::from_secs(3)).await;

        assert_eq!(collect_evicted(&mut store, &config), Vec::new());
        assert_state!(store, project_key1, ProjectState::Disabled);

        let fetch = store.try_begin_fetch(project_key2, &config).unwrap();
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch, &config).is_none());

        // A total of 6 seconds should expire the first project.
        tokio::time::advance(Duration::from_secs(3)).await;

        assert_eq!(collect_evicted(&mut store, &config), vec![project_key1]);
        assert_state!(store, project_key1, ProjectState::Pending);
        assert_state!(store, project_key2, ProjectState::Disabled);
    }

    #[tokio::test(start_paused = true)]
    async fn test_store_evict_projects_pending_not_expired() {
        let project_key1 = ProjectKey::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let project_key2 = ProjectKey::parse("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        let mut store = ProjectStore::default();
        let config = Config::from_json_value(serde_json::json!({
            "cache": {
                "project_expiry": 5,
                "project_grace_period": 0,
            }
        }))
        .unwrap();

        let fetch = store.try_begin_fetch(project_key1, &config).unwrap();
        // Create a new project in a pending state, but never fetch it, this should also never expire.
        store.shared().get_or_create(project_key2);

        tokio::time::advance(Duration::from_secs(6)).await;

        // No evictions, project is pending.
        assert_eq!(collect_evicted(&mut store, &config), Vec::new());

        // Complete the project.
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch, &config).is_none());

        // Still should not be evicted, because we do have 5 seconds to expire since completion.
        assert_eq!(collect_evicted(&mut store, &config), Vec::new());
        tokio::time::advance(Duration::from_secs(4)).await;
        assert_eq!(collect_evicted(&mut store, &config), Vec::new());
        assert_state!(store, project_key1, ProjectState::Disabled);

        // Just enough to expire the project.
        tokio::time::advance(Duration::from_millis(1001)).await;
        assert_eq!(collect_evicted(&mut store, &config), vec![project_key1]);
        assert_state!(store, project_key1, ProjectState::Pending);
        assert_state!(store, project_key2, ProjectState::Pending);
    }

    #[tokio::test(start_paused = true)]
    async fn test_store_evict_projects_stale() {
        let project_key = ProjectKey::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut store = ProjectStore::default();
        let config = Config::from_json_value(serde_json::json!({
            "cache": {
                "project_expiry": 5,
                "project_grace_period": 5,
            }
        }))
        .unwrap();

        let fetch = store.try_begin_fetch(project_key, &config).unwrap();
        let fetch = fetch.complete(ProjectState::Disabled.into());
        assert!(store.complete_fetch(fetch, &config).is_none());

        // This is in the grace period, but not yet expired.
        tokio::time::advance(Duration::from_millis(9500)).await;

        assert_eq!(collect_evicted(&mut store, &config), Vec::new());
        assert_state!(store, project_key, ProjectState::Disabled);

        // Now it's expired.
        tokio::time::advance(Duration::from_secs(1)).await;

        assert_eq!(collect_evicted(&mut store, &config), vec![project_key]);
        assert_state!(store, project_key, ProjectState::Pending);
    }
}
