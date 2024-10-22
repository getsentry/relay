use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::access::Access;
use arc_swap::ArcSwap;
use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_quotas::CachedRateLimits;
use relay_sampling::evaluation::ReservoirCounters;

use crate::services::projects::project::{ProjectState, Revision};
use crate::services::projects::source::SourceProjectState;
use crate::utils::RetryBackoff;

/// The backing storage for a project cache.
///
/// Exposes the only interface to delete from [`Shared`], gurnatueed by
/// requiring exclusive/mutable access to [`ProjectStore`].
///
/// [`Shared`] can be extended through [`Shared::get_or_create`], in which case
/// the private state is missing. Users of [`Shared::get_or_create`] *must* trigger
/// a fetch to create the private state when [`Missing`] is returned.
/// This gurnatuees that eventually the project state is populated, but for a undetermined,
/// time it is possible that shared state exists without the respective private state.
#[derive(Default)]
pub struct ProjectStore {
    /// The shared state, which can be accessed concurrently.
    shared: Arc<Shared>,
    /// The private, mutably exclusve state, used to maintain the project state.
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
        // TODO: what if in the meantime the state expired and was evicted?
        // Should eviction be possible for states that are currently in progress, I don't think so.
        // Maybe need to discard outdated fetches (for evicted projects)?
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

            evicted += 1;

            // TODO: garbage disposal? Do we still need that, we shouldn't have a problem with
            // timings anymore.

            on_evict(project_key);
        }
        drop(shared);

        evicted
    }

    /// Get a reference to the current project or create a new project.
    ///
    /// For internal use only, a created project must always be fetched immeditately.
    fn get_or_create(&mut self, project_key: ProjectKey, config: &Config) -> ProjectRef<'_> {
        #[cfg(debug_assertions)]
        if self.private.contains_key(&project_key) {
            // We have exclusive access to the private part, there are no concurrent deletions
            // hence when if we have a private state there must always be a shared state as well.
            //
            // The opposite is not true, the shared state may have been created concurrently
            // through the shared access.
            debug_assert!(self.shared.projects.pin().contains_key(&project_key));
        }

        let private = self
            .private
            .entry(project_key)
            .or_insert_with(|| PrivateProjectState::new(project_key, config));

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
    projects: papaya::HashMap<ProjectKey, SharedProjectState>,
}

impl Shared {
    /// Returns the existing project state or creates a new one.
    ///
    /// The caller must ensure that the project cache is instructed to
    /// [`super::ProjectCache::Fetch`] the retrieved project.
    pub fn get_or_create(&self, project_key: ProjectKey) -> SharedProject {
        // TODO: do we need to check for expiry here?
        // TODO: if yes, we need to include the timestamp in the shared project state.
        // TODO: grace periods?

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
        self.private.complete_fetch(&fetch);

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
    when: Instant,
    revision: Revision,
}

impl Fetch {
    /// Returns the [`ProjectKey`] of the project to fetch.
    pub fn project_key(&self) -> ProjectKey {
        self.project_key
    }

    /// Returns when the the fetch for the project should be scheduled.
    ///
    /// This can be now (as soon as possible) or a alter point in time, if the project is currently
    /// in a backoff.
    pub fn when(&self) -> Instant {
        self.when
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

    /// Returns a reference to the fetched [`ProjectState`].
    pub fn project_state(&self) -> Option<&ProjectState> {
        match &self.state {
            SourceProjectState::New(state) => Some(state),
            SourceProjectState::NotModified => None,
        }
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
                //
                // TODO: Remove the lock, we already have interior mutability with the `ArcSwap`
                // and the counters themselves can be atomics.
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
    /// There was a successful non-pending fetch,
    NonPending {
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
            FetchState::NonPending { last_fetch } => Some(*last_fetch),
            _ => None,
        }
    }

    fn try_begin_fetch(&mut self, now: Instant, config: &Config) -> Option<Fetch> {
        let when = match &self.state {
            FetchState::InProgress {} => {
                relay_log::trace!(
                    project_key = self.project_key.as_str(),
                    "project fetch skipped, fetch in progress"
                );
                return None;
            }
            FetchState::Pending { next_fetch_attempt } => {
                // Schedule a new fetch, even if there is a backoff, it will just be sleeping for a while.
                next_fetch_attempt.unwrap_or(now)
            }
            FetchState::NonPending { last_fetch } => {
                if last_fetch.check_expiry(now, config).is_updated() {
                    // The current state is up to date, no need to start another fetch.
                    relay_log::trace!(
                        project_key = self.project_key.as_str(),
                        "project fetch skipped, already up to date"
                    );
                    return None;
                }
                now
            }
        };

        // Mark a current fetch in progress.
        self.state = FetchState::InProgress {};

        relay_log::debug!(
            project_key = &self.project_key.as_str(),
            attempts = self.backoff.attempt() + 1,
            "project state fetch scheduled in {:?}",
            when.saturating_duration_since(Instant::now()),
        );

        Some(Fetch {
            project_key: self.project_key,
            when,
            revision: Revision::default(),
        })
    }

    fn complete_fetch(&mut self, fetch: &CompletedFetch) {
        debug_assert!(
            matches!(self.state, FetchState::InProgress),
            "fetch completed while there was no current fetch registered"
        );

        if fetch.is_pending() {
            self.state = FetchState::Pending {
                next_fetch_attempt: Instant::now().checked_add(self.backoff.next_backoff()),
            };
        } else {
            self.backoff.reset();
            self.state = FetchState::NonPending {
                last_fetch: LastFetch(Instant::now()),
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
            Expiry::Updated
        }
    }
}

/// Expiry state of a project.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum Expiry {
    /// The project state is perfectly up to date.
    Updated,
    /// The project state is outdated but events depending on this project state can still be
    /// processed. The state should be refreshed in the background though.
    Stale,
    /// The project state is completely outdated and events need to be buffered up until the new
    /// state has been fetched.
    Expired,
}

impl Expiry {
    /// Returns `true` if the project is uo to date and does not need to be fetched.
    fn is_updated(&self) -> bool {
        matches!(self, Self::Updated)
    }

    /// Returns `true` if the project is expired and can be evicted.
    fn is_expired(&self) -> bool {
        matches!(self, Self::Expired)
    }
}
