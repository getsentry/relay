use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_quotas::{CachedRateLimits, RateLimits};
use relay_sampling::evaluation::ReservoirCounters;

use crate::services::projects::cache2::ProjectCache;
use crate::services::projects::project::ProjectState;
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
pub struct ProjectStore {
    shared: Arc<Shared>,
    private: hashbrown::HashMap<ProjectKey, PrivateProjectState>,
}

impl ProjectStore {
    pub fn get(&mut self, project_key: ProjectKey) -> Option<ProjectRef<'_>> {
        let private = self.private.get_mut(&project_key)?;
        let shared = self.shared.projects.pin().get(&project_key).cloned();
        debug_assert!(
            shared.is_some(),
            "there must be a shared project if private state exists"
        );

        Some(ProjectRef {
            private,
            shared: shared?,
        })
    }

    pub fn try_begin_fetch(&mut self, project_key: ProjectKey, config: &Config) -> Option<Fetch> {
        self.get_or_create(project_key, config)
            .try_begin_fetch(config)
    }

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

    pub fn evict_stale_projects<F>(&mut self, config: &Config, mut on_evict: F) -> usize
    where
        F: FnMut(ProjectKey),
    {
        let eviction_start = Instant::now();
        let delta = 2 * config.project_cache_expiry() + config.project_grace_period();

        // TODO: what do we do with forever fetching projects, do we fail eventually?
        let expired = self.private.extract_if(|_, private| {
            if private.has_fetch_in_progress() {
                return false;
            }

            // Invariant: if there is no last successful fetch,
            // there must be a fetch currently in progress.
            debug_assert!(private.last_non_pending_fetch.is_some());

            private
                .last_non_pending_fetch
                .map_or(true, |ts| ts + delta <= eviction_start)
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

    fn get_or_create(&mut self, project_key: ProjectKey, config: &Config) -> ProjectRef<'_> {
        #[cfg(debug_assertions)]
        if self.private.contains_key(&project_key) {
            // We have exclusive access to the private part, there are no concurrent deletions
            // hence when if we have a private state there must always be a shared state as well.
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

pub struct ProjectRef<'a> {
    shared: SharedProjectState,
    private: &'a mut PrivateProjectState,
}

impl ProjectRef<'_> {
    pub fn merge_rate_limits(&mut self, rate_limits: RateLimits) {
        self.shared.merge_rate_limits(rate_limits)
    }

    fn try_begin_fetch(&mut self, config: &Config) -> Option<Fetch> {
        self.private.try_begin_fetch(config)
    }

    fn complete_fetch(&mut self, fetch: CompletedFetch) {
        self.private.complete_fetch(&fetch);

        // Keep the old state around if the current fetch is pending.
        // It may still be useful to callers.
        if !fetch.project_state.is_pending() {
            self.shared.set_project_state(fetch.project_state);
        }
    }
}

pub struct Shared {
    projects: papaya::HashMap<ProjectKey, SharedProjectState>,
}

impl Shared {
    // pub fn get(&self, project_key: ProjectKey) -> Option<ProjectState> {
    //     self.projects
    //         .pin()
    //         .get(&project_key)
    //         .map(|v| v.state.load().as_ref().clone())
    // }

    /// Returns the existing project state or creates a new one and returns `Err([`Missing`])`.
    ///
    /// The returned [`Missing`] value must be used to trigger a fetch for this project
    /// or it will stay pending forever.
    pub fn get_or_create(&self, project_key: ProjectKey) -> Result<SharedProject, Missing> {
        // TODO: do we need to check for expiry here?
        // TODO: if yes, we need to include the timestamp in the shared project state.
        // TODO: grace periods?

        // The fast path, we expect the project to exist.
        let projects = self.projects.pin();
        if let Some(project) = projects.get(&project_key) {
            return Ok(project.to_shared_project());
        }

        // The slow path, try to attempt to insert, somebody else may have been faster, but that's okay.
        match projects.try_insert(project_key, Default::default()) {
            Ok(inserted) => Err(Missing {
                project_key,
                shared_project: inserted.to_shared_project(),
            }),
            Err(occupied) => Ok(occupied.current.to_shared_project()),
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

#[must_use = "a missing project must be fetched"]
pub struct Missing {
    project_key: ProjectKey,
    shared_project: SharedProject,
}

impl Missing {
    pub fn fetch(self, project_cache: &relay_system::Addr<ProjectCache>) -> SharedProject {
        project_cache.send(ProjectCache::Fetch(self.project_key));
        self.shared_project
    }
}

pub struct SharedProject(Arc<SharedProjectStateInner>);

impl SharedProject {
    pub fn project_state(&self) -> &ProjectState {
        &self.0.state
    }

    pub fn cached_rate_limits(&self) -> &CachedRateLimits {
        // TODO: exposing cached rate limits may be a bad idea, this allows mutation
        // and caching of rate limits for pending projects, which may or may not be fine.
        //
        // Read only access is easily achievable if we return only the current rate limits.
        &self.0.rate_limits
    }

    pub fn reservoir_counters(&self) -> &ReservoirCounters {
        &self.0.reservoir_counters
    }
}

#[derive(Debug, Default, Clone)]
struct SharedProjectState(Arc<ArcSwap<SharedProjectStateInner>>);

impl SharedProjectState {
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

    fn merge_rate_limits(&self, rate_limits: RateLimits) {
        self.0.load().rate_limits.merge(rate_limits)
    }

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

struct PrivateProjectState {
    project_key: ProjectKey,
    current_fetch: Option<()>,

    last_non_pending_fetch: Option<Instant>,

    next_fetch_attempt: Option<Instant>,
    backoff: RetryBackoff,
}

impl PrivateProjectState {
    fn new(project_key: ProjectKey, config: &Config) -> Self {
        Self {
            project_key,
            current_fetch: None,
            last_non_pending_fetch: None,
            next_fetch_attempt: None,
            backoff: RetryBackoff::new(config.http_max_retry_interval()),
        }
    }

    fn has_fetch_in_progress(&self) -> bool {
        self.current_fetch.is_some()
    }

    fn try_begin_fetch(&mut self, config: &Config) -> Option<Fetch> {
        if self.current_fetch.is_some() {
            // Already a fetch in progress.
            relay_log::trace!(
                project_key = self.project_key.as_str(),
                "project fetch skipped, fetch in progress"
            );
            return None;
        }

        if matches!(self.check_expiry(config), Expiry::Updated) {
            // The current state is up to date, no need to start another fetch.
            relay_log::trace!(
                project_key = self.project_key.as_str(),
                "project fetch skipped, already up to date"
            );
            return None;
        }

        // Mark a current fetch in progress.
        self.current_fetch.insert(());

        // Schedule a new fetch, even if there is a backoff, it will just be sleeping for a while.
        let when = self.next_fetch_attempt.take().unwrap_or_else(Instant::now);

        relay_log::debug!(
            project_key = &self.project_key.as_str(),
            attempts = self.backoff.attempt() + 1,
            "project state fetch scheduled in {:?}",
            when.saturating_duration_since(Instant::now()),
        );

        Some(Fetch {
            project_key: self.project_key,
            when,
        })
    }

    fn complete_fetch(&mut self, fetch: &CompletedFetch) {
        if fetch.project_state.is_pending() {
            self.next_fetch_attempt = Instant::now().checked_add(self.backoff.next_backoff());
        } else {
            debug_assert!(
                self.next_fetch_attempt.is_none(),
                "the scheduled fetch should have cleared the next attempt"
            );
            self.next_fetch_attempt = None;
            self.backoff.reset();
            self.last_non_pending_fetch = Some(Instant::now());
        }

        let _current_fetch = self.current_fetch.take();
        debug_assert!(
            _current_fetch.is_some(),
            "fetch completed while there was no current fetch registered"
        );
    }

    fn check_expiry(&self, config: &Config) -> Expiry {
        let Some(last_fetch) = self.last_non_pending_fetch else {
            return Expiry::Expired;
        };

        let expiry = config.project_cache_expiry();
        let elapsed = last_fetch.elapsed();

        if elapsed >= expiry + config.project_grace_period() {
            Expiry::Expired
        } else if elapsed >= expiry {
            Expiry::Stale
        } else {
            Expiry::Updated
        }
    }
}

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

#[derive(Debug)]
pub struct Fetch {
    project_key: ProjectKey,
    when: Instant,
}

impl Fetch {
    pub fn project_key(&self) -> ProjectKey {
        self.project_key
    }

    pub fn when(&self) -> Instant {
        self.when
    }

    pub fn complete(self, state: ProjectState) -> CompletedFetch {
        CompletedFetch {
            project_key: self.project_key,
            project_state: state,
        }
    }
}

pub struct CompletedFetch {
    project_key: ProjectKey,
    project_state: ProjectState,
}

impl CompletedFetch {
    pub fn project_key(&self) -> ProjectKey {
        self.project_key
    }

    pub fn project_state(&self) -> &ProjectState {
        &self.project_state
    }
}
