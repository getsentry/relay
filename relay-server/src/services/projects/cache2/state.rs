use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_quotas::{CachedRateLimits, RateLimits};

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
    pub fn get_or_create(&mut self, project_key: ProjectKey, config: &Config) -> ProjectRef<'_> {
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

    // pub fn try_create(&mut self, project_key: ProjectKey, config: &Config) -> Option<ProjectRef<'_>> {
    //     let private = match self.state.entry(project_key) {
    //         hashbrown::hash_map::Entry::Occupied(_) => return None,
    //         hashbrown::hash_map::Entry::Vacant(v) => {
    //             v.insert(PrivateProjectState::new(project_key, config))
    //         }
    //     };
    //
    //     let shared = Arc::new(SharedProjectState::default());
    //     let _previous = self
    //         .shared
    //         .projects
    //         .pin()
    //         .insert(project_key, Arc::clone(&shared))
    //         .is_some();
    //
    //     debug_assert!(
    //         !_previous,
    //         "project should not exist in shared state if it did not exist in private state"
    //     );
    //
    //     Some(ProjectRef { private, shared })
    // }

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
}

pub struct ProjectRef<'a> {
    shared: SharedProjectState,
    private: &'a mut PrivateProjectState,
}

impl ProjectRef<'_> {
    pub fn try_begin_fetch(&mut self, config: &Config) -> Option<Fetch> {
        self.private.try_begin_fetch(config)
    }

    fn complete_fetch(&mut self, fetch: CompletedFetch) {
        self.private.complete_fetch(&fetch);

        // Keep the old state around if the current fetch is pending.
        // It may still be useful to callers.
        if !fetch.state.is_pending() {
            self.shared.set_project_state(fetch.state);
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

#[must_use = "a missing value must be used to trigger a fetch"]
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
        &self.0.as_ref().state
    }

    pub fn current_rate_limits(&self) -> RateLimits {
        let rate_limits = self.0.as_ref().rate_limits.clone(); // TODO
        rate_limits.current()
    }
}

#[derive(Debug, Default, Clone)]
struct SharedProjectState(Arc<ArcSwap<SharedProjectStateInner>>);

impl SharedProjectState {
    fn set_project_state(&self, state: ProjectState) {
        self.0.rcu(move |stored| SharedProjectStateInner {
            state: state.clone(),
            rate_limits: stored.rate_limits.clone(),
        });
    }

    fn to_shared_project(&self) -> SharedProject {
        SharedProject(self.0.as_ref().load_full())
    }
}

/// The data contained in a [`SharedProjectState`].
///
/// All fields must be cheap to clone and are ideally just a single `Arc`.
/// Partial updates to [`SharedProjectState`], are performed using `rcu` cloning all fields.
#[derive(Debug)]
struct SharedProjectStateInner {
    state: ProjectState,
    // TODO: these should possibly have their own inner ArcSwap to make dropping expired limits
    // possible without mutable access.
    rate_limits: CachedRateLimits,
}

impl Default for SharedProjectStateInner {
    fn default() -> Self {
        Self {
            state: ProjectState::Pending,
            rate_limits: Default::default(),
        }
    }
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
        if fetch.state.is_pending() {
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
            state,
        }
    }
}

pub struct CompletedFetch {
    project_key: ProjectKey,
    state: ProjectState,
}

impl CompletedFetch {
    fn project_key(&self) -> ProjectKey {
        self.project_key
    }
}
