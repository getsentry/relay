use std::sync::Arc;
use std::time::Instant;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;

use crate::services::projects::project::ProjectState;
use crate::utils::RetryBackoff;

pub struct Inner {
    shared: Arc<Shared>,
    state: hashbrown::HashMap<ProjectKey, PrivateProjectState>,
}

impl Inner {
    pub fn get_or_create(&mut self, project_key: ProjectKey, config: &Config) -> InnerRef<'_> {
        let private = self
            .state
            .entry(project_key)
            .or_insert_with(|| PrivateProjectState::new(project_key, config));

        let shared = self
            .shared
            .projects
            .pin()
            .get_or_insert_with(project_key, Default::default)
            .clone();

        InnerRef { private, shared }
    }

    pub fn try_create(&mut self, project_key: ProjectKey, config: &Config) -> Option<InnerRef<'_>> {
        let private = match self.state.entry(project_key) {
            hashbrown::hash_map::Entry::Occupied(_) => return None,
            hashbrown::hash_map::Entry::Vacant(v) => {
                v.insert(PrivateProjectState::new(project_key, config))
            }
        };

        let shared = Arc::new(SharedProjectState::default());
        let _previous = self
            .shared
            .projects
            .pin()
            .insert(project_key, Arc::clone(&shared))
            .is_some();

        debug_assert!(
            !_previous,
            "project should not exist in shared state if it did not exist in private state"
        );

        Some(InnerRef { private, shared })
    }

    pub fn try_begin_fetch(&mut self, project_key: ProjectKey, config: &Config) -> Option<Fetch> {
        self.get_or_create(project_key, config)
            .try_begin_fetch(config)
    }

    #[must_use = "an incomplete fetch must be retried"]
    pub fn complete_fetch(&mut self, fetch: CompletedFetch, config: &Config) -> Option<Fetch> {
        // TODO: what if in the meantime the state expired and was evicted?
        // Should eviction be possible for states that are currently in progress, I don't think so.
        debug_assert!(self.shared.projects.pin().get(&fetch.project_key).is_some());
        debug_assert!(self.state.get(&fetch.project_key).is_some());

        let mut project = self.get_or_create(fetch.project_key, config);
        project.complete_fetch(fetch);

        project.try_begin_fetch(config)
    }
}

pub struct InnerRef<'a> {
    shared: Arc<SharedProjectState>,
    private: &'a mut PrivateProjectState,
}

impl InnerRef<'_> {
    pub fn try_begin_fetch(&mut self, config: &Config) -> Option<Fetch> {
        self.private.try_begin_fetch(config)
    }

    fn complete_fetch(&mut self, fetch: CompletedFetch) {
        // TODO: schedule another fetch on pending?
        self.private.complete_fetch(&fetch);
        self.shared.update(fetch.state);
    }
}

pub struct Shared {
    projects: papaya::HashMap<ProjectKey, Arc<SharedProjectState>>,
}

impl Shared {
    pub fn get(&self, project_key: ProjectKey) -> Option<Arc<ProjectState>> {
        self.projects
            .pin()
            .get(&project_key)
            .map(|v| v.state.load_full())
    }
}

#[derive(Debug)]
struct SharedProjectState {
    // TODO: ProjectState does no longer need an internal arc on the project info?
    // Maybe we still want the internal one, to only pass around project infos.
    state: arc_swap::ArcSwap<ProjectState>,
}

impl SharedProjectState {
    fn update(&self, state: ProjectState) {
        self.state.swap(Arc::new(state));
    }
}

impl Default for SharedProjectState {
    fn default() -> Self {
        Self {
            state: arc_swap::ArcSwap::new(Arc::new(ProjectState::Pending)),
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
