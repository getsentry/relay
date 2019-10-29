use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::iter;
use std::mem;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use actix::fut;
use actix::prelude::*;
use actix_web::{http::Method, ResponseError};
use chrono::{DateTime, Utc};
use failure::Fail;
use futures::{future::Shared, sync::oneshot, Future};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use semaphore_common::{
    metric, Config, LogError, ProjectId, PublicKey, RelayMode, RetryBackoff, Uuid,
};
use semaphore_general::{
    datascrubbing::DataScrubbingConfig, filter::FiltersConfig, pii::PiiConfig,
};

use crate::actors::controller::{Controller, Shutdown, Subscribe, TimeoutError};
use crate::actors::outcome::DiscardReason;
use crate::actors::upstream::{SendQuery, UpstreamQuery, UpstreamRelay};
use crate::extractors::EventMeta;
use crate::utils::{self, ErrorBoundary, One, Response, SyncActorFuture, SyncHandle};

#[derive(Fail, Debug)]
pub enum ProjectError {
    #[fail(display = "failed to fetch project state from upstream")]
    FetchFailed,

    #[fail(display = "could not schedule project fetching")]
    ScheduleFailed(#[cause] MailboxError),

    #[fail(display = "shutdown timer expired")]
    Shutdown,
}

impl ResponseError for ProjectError {}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum RateLimitScope {
    Key(String),
}

impl RateLimitScope {
    fn iter_variants(meta: &EventMeta) -> impl Iterator<Item = RateLimitScope> {
        iter::once(RateLimitScope::Key(meta.auth().public_key().to_owned()))
    }
}

pub struct Project {
    id: ProjectId,
    config: Arc<Config>,
    manager: Addr<ProjectCache>,
    state: Option<Arc<ProjectState>>,
    state_channel: Option<Shared<oneshot::Receiver<Arc<ProjectState>>>>,
    rate_limits: HashMap<RateLimitScope, RetryAfter>,
    is_local: bool,
}

impl Project {
    pub fn new(id: ProjectId, config: Arc<Config>, manager: Addr<ProjectCache>) -> Self {
        Project {
            id,
            config,
            manager,
            state: None,
            state_channel: None,
            rate_limits: HashMap::new(),
            is_local: false,
        }
    }

    pub fn state(&self) -> Option<&ProjectState> {
        self.state.as_ref().map(AsRef::as_ref)
    }

    fn get_or_fetch_state(
        &mut self,
        context: &mut Context<Self>,
    ) -> Response<Arc<ProjectState>, ProjectError> {
        // count number of times we are looking for the project state
        metric!(counter("project_state.get") += 1);
        if let Some(ref state) = self.state {
            // In case the state is fetched from a local file, don't use own caching logic. Rely on
            // `ProjectCache#local_states` for caching.
            if !self.is_local && !state.outdated(self.id, &self.config) {
                return Response::ok(state.clone());
            }
        }

        let channel = match self.state_channel {
            Some(ref channel) => {
                log::debug!("project {} state request amended", self.id);
                channel.clone()
            }
            None => {
                log::debug!("project {} state requested", self.id);
                let channel = self.fetch_state(context);
                self.state_channel = Some(channel.clone());
                channel
            }
        };

        let future = channel
            .map(|shared| (*shared).clone())
            .map_err(|_| ProjectError::FetchFailed);

        Response::r#async(future)
    }

    fn fetch_state(
        &mut self,
        context: &mut Context<Self>,
    ) -> Shared<oneshot::Receiver<Arc<ProjectState>>> {
        let (sender, receiver) = oneshot::channel();
        let id = self.id;

        self.manager
            .send(FetchProjectState { id })
            .into_actor(self)
            .and_then(move |state_result, slf, _ctx| {
                slf.state_channel = None;
                slf.is_local = state_result
                    .as_ref()
                    .map(|resp| resp.is_local)
                    .unwrap_or(false);
                slf.state = state_result.map(|resp| resp.state).ok();

                if let Some(ref state) = slf.state {
                    log::debug!("project {} state updated", id);
                    sender.send(state.clone()).ok();
                }

                fut::ok(())
            })
            .drop_err()
            .spawn(context);

        receiver.shared()
    }
}

impl Actor for Project {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        log::debug!("project {} initialized without state", self.id);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::debug!("project {} removed from cache", self.id);
    }
}

pub struct GetProjectId;

impl Message for GetProjectId {
    type Result = One<ProjectId>;
}

impl Handler<GetProjectId> for Project {
    type Result = One<ProjectId>;

    fn handle(&mut self, _message: GetProjectId, _context: &mut Context<Self>) -> Self::Result {
        metric!(set("unique_projects") = self.id as i64);
        One(self.id)
    }
}

/// A helper enum indicating the public key state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PublicKeyStatus {
    /// The state of the public key is not known.
    ///
    /// This can indicate that the key is not yet known or that the
    /// key just does not exist.  We can not tell these two cases
    /// apart as there is always a lag since the last update from the
    /// upstream server.  As such the project state uses a heuristic
    /// to decide if it should treat a key as not existing or just
    /// not yet known.
    Unknown,
    /// This key is known but was disabled.
    Disabled,
    /// This key is known and is enabled.
    Enabled,
}

/// These are config values that the user can modify in the UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ProjectConfig {
    /// URLs that are permitted for cross original JavaScript requests.
    pub allowed_domains: Vec<String>,
    /// List of relay public keys that are permitted to access this project.
    pub trusted_relays: Vec<PublicKey>,
    /// Configuration for PII stripping.
    pub pii_config: Option<PiiConfig>,
    /// The grouping configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grouping_config: Option<Value>,
    /// Configuration for filter rules.
    #[serde(skip_serializing_if = "FiltersConfig::is_empty")]
    pub filter_settings: FiltersConfig,
    /// Configuration for data scrubbers.
    #[serde(skip_serializing_if = "DataScrubbingConfig::is_disabled")]
    pub datascrubbing_settings: DataScrubbingConfig,
}

impl Default for ProjectConfig {
    fn default() -> Self {
        ProjectConfig {
            allowed_domains: vec!["*".to_string()],
            trusted_relays: vec![],
            pii_config: None,
            grouping_config: None,
            filter_settings: FiltersConfig::default(),
            datascrubbing_settings: DataScrubbingConfig::default(),
        }
    }
}

impl ProjectConfig {
    /// Get all PII configs that should be applied to this project.
    ///
    /// Yields multiple because:
    ///
    /// 1. User will be able to define PII config themselves (in Sentry, `self.pii_config`)
    /// 2. datascrubbing settings (in Sentry, `self.datascrubbing_settings`) are converted in Relay to a PII config.
    pub fn pii_configs(&self) -> impl Iterator<Item = &PiiConfig> {
        self.pii_config
            .as_ref()
            .into_iter()
            .chain(self.datascrubbing_settings.pii_config().into_iter())
    }
}

/// The project state is a cached server state of a project.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectState {
    /// The timestamp of when the state was received.
    #[serde(default = "Utc::now")]
    pub last_fetch: DateTime<Utc>,
    /// The timestamp of when the state was last changed.
    ///
    /// This might be `None` in some rare cases like where states
    /// are faked locally.
    #[serde(default)]
    pub last_change: Option<DateTime<Utc>>,
    /// Indicates that the project is disabled.
    #[serde(default)]
    pub disabled: bool,
    /// A container of known public keys in the project.
    #[serde(default)]
    pub public_keys: Vec<PublicKeyConfig>,
    /// The project's slug if available.
    #[serde(default)]
    pub slug: Option<String>,
    /// The project's current config.
    #[serde(default)]
    pub config: ProjectConfig,
    /// The project state's revision id.
    #[serde(default)]
    pub rev: Option<Uuid>,
    /// The organization id.
    #[serde(default)]
    pub organization_id: Option<u64>,

    /// True if this project state was fetched but incompatible with this Relay.
    #[serde(skip, default)]
    pub invalid: bool,
}

impl ProjectState {
    /// Project state for a missing project.
    pub fn missing() -> Self {
        ProjectState {
            last_fetch: Utc::now(),
            last_change: None,
            disabled: true,
            public_keys: Vec::new(),
            slug: None,
            config: ProjectConfig::default(),
            rev: None,
            organization_id: None,
            invalid: false,
        }
    }

    /// Project state for an unknown but allowed project.
    ///
    /// This state is used for forwarding in Proxy mode.
    pub fn allowed() -> Self {
        let mut state = ProjectState::missing();
        state.disabled = false;
        state
    }

    /// Project state for a deserialization error.
    pub fn err() -> Self {
        let mut state = ProjectState::missing();
        state.invalid = true;
        state
    }

    /// Returns configuration options for a public key.
    pub fn get_public_key_config(&self, public_key: &str) -> Option<&PublicKeyConfig> {
        for key in &self.public_keys {
            if key.public_key == public_key {
                return Some(key);
            }
        }
        None
    }

    /// Returns the current status of a key.
    pub fn get_public_key_status(&self, public_key: &str) -> PublicKeyStatus {
        if let Some(key) = self.get_public_key_config(public_key) {
            if key.is_enabled {
                PublicKeyStatus::Enabled
            } else {
                PublicKeyStatus::Disabled
            }
        } else {
            PublicKeyStatus::Unknown
        }
    }

    /// Returns `true` if the entire project should be considered
    /// disabled (blackholed, deleted etc.).
    pub fn disabled(&self) -> bool {
        self.disabled
    }

    /// Returns `true` if the project state obtained from the upstream could not be parsed. This
    /// results in events being dropped similar to disabled states, but can provide separate
    /// metrics.
    pub fn invalid(&self) -> bool {
        self.invalid
    }

    /// Returns whether this state is outdated and needs to be refetched.
    pub fn outdated(&self, project_id: ProjectId, config: &Config) -> bool {
        let expiry = match self.slug {
            Some(_) => config.project_cache_expiry(),
            None => config.cache_miss_expiry(),
        };

        // Project state updates are aligned to a fixed grid based on the expiry interval. By
        // default, that's a grid of 1 minute intervals for invalid projects, and a grid of 5
        // minutes for existing projects (cache hits). The exception to this is when a project is
        // seen for the first time, where it is fetched immediately.
        let window = expiry.as_secs();

        // To spread out project state updates more evenly, they are shifted deterministically
        // within the grid window. A 5 minute interval results in 300 theoretical slots that can be
        // chosen for each project based on its project id.
        let project_shift = project_id % window;

        // Based on the last fetch, compute the timestamp of the next fetch. The time stamp is
        // shifted by the project shift to move the grid accordingly. Note that if the remainder is
        // zero, the next fetch is one full window ahead to avoid instant reloading.
        let last_fetch = self.last_fetch.timestamp() as u64;
        let remainder = (last_fetch - project_shift) % window;
        let next_fetch = last_fetch + (window - remainder);

        // See the below assertion for constraints on the next fetch time.
        debug_assert!(next_fetch > last_fetch && next_fetch <= last_fetch + window);

        // A project state counts as outdated when the time of the next fetch has passed.
        Utc::now().timestamp() as u64 >= next_fetch
    }

    /// Returns the project config.
    pub fn config(&self) -> &ProjectConfig {
        &self.config
    }

    /// Checks if this origin is allowed for this project.
    fn is_valid_origin(&self, origin: Option<&Url>) -> bool {
        // Generally accept any event without an origin.
        let origin = match origin {
            Some(origin) => origin,
            None => return true,
        };

        // If the list of allowed domains is empty, we accept any origin. Otherwise, we have to
        // match with the whitelist.
        let allowed = &self.config().allowed_domains;
        !allowed.is_empty()
            && allowed
                .iter()
                .any(|x| x.as_str() == "*" || Some(x.as_str()) == origin.host_str())
    }

    /// Determines whether the given event should be accepted or dropped.
    pub fn get_event_action(
        &self,
        project_id: ProjectId,
        meta: &EventMeta,
        config: &Config,
    ) -> EventAction {
        // Try to verify the request origin with the project config.
        if !self.is_valid_origin(meta.origin()) {
            return EventAction::Discard(DiscardReason::ProjectId);
        }

        if self.outdated(project_id, config) {
            // if the state is out of date, we proceed as if it was still up to date. The
            // upstream relay (or sentry) will still filter events.

            // we assume it is unlikely to re-activate a disabled public key.
            // thus we handle events pretending the config is still valid,
            // except queueing events for unknown DSNs as they might have become
            // available in the meanwhile.
            match self.get_public_key_status(meta.auth().public_key()) {
                PublicKeyStatus::Enabled => EventAction::Accept,
                PublicKeyStatus::Disabled => EventAction::Discard(DiscardReason::AuthClient),
                PublicKeyStatus::Unknown => EventAction::Accept,
            }
        } else {
            // if we recorded an invalid project state response from the upstream (i.e. parsing
            // failed), discard the event with a s
            if self.invalid() {
                return EventAction::Discard(DiscardReason::ProjectState);
            }

            // only drop events if we know for sure the project is disabled.
            if self.disabled() {
                return EventAction::Discard(DiscardReason::ProjectId);
            }

            // since the config has been fetched recently, we assume unknown
            // public keys do not exist and drop events eagerly. proxy mode is
            // an exception, where public keys are backfilled lazily after
            // events are sent to the upstream.
            match self.get_public_key_status(meta.auth().public_key()) {
                PublicKeyStatus::Enabled => EventAction::Accept,
                PublicKeyStatus::Disabled => EventAction::Discard(DiscardReason::AuthClient),
                PublicKeyStatus::Unknown => match config.relay_mode() {
                    RelayMode::Proxy => EventAction::Accept,
                    _ => EventAction::Discard(DiscardReason::AuthClient),
                },
            }
        }
    }
}

/// Represents a public key received from the projectconfig endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKeyConfig {
    /// Public part of key (random hash).
    pub public_key: String,
    /// Whether this key can be used.
    pub is_enabled: bool,

    /// The primary key of the DSN in Sentry's main database.
    ///
    /// Only available for internal relays.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numeric_id: Option<u64>,

    /// List of quotas to apply to events that use this key.
    ///
    /// Only available for internal relays.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub quotas: Vec<Quota>,
}

/// Data for applying rate limits in Redis
#[derive(Debug, Clone)]
pub enum Quota {
    RejectAll(RejectAllQuota),
    Redis(RedisQuota),
}

mod __quota_serialization {
    use std::borrow::Cow;

    use super::{Quota, RedisQuota, RejectAllQuota};

    use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize, Default)]
    #[serde(default)]
    struct QuotaSerdeHelper<'a> {
        #[serde(skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        #[serde(borrow, skip_serializing_if = "Option::is_none")]
        reason_code: Option<Cow<'a, str>>,
        #[serde(borrow, skip_serializing_if = "Option::is_none")]
        prefix: Option<Cow<'a, str>>,
        #[serde(borrow, skip_serializing_if = "Option::is_none")]
        subscope: Option<Cow<'a, str>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        window: Option<u64>,
    }

    impl Serialize for Quota {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match *self {
                Quota::RejectAll(RejectAllQuota { ref reason_code }) => QuotaSerdeHelper {
                    limit: Some(0),
                    reason_code: reason_code.as_ref().map(String::as_str).map(Cow::Borrowed),
                    ..Default::default()
                },
                Quota::Redis(RedisQuota {
                    limit,
                    ref reason_code,
                    ref prefix,
                    ref subscope,
                    window,
                }) => QuotaSerdeHelper {
                    limit,
                    reason_code: reason_code.as_ref().map(String::as_str).map(Cow::Borrowed),
                    prefix: Some(Cow::Borrowed(&prefix)),
                    subscope: subscope.as_ref().map(String::as_str).map(Cow::Borrowed),
                    window: Some(window),
                },
            }
            .serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for Quota {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let helper = QuotaSerdeHelper::deserialize(deserializer)?;

            match (
                helper.limit,
                helper.reason_code,
                helper.prefix,
                helper.subscope,
                helper.window,
            ) {
                (Some(0), reason_code, None, None, None) => Ok(Quota::RejectAll(RejectAllQuota {
                    reason_code: reason_code.map(Cow::into_owned),
                })),
                (limit, reason_code, Some(prefix), subscope, Some(window)) => {
                    Ok(Quota::Redis(RedisQuota {
                        limit,
                        reason_code: reason_code.map(Cow::into_owned),
                        prefix: prefix.into_owned(),
                        subscope: subscope.map(Cow::into_owned),
                        window,
                    }))
                }
                _ => Err(D::Error::custom(
                    "Could not deserialize Quota, no variant matched",
                )),
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisQuota {
    /// How many events should be accepted within the window.
    ///
    /// "We should accept <limit> events per <window> seconds"
    pub limit: Option<u64>,

    /// Some string identifier that will be part of the 429 Rate Limit Exceeded response if it
    /// comes to that.
    pub reason_code: Option<String>,

    /// Type of quota.
    ///
    /// E.g. `k` for key quotas, or `p` for project quotas. This is used when creating the Redis
    /// key where the counters and refunds are stored.
    pub prefix: String,

    /// Usually a project/key/organization ID, depending on type of quota.
    pub subscope: Option<String>,

    /// Size of the timewindow we look at (seconds).
    ///
    /// "We should accept <limit> events per <window> seconds"
    pub window: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RejectAllQuota {
    /// Some string identifier that will be part of the 429 Rate Limit Exceeded response if it
    /// comes to that.
    pub reason_code: Option<String>,
}

pub struct GetProjectState;

impl Message for GetProjectState {
    type Result = Result<Arc<ProjectState>, ProjectError>;
}

impl Handler<GetProjectState> for Project {
    type Result = Response<Arc<ProjectState>, ProjectError>;

    fn handle(&mut self, _message: GetProjectState, context: &mut Context<Self>) -> Self::Result {
        self.get_or_fetch_state(context)
    }
}

pub struct GetEventAction {
    meta: Arc<EventMeta>,
    fetch: bool,
}

impl GetEventAction {
    pub fn fetched(meta: Arc<EventMeta>) -> Self {
        GetEventAction { meta, fetch: true }
    }

    pub fn cached(meta: Arc<EventMeta>) -> Self {
        GetEventAction { meta, fetch: false }
    }
}

/// Indicates what should happen to events based on their meta data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventAction {
    /// The event should be discarded.
    Discard(DiscardReason),
    /// The event should be discarded and the client should back off for some time.
    RetryAfter(RateLimit),
    /// The event should be processed and sent to upstream.
    Accept,
}

impl Message for GetEventAction {
    type Result = Result<EventAction, ProjectError>;
}

impl Handler<GetEventAction> for Project {
    type Result = Response<EventAction, ProjectError>;

    fn handle(&mut self, message: GetEventAction, context: &mut Self::Context) -> Self::Result {
        let project_id = self.id;

        // Check for an eventual rate limit. Note that we need to check for all variants of
        // RateLimitScope, otherwise we might miss a rate limit.
        let rate_limit = RateLimitScope::iter_variants(&message.meta)
            .filter_map(|scope| {
                if let Entry::Occupied(entry) = self.rate_limits.entry(scope.clone()) {
                    if entry.get().can_retry() {
                        entry.remove_entry();
                        None
                    } else {
                        Some(RateLimit(scope, entry.get().clone()))
                    }
                } else {
                    None
                }
            })
            .max_by_key(|rate_limit| rate_limit.remaining_seconds());

        if let Some(rate_limit) = rate_limit {
            Response::ok(EventAction::RetryAfter(rate_limit))
        } else if message.fetch {
            // Project state fetching is allowed, so ensure the state is fetched and up-to-date.
            // This will return synchronously if the state is still cached.
            let config = self.config.clone();
            self.get_or_fetch_state(context)
                .map(move |state| state.get_event_action(project_id, &message.meta, &config))
        } else {
            // Fetching is not permitted (as part of the store request). In case the state is not
            // cached, assume that the event can be accepted. The EventManager will later fetch the
            // project state and reevaluate the event action.
            Response::ok(self.state().map_or(EventAction::Accept, |state| {
                state.get_event_action(project_id, &message.meta, &self.config)
            }))
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RetryAfter {
    pub when: Instant,
    pub reason_code: Option<String>,
}

impl RetryAfter {
    pub fn remaining_seconds(&self) -> u64 {
        let now = Instant::now();
        if now > self.when {
            return 0;
        }

        // Compensate for the missing subsec part by adding 1s
        (self.when - now).as_secs() + 1
    }

    pub fn can_retry(&self) -> bool {
        self.remaining_seconds() == 0
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RateLimit(pub RateLimitScope, pub RetryAfter);

impl RateLimit {
    #[cfg(feature = "processing")]
    pub fn reason_code(&self) -> Option<&str> {
        Some(&self.1.reason_code.as_ref()?)
    }

    pub fn remaining_seconds(&self) -> u64 {
        self.1.remaining_seconds()
    }
}

impl Message for RateLimit {
    type Result = ();
}

impl Handler<RateLimit> for Project {
    type Result = ();

    fn handle(&mut self, message: RateLimit, _context: &mut Self::Context) -> Self::Result {
        let RateLimit(scope, value) = message;

        if let Some(ref old_value) = self.rate_limits.get(&scope) {
            if old_value.when > value.when {
                return;
            }
        }

        self.rate_limits.insert(scope, value);
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetProjectStates {
    pub projects: Vec<ProjectId>,
    #[cfg(feature = "processing")]
    #[serde(default)]
    pub full_config: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetProjectStatesResponse {
    pub configs: HashMap<ProjectId, ErrorBoundary<Option<ProjectState>>>,
}

impl UpstreamQuery for GetProjectStates {
    type Response = GetProjectStatesResponse;

    fn method(&self) -> Method {
        Method::POST
    }

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/api/0/relays/projectconfigs/")
    }
}

pub struct ProjectCache {
    backoff: RetryBackoff,
    config: Arc<Config>,
    upstream: Addr<UpstreamRelay>,
    projects: HashMap<ProjectId, Addr<Project>>,
    local_states: HashMap<ProjectId, Arc<ProjectState>>,
    state_channels: HashMap<ProjectId, oneshot::Sender<ProjectState>>,
    shutdown: SyncHandle,
}

impl ProjectCache {
    pub fn new(config: Arc<Config>, upstream: Addr<UpstreamRelay>) -> Self {
        ProjectCache {
            backoff: RetryBackoff::from_config(&config),
            config,
            upstream,
            projects: HashMap::new(),
            local_states: HashMap::new(),
            state_channels: HashMap::new(),
            shutdown: SyncHandle::new(),
        }
    }

    /// Returns the backoff timeout for a batched upstream query.
    ///
    /// If previous queries succeeded, this will be the general batch interval. Additionally, an
    /// exponentially increasing backoff is used for retrying the upstream request.
    fn next_backoff(&mut self) -> Duration {
        self.config.query_batch_interval() + self.backoff.next_backoff()
    }

    /// Schedules a batched upstream query with exponential backoff.
    fn schedule_fetch(&mut self, context: &mut Context<Self>) {
        utils::run_later(self.next_backoff(), Self::fetch_states)
            .sync(&self.shutdown, ())
            .spawn(context)
    }

    /// Executes an upstream request to fetch project configs.
    ///
    /// This assumes that currently no request is running. If the upstream request fails or new
    /// channels are pushed in the meanwhile, this will reschedule automatically.
    fn fetch_states(&mut self, context: &mut Context<Self>) {
        if self.state_channels.is_empty() {
            log::error!("project state update scheduled without projects");
            return;
        }

        let channels = mem::replace(&mut self.state_channels, HashMap::new());
        log::debug!(
            "updating project states for {} projects (attempt {})",
            channels.len(),
            self.backoff.attempt(),
        );

        let request = GetProjectStates {
            projects: channels.keys().cloned().collect(),
            #[cfg(feature = "processing")]
            full_config: self.config.processing_enabled(),
        };
        // count number of http requests for project states
        metric!(counter("project_state.request") += 1);
        self.upstream
            .send(SendQuery(request))
            .map_err(ProjectError::ScheduleFailed)
            .into_actor(self)
            .and_then(|response, slf, ctx| {
                match response {
                    Ok(mut response) => {
                        slf.backoff.reset();

                        // count number of project states returned (via http requests)
                        metric!(counter("project_state.received") += channels.len() as i64);
                        for (id, channel) in channels {
                            let state = response
                                .configs
                                .remove(&id)
                                .unwrap_or(ErrorBoundary::Ok(None))
                                .unwrap_or_else(|| Some(ProjectState::err()))
                                .unwrap_or_else(ProjectState::missing);

                            channel.send(state).ok();
                        }
                    }
                    Err(error) => {
                        log::error!("error fetching project states: {}", LogError(&error));

                        if !slf.shutdown.requested() {
                            // Put the channels back into the queue, in addition to channels that
                            // have been pushed in the meanwhile. We will retry again shortly.
                            slf.state_channels.extend(channels);
                        }
                    }
                }

                if !slf.state_channels.is_empty() {
                    slf.schedule_fetch(ctx);
                }

                fut::ok(())
            })
            .sync(&self.shutdown, ProjectError::Shutdown)
            .drop_err()
            .spawn(context);
    }
}

fn load_local_states(projects_path: &Path) -> io::Result<HashMap<ProjectId, Arc<ProjectState>>> {
    let mut states = HashMap::new();

    let directory = match fs::read_dir(projects_path) {
        Ok(directory) => directory,
        Err(error) => {
            return match error.kind() {
                io::ErrorKind::NotFound => Ok(states),
                _ => Err(error),
            };
        }
    };

    // only printed when directory even exists.
    log::debug!("Loading local states from directory {:?}", projects_path);

    for entry in directory {
        let entry = entry?;
        let path = entry.path();

        if !entry.metadata()?.is_file() {
            log::warn!("skipping {:?}, not a file", path);
            continue;
        }

        if path.extension().map(|x| x != "json").unwrap_or(true) {
            log::warn!("skipping {:?}, file extension must be .json", path);
            continue;
        }

        let id = match path
            .file_stem()
            .and_then(OsStr::to_str)
            .and_then(|stem| stem.parse().ok())
        {
            Some(id) => id,
            None => {
                log::warn!("skipping {:?}, filename is not a valid project id", path);
                continue;
            }
        };

        let state = serde_json::from_reader(io::BufReader::new(fs::File::open(path)?))?;
        states.insert(id, Arc::new(state));
    }

    Ok(states)
}

fn poll_local_states(
    manager: Addr<ProjectCache>,
    config: Arc<Config>,
) -> impl Future<Item = (), Error = ()> {
    let (sender, receiver) = oneshot::channel();

    let _ = thread::spawn(move || {
        let path = config.project_configs_path();
        let mut sender = Some(sender);

        loop {
            match load_local_states(&path) {
                Ok(states) => {
                    manager.do_send(UpdateLocalStates { states });
                    sender.take().map(|sender| sender.send(()).ok());
                }
                Err(error) => log::error!(
                    "failed to load static project configs: {}",
                    LogError(&error)
                ),
            }

            thread::sleep(config.local_cache_interval());
        }
    });

    receiver.map_err(|_| ())
}

impl Actor for ProjectCache {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        log::info!("project cache started");
        Controller::from_registry().do_send(Subscribe(context.address().recipient()));

        // Start the background thread that reads the local states from disk.
        // `poll_local_states` returns a future that resolves as soon as the first read is done.
        poll_local_states(context.address(), self.config.clone())
            .into_actor(self)
            // Block entire actor on first local state read, such that we don't e.g. drop events on
            // startup
            .wait(context);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::info!("project cache stopped");
    }
}

struct UpdateLocalStates {
    states: HashMap<ProjectId, Arc<ProjectState>>,
}

impl Message for UpdateLocalStates {
    type Result = ();
}

impl Handler<UpdateLocalStates> for ProjectCache {
    type Result = ();

    fn handle(&mut self, message: UpdateLocalStates, _context: &mut Context<Self>) -> Self::Result {
        self.local_states = message.states;
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetProject {
    pub id: ProjectId,
}

impl Message for GetProject {
    type Result = Addr<Project>;
}

impl Handler<GetProject> for ProjectCache {
    type Result = Addr<Project>;

    fn handle(&mut self, message: GetProject, context: &mut Context<Self>) -> Self::Result {
        let config = self.config.clone();
        metric!(histogram("project_cache.size") = self.projects.len() as u64);
        match self.projects.entry(message.id) {
            Entry::Occupied(value) => {
                metric!(counter("project_cache.hit") += 1);
                value.get().clone()
            }
            Entry::Vacant(_) => {
                metric!(counter("project_cache.miss") += 1);
                let new_val = Project::new(message.id, config, context.address()).start();
                let ret_val = new_val.clone();
                self.projects.insert(message.id, new_val);
                ret_val
            }
        }
    }
}

struct FetchProjectState {
    id: ProjectId,
}

struct ProjectStateResponse {
    state: Arc<ProjectState>,
    is_local: bool,
}

impl ProjectStateResponse {
    pub fn managed(state: ProjectState) -> Self {
        ProjectStateResponse {
            state: Arc::new(state),
            is_local: false,
        }
    }

    pub fn local(state: ProjectState) -> Self {
        ProjectStateResponse {
            state: Arc::new(state),
            is_local: true,
        }
    }
}

impl Message for FetchProjectState {
    type Result = Result<ProjectStateResponse, ()>;
}

impl Handler<FetchProjectState> for ProjectCache {
    type Result = Response<ProjectStateResponse, ()>;

    fn handle(&mut self, message: FetchProjectState, context: &mut Self::Context) -> Self::Result {
        if let Some(state) = self.local_states.get(&message.id) {
            return Response::ok(ProjectStateResponse {
                state: state.clone(),
                is_local: true,
            });
        }

        match self.config.relay_mode() {
            RelayMode::Proxy => {
                return Response::ok(ProjectStateResponse::local(ProjectState::allowed()));
            }
            RelayMode::Static => {
                return Response::ok(ProjectStateResponse::local(ProjectState::missing()));
            }
            RelayMode::Capture => {
                return Response::ok(ProjectStateResponse::local(ProjectState::allowed()));
            }
            RelayMode::Managed => {
                // Proceed with loading the config from upstream
            }
        }

        if !self.backoff.started() {
            self.backoff.reset();
            self.schedule_fetch(context);
        }

        let (sender, receiver) = oneshot::channel();
        if self.state_channels.insert(message.id, sender).is_some() {
            log::error!("project {} state fetched multiple times", message.id);
        }

        Response::r#async(receiver.map(ProjectStateResponse::managed).map_err(|_| ()))
    }
}

impl Handler<Shutdown> for ProjectCache {
    type Result = ResponseFuture<(), TimeoutError>;

    fn handle(&mut self, message: Shutdown, _context: &mut Self::Context) -> Self::Result {
        match message.timeout {
            Some(timeout) => self.shutdown.timeout(timeout),
            None => self.shutdown.now(),
        }
    }
}
