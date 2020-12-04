use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use chrono::{DateTime, Utc};
use futures::{future::Shared, sync::oneshot, Future};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smallvec::SmallVec;
use url::Url;

use relay_auth::PublicKey;
use relay_common::{metric, ProjectId, ProjectKey};
use relay_config::Config;
use relay_filter::{matches_any_origin, FiltersConfig};
use relay_general::pii::{DataScrubbingConfig, PiiConfig};
use relay_quotas::{Quota, RateLimits, Scoping};

use crate::actors::outcome::DiscardReason;
use crate::actors::project_cache::{FetchProjectState, ProjectCache, ProjectError};
use crate::envelope::Envelope;
use crate::extractors::RequestMeta;
use crate::metrics::RelayCounters;
use crate::utils::{ActorResponse, EnvelopeLimiter, Response, SamplingConfig};

/// The current status of a project state. Return value of `ProjectState::outdated`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Outdated {
    /// The project state is perfectly up to date.
    Updated,
    /// The project state is outdated but events depending on this project state can still be
    /// processed. The state should be refreshed in the background though.
    SoftOutdated,
    /// The project state is completely outdated and events need to be buffered up until the new
    /// state has been fetched.
    HardOutdated,
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
    /// Maximum event retention for the organization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_retention: Option<u16>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub quotas: Vec<Quota>,
    /// Configuration for sampling traces, if not present there will be no sampling.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampling: Option<SamplingConfig>,
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
            event_retention: None,
            quotas: Vec::new(),
            sampling: None,
        }
    }
}

/// These are config values that the user can modify in the UI.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase", remote = "ProjectConfig")]
pub struct LimitedProjectConfig {
    pub allowed_domains: Vec<String>,
    pub trusted_relays: Vec<PublicKey>,
    pub pii_config: Option<PiiConfig>,
    pub datascrubbing_settings: DataScrubbingConfig,
}

/// The project state is a cached server state of a project.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectState {
    /// Unique identifier of this project.
    pub project_id: Option<ProjectId>,
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
    ///
    /// Since version 2, each project state corresponds to a single public key. For this reason,
    /// only a single key can occur in this list.
    #[serde(default)]
    pub public_keys: SmallVec<[PublicKeyConfig; 1]>,
    /// The project's slug if available.
    #[serde(default)]
    pub slug: Option<String>,
    /// The project's current config.
    #[serde(default)]
    pub config: ProjectConfig,
    /// The organization id.
    #[serde(default)]
    pub organization_id: Option<u64>,

    /// The time at which this project state was last updated.
    #[serde(skip, default = "Instant::now")]
    pub last_fetch: Instant,

    /// True if this project state was fetched but incompatible with this Relay.
    #[serde(skip, default)]
    pub invalid: bool,
}

/// Controls how we serialize a ProjectState for an external Relay
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase", remote = "ProjectState")]
pub struct LimitedProjectState {
    pub project_id: Option<ProjectId>,
    pub last_change: Option<DateTime<Utc>>,
    pub disabled: bool,
    pub public_keys: SmallVec<[PublicKeyConfig; 1]>,
    pub slug: Option<String>,
    #[serde(with = "LimitedProjectConfig")]
    pub config: ProjectConfig,
    pub organization_id: Option<u64>,
}

impl ProjectState {
    /// Project state for a missing project.
    pub fn missing() -> Self {
        ProjectState {
            project_id: None,
            last_change: None,
            disabled: true,
            public_keys: SmallVec::new(),
            slug: None,
            config: ProjectConfig::default(),
            organization_id: None,
            last_fetch: Instant::now(),
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

    /// Returns configuration options for the public key.
    pub fn get_public_key_config(&self) -> Option<&PublicKeyConfig> {
        self.public_keys.get(0)
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
    pub fn outdated(&self, config: &Config) -> Outdated {
        let expiry = match self.project_id {
            None => config.cache_miss_expiry(),
            Some(_) => config.project_cache_expiry(),
        };

        let elapsed = self.last_fetch.elapsed();
        if elapsed >= expiry + config.project_grace_period() {
            Outdated::HardOutdated
        } else if elapsed >= expiry {
            Outdated::SoftOutdated
        } else {
            Outdated::Updated
        }
    }

    /// Returns the project config.
    pub fn config(&self) -> &ProjectConfig {
        &self.config
    }

    /// Returns `true` if the given project ID matches this project.
    ///
    /// If the project state has not been loaded, this check is skipped because the project
    /// identifier is not yet known. Likewise, this check is skipped for the legacy store endpoint
    /// which comes without a project ID. The id is later overwritten in `check_envelope`.
    pub fn is_valid_project_id(&self, stated_id: Option<ProjectId>) -> bool {
        match (self.project_id, stated_id) {
            (Some(actual_id), Some(stated_id)) => actual_id == stated_id,
            _ => true,
        }
    }

    /// Checks if this origin is allowed for this project.
    fn is_valid_origin(&self, origin: Option<&Url>) -> bool {
        // Generally accept any event without an origin.
        let origin = match origin {
            Some(origin) => origin,
            None => return true,
        };

        // Match against list of allowed origins. If the list is empty we always reject.
        let allowed = &self.config().allowed_domains;
        if allowed.is_empty() {
            return false;
        }

        let allowed: Vec<_> = allowed
            .iter()
            .map(|origin| origin.as_str().into())
            .collect();

        matches_any_origin(Some(origin.as_str()), &allowed)
    }

    /// Returns `true` if the given public key matches this state.
    ///
    /// This is a sanity check since project states are keyed by the DSN public key. Unless the
    /// state is invalid or unloaded, it must always match the public key.
    pub fn is_matching_key(&self, public_key: ProjectKey) -> bool {
        if let Some(key_config) = self.get_public_key_config() {
            // Always validate if we have a key config.
            key_config.public_key == public_key
        } else {
            // Loaded states must have a key config, but ignore missing and invalid states.
            self.project_id.is_none()
        }
    }

    /// Returns `Scoping` information for this project state.
    ///
    /// This scoping amends `RequestMeta::get_partial_scoping` by adding organization and key info.
    /// The processor must fetch the full scoping before attempting to rate limit with partial
    /// scoping.
    pub fn get_scoping(&self, meta: &RequestMeta) -> Scoping {
        let mut scoping = meta.get_partial_scoping();

        // The key configuration may be missing if the event has been queued for extended times and
        // project was refetched in between. In such a case, access to key quotas is not availabe,
        // but we can gracefully execute all other rate limiting.
        scoping.key_id = self
            .get_public_key_config()
            .and_then(|config| config.numeric_id);

        // The original project identifier is part of the DSN. If the DSN was moved to another
        // project, the actual project identifier is different and can be obtained from project
        // states. This is only possible when the project state has been loaded.
        if let Some(project_id) = self.project_id {
            scoping.project_id = project_id;
        }

        // This is a hack covering three cases:
        //  1. Relay has not fetched the project state. In this case we have no way of knowing
        //     which organization this project belongs to and we need to ignore any
        //     organization-wide rate limits stored globally. This project state cannot hold
        //     organization rate limits yet.
        //  2. The state has been loaded, but the organization_id is not available. This is only
        //     the case for legacy Sentry servers that do not reply with organization rate
        //     limits. Thus, the organization_id doesn't matter.
        //  3. An organization id is available and can be matched against rate limits. In this
        //     project, all organizations will match automatically, unless the organization id
        //     has changed since the last fetch.
        scoping.organization_id = self.organization_id.unwrap_or(0);

        scoping
    }

    /// Returns quotas declared in this project state.
    pub fn get_quotas(&self) -> &[Quota] {
        self.config.quotas.as_slice()
    }

    /// Determines whether the given request should be accepted or discarded.
    ///
    /// Returns `Ok(())` if the request should be accepted. Returns `Err(DiscardReason)` if the
    /// request should be discarded, by indicating the reason. The checks preformed for this are:
    ///
    ///  - Allowed origin headers
    ///  - Disabled or unknown projects
    ///  - Disabled project keys (DSN)
    pub fn check_request(&self, meta: &RequestMeta, config: &Config) -> Result<(), DiscardReason> {
        // Verify that the stated project id in the DSN matches the public key used to retrieve this
        // project state.
        if !self.is_valid_project_id(meta.project_id()) {
            return Err(DiscardReason::ProjectId);
        }

        // Try to verify the request origin with the project config.
        if !self.is_valid_origin(meta.origin()) {
            return Err(DiscardReason::Cors);
        }

        // sanity-check that the state has a matching public key loaded.
        if !self.is_matching_key(meta.public_key()) {
            relay_log::error!("public key mismatch on state {}", meta.public_key());
            return Err(DiscardReason::ProjectId);
        }

        // if the state is out of date, we proceed as if it was still up to date. The
        // upstream relay (or sentry) will still filter events.

        if self.outdated(config) != Outdated::HardOutdated {
            // if we recorded an invalid project state response from the upstream (i.e. parsing
            // failed), discard the event with a state reason.
            if self.invalid() {
                return Err(DiscardReason::ProjectState);
            }

            // only drop events if we know for sure the project or key are disabled.
            if self.disabled() {
                return Err(DiscardReason::ProjectId);
            }
        }

        Ok(())
    }

    /// Validates data in this project state and removes values that are partially invalid.
    pub fn sanitize(mut self) -> Self {
        self.config.quotas.retain(Quota::is_valid);
        self
    }
}

/// Represents a public key received from the projectconfig endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKeyConfig {
    /// Public part of key (random hash).
    pub public_key: ProjectKey,

    /// The primary key of the DSN in Sentry's main database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numeric_id: Option<u64>,
}

/// Actor representing organization and project configuration for a project key.
///
/// This actor no longer uniquely identifies a project. Instead, it identifies a project key.
/// Projects can define multiple keys, in which case this actor is duplicated for each instance.
pub struct Project {
    public_key: ProjectKey,
    config: Arc<Config>,
    manager: Addr<ProjectCache>,
    state: Option<Arc<ProjectState>>,
    state_channel: Option<Shared<oneshot::Receiver<Arc<ProjectState>>>>,
    rate_limits: RateLimits,
}

impl Project {
    pub fn new(key: ProjectKey, config: Arc<Config>, manager: Addr<ProjectCache>) -> Self {
        Project {
            public_key: key,
            config,
            manager,
            state: None,
            state_channel: None,
            rate_limits: RateLimits::new(),
        }
    }

    pub fn state(&self) -> Option<&ProjectState> {
        self.state.as_deref()
    }

    fn get_or_fetch_state(
        &mut self,
        context: &mut Context<Self>,
    ) -> Response<Arc<ProjectState>, ProjectError> {
        // count number of times we are looking for the project state
        metric!(counter(RelayCounters::ProjectStateGet) += 1);

        let state = self.state.as_ref();
        let outdated = state
            .map(|s| s.outdated(&self.config))
            .unwrap_or(Outdated::HardOutdated);

        let cached_state = match (state, outdated) {
            // There is no project state that can be used, fetch a state and return it.
            (None, _) | (_, Outdated::HardOutdated) => None,

            // The project is semi-outdated, fetch new state but return old one.
            (Some(state), Outdated::SoftOutdated) => Some(state.clone()),

            // The project is not outdated, return early here to jump over fetching logic below.
            (Some(state), Outdated::Updated) => return Response::ok(state.clone()),
        };

        let channel = match self.state_channel {
            Some(ref channel) => {
                relay_log::debug!("project {} state request amended", self.public_key);
                channel.clone()
            }
            None => {
                relay_log::debug!("project {} state requested", self.public_key);
                let channel = self.fetch_state(context);
                self.state_channel = Some(channel.clone());
                channel
            }
        };

        if let Some(rv) = cached_state {
            return Response::ok(rv);
        }

        let future = channel
            .map(|shared| (*shared).clone())
            .map_err(|_| ProjectError::FetchFailed);

        Response::future(future)
    }

    fn fetch_state(
        &mut self,
        context: &mut Context<Self>,
    ) -> Shared<oneshot::Receiver<Arc<ProjectState>>> {
        let (sender, receiver) = oneshot::channel();
        let public_key = self.public_key;

        self.manager
            .send(FetchProjectState { public_key })
            .into_actor(self)
            .map(move |state_result, slf, _ctx| {
                slf.state_channel = None;
                slf.state = state_result.map(|resp| resp.state).ok();

                if let Some(ref state) = slf.state {
                    relay_log::debug!("project state {} updated", public_key);
                    sender.send(state.clone()).ok();
                }
            })
            .drop_err()
            .spawn(context);

        receiver.shared()
    }

    fn get_scoping(&mut self, meta: &RequestMeta) -> Scoping {
        match self.state() {
            Some(state) => state.get_scoping(meta),
            None => meta.get_partial_scoping(),
        }
    }

    fn check_envelope(
        &mut self,
        mut envelope: Envelope,
        scoping: &Scoping,
    ) -> Result<CheckedEnvelope, DiscardReason> {
        if let Some(state) = self.state() {
            state.check_request(envelope.meta(), &self.config)?;
        }

        self.rate_limits.clean_expired();

        let quotas = self.state().map(|s| s.get_quotas()).unwrap_or(&[]);
        let envelope_limiter = EnvelopeLimiter::new(|item_scoping, _| {
            Ok(self.rate_limits.check_with_quotas(quotas, item_scoping))
        });

        let rate_limits = envelope_limiter.enforce(&mut envelope, scoping)?;
        let envelope = if envelope.is_empty() {
            None
        } else {
            Some(envelope)
        };

        Ok(CheckedEnvelope {
            envelope,
            rate_limits,
        })
    }

    fn check_envelope_scoped(&mut self, message: CheckEnvelope) -> CheckEnvelopeResponse {
        let scoping = self.get_scoping(message.envelope.meta());
        let result = self.check_envelope(message.envelope, &scoping);
        CheckEnvelopeResponse { result, scoping }
    }
}

impl Actor for Project {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        relay_log::debug!("project {} initialized without state", self.public_key);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        relay_log::debug!("project {} removed from cache", self.public_key);
    }
}

/// Returns the project state if it is already cached.
///
/// This is used for cases when we only want to perform operations that do
/// not require waiting for network requests.
///
pub struct GetCachedProjectState;

impl Message for GetCachedProjectState {
    type Result = Option<Arc<ProjectState>>;
}

impl Handler<GetCachedProjectState> for Project {
    type Result = Option<Arc<ProjectState>>;

    fn handle(
        &mut self,
        _message: GetCachedProjectState,
        _context: &mut Context<Self>,
    ) -> Self::Result {
        self.state.clone()
    }
}

/// Returns the project state.
///
/// The project state is fetched if it is missing or outdated.
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

/// Checks the envelope against project configuration and rate limits.
///
/// When `fetched`, then the project state is ensured to be up to date. When `cached`, an outdated
/// project state may be used, or otherwise the envelope is passed through unaltered.
///
/// To check the envelope, this runs:
///  - Validate origins and public keys
///  - Quotas with a limit of `0`
///  - Cached rate limits
#[derive(Debug)]
pub struct CheckEnvelope {
    envelope: Envelope,
    fetch: bool,
}

impl CheckEnvelope {
    /// Fetches the project state and checks the envelope.
    pub fn fetched(envelope: Envelope) -> Self {
        Self {
            envelope,
            fetch: true,
        }
    }

    /// Uses a cached project state and checks the envelope.
    pub fn cached(envelope: Envelope) -> Self {
        Self {
            envelope,
            fetch: false,
        }
    }
}

/// A checked envelope and associated rate limits.
///
/// Items violating the rate limits have been removed from the envelope. If all items are removed
/// from the envelope, `None` is returned in place of the envelope.
#[derive(Debug)]
pub struct CheckedEnvelope {
    pub envelope: Option<Envelope>,
    pub rate_limits: RateLimits,
}

/// Scoping information along with a checked envelope.
#[derive(Debug)]
pub struct CheckEnvelopeResponse {
    pub result: Result<CheckedEnvelope, DiscardReason>,
    pub scoping: Scoping,
}

impl Message for CheckEnvelope {
    type Result = Result<CheckEnvelopeResponse, ProjectError>;
}

impl Handler<CheckEnvelope> for Project {
    type Result = ActorResponse<Self, CheckEnvelopeResponse, ProjectError>;

    fn handle(&mut self, message: CheckEnvelope, context: &mut Self::Context) -> Self::Result {
        if message.fetch {
            // Project state fetching is allowed, so ensure the state is fetched and up-to-date.
            // This will return synchronously if the state is still cached.
            self.get_or_fetch_state(context)
                .into_actor()
                .map(self, context, move |_, slf, _ctx| {
                    slf.check_envelope_scoped(message)
                })
        } else {
            self.get_or_fetch_state(context);
            // message.fetch == false: Fetching must not block the store request. The EventManager
            // will later fetch the project state.
            ActorResponse::ok(self.check_envelope_scoped(message))
        }
    }
}

pub struct UpdateRateLimits(pub RateLimits);

impl Message for UpdateRateLimits {
    type Result = ();
}

impl Handler<UpdateRateLimits> for Project {
    type Result = ();

    fn handle(&mut self, message: UpdateRateLimits, _context: &mut Self::Context) -> Self::Result {
        let UpdateRateLimits(rate_limits) = message;
        self.rate_limits.merge(rate_limits);
    }
}
