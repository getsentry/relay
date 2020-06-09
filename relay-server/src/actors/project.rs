use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use chrono::{DateTime, Utc};
use futures::{future::Shared, sync::oneshot, Future};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use url::Url;

use relay_auth::PublicKey;
use relay_common::{metric, ProjectId};
use relay_config::{Config, RelayMode};
use relay_filter::{matches_any_origin, FiltersConfig};
use relay_general::pii::{DataScrubbingConfig, PiiConfig};
use relay_quotas::{DataCategory, Quota, RateLimits, Scoping};

use crate::actors::outcome::DiscardReason;
use crate::actors::project_cache::{FetchProjectState, ProjectCache, ProjectError};
use crate::envelope::Envelope;
use crate::extractors::RequestMeta;
use crate::metrics::RelayCounters;
use crate::utils::{ActorResponse, Response};

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
    /// Maximum event retention for the organization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_retention: Option<u16>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub quotas: Vec<Quota>,
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
    pub last_change: Option<DateTime<Utc>>,
    pub disabled: bool,
    #[serde(with = "limited_public_key_comfigs")]
    pub public_keys: Vec<PublicKeyConfig>,
    pub slug: Option<String>,
    #[serde(with = "LimitedProjectConfig")]
    pub config: ProjectConfig,
    pub organization_id: Option<u64>,
}

impl ProjectState {
    /// Project state for a missing project.
    pub fn missing() -> Self {
        ProjectState {
            last_change: None,
            disabled: true,
            public_keys: Vec::new(),
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
    pub fn outdated(&self, config: &Config) -> Outdated {
        let expiry = match self.slug {
            Some(_) => config.project_cache_expiry(),
            None => config.cache_miss_expiry(),
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
            .get_public_key_config(&scoping.public_key)
            .and_then(|config| config.numeric_id);

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

    /// Determines whether the given request should be accepted or discarded.
    ///
    /// Returns `Ok(())` if the request should be accepted. Returns `Err(DiscardReason)` if the
    /// request should be discarded, by indicating the reason. The checks preformed for this are:
    ///
    ///  - Allowed origin headers
    ///  - Disabled or unknown projects
    ///  - Disabled project keys (DSN)
    pub fn check_request(&self, meta: &RequestMeta, config: &Config) -> Result<(), DiscardReason> {
        // Try to verify the request origin with the project config.
        if !self.is_valid_origin(meta.origin()) {
            return Err(DiscardReason::Cors);
        }

        if self.outdated(config) == Outdated::HardOutdated {
            // if the state is out of date, we proceed as if it was still up to date. The
            // upstream relay (or sentry) will still filter events.

            // we assume it is unlikely to re-activate a disabled public key.
            // thus we handle events pretending the config is still valid,
            // except queueing events for unknown DSNs as they might have become
            // available in the meanwhile.
            match self.get_public_key_status(meta.public_key()) {
                PublicKeyStatus::Enabled => Ok(()),
                PublicKeyStatus::Disabled => Err(DiscardReason::ProjectId),
                PublicKeyStatus::Unknown => Ok(()),
            }
        } else {
            // if we recorded an invalid project state response from the upstream (i.e. parsing
            // failed), discard the event with a s
            if self.invalid() {
                return Err(DiscardReason::ProjectState);
            }

            // only drop events if we know for sure the project is disabled.
            if self.disabled() {
                return Err(DiscardReason::ProjectId);
            }

            // since the config has been fetched recently, we assume unknown
            // public keys do not exist and drop events eagerly. proxy mode is
            // an exception, where public keys are backfilled lazily after
            // events are sent to the upstream.
            match self.get_public_key_status(meta.public_key()) {
                PublicKeyStatus::Enabled => Ok(()),
                PublicKeyStatus::Disabled => Err(DiscardReason::ProjectId),
                PublicKeyStatus::Unknown => match config.relay_mode() {
                    RelayMode::Proxy => Ok(()),
                    _ => Err(DiscardReason::ProjectId),
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
}

mod limited_public_key_comfigs {
    use super::*;
    use serde::ser::SerializeSeq;

    /// Represents a public key received from the projectconfig endpoint.
    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase", remote = "PublicKeyConfig")]
    pub struct LimitedPublicKeyConfig {
        pub public_key: String,
        pub is_enabled: bool,
    }

    /// Serializes a list of `PublicKeyConfig` objects using `LimitedPublicKeyConfig`.
    pub fn serialize<S>(keys: &[PublicKeyConfig], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Wrapper<'a>(#[serde(with = "LimitedPublicKeyConfig")] &'a PublicKeyConfig);

        let mut seq = serializer.serialize_seq(Some(keys.len()))?;
        for key in keys {
            seq.serialize_element(&Wrapper(key))?;
        }
        seq.end()
    }
}
pub struct Project {
    id: ProjectId,
    config: Arc<Config>,
    manager: Addr<ProjectCache>,
    state: Option<Arc<ProjectState>>,
    state_channel: Option<Shared<oneshot::Receiver<Arc<ProjectState>>>>,
    rate_limits: RateLimits,
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
            rate_limits: RateLimits::new(),
            is_local: false,
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

        let alternative_rv = match (state, outdated, self.is_local) {
            // The state is fetched from a local file, don't use own caching logic. Rely on
            // `ProjectCache#local_states` for caching.
            (_, _, true) => None,

            // There is no project state that can be used, fetch a state and return it.
            (None, _, false) | (_, Outdated::HardOutdated, false) => None,

            // The project is semi-outdated, fetch new state but return old one.
            (Some(state), Outdated::SoftOutdated, false) => Some(state.clone()),

            // The project is not outdated, return early here to jump over fetching logic below.
            (Some(state), Outdated::Updated, false) => return Response::ok(state.clone()),
        };

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

        if let Some(rv) = alternative_rv {
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
        let id = self.id;

        self.manager
            .send(FetchProjectState { id })
            .into_actor(self)
            .map(move |state_result, slf, _ctx| {
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
        envelope: Envelope,
        scoping: &Scoping,
    ) -> Result<CheckedEnvelope, DiscardReason> {
        if let Some(state) = self.state() {
            state.check_request(envelope.meta(), &self.config)?;
        }

        self.rate_limits.clean_expired();

        // TODO: Apply rate limits no non-event items once implemented.
        let rate_limits = self.rate_limits.check(scoping.item(DataCategory::Error));
        let envelope = if rate_limits.is_limited() {
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
        log::debug!("project {} initialized without state", self.id);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::debug!("project {} removed from cache", self.id);
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
