use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use relay_base_schema::project::{ProjectId, ProjectKey};
#[cfg(feature = "processing")]
use relay_cardinality::CardinalityLimit;
use relay_config::Config;
use relay_dynamic_config::{ErrorBoundary, Feature, LimitedProjectConfig, ProjectConfig};
use relay_filter::matches_any_origin;
use relay_metrics::{
    Aggregator, Bucket, MergeBuckets, MetaAggregator, MetricMeta, MetricNamespace,
};
use relay_quotas::{
    CachedRateLimits, DataCategory, MetricNamespaceScoping, Quota, RateLimits, Scoping,
};
use relay_sampling::evaluation::ReservoirCounters;
use relay_statsd::metric;
use relay_system::{Addr, BroadcastChannel};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tokio::time::Instant;
use url::Url;

use crate::extractors::RequestMeta;
use crate::metrics::{MetricOutcomes, MetricsLimiter};
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
#[cfg(feature = "processing")]
use crate::services::processor::RateLimitBuckets;
use crate::services::processor::{EncodeMetricMeta, EnvelopeProcessor};
use crate::services::project::metrics::filter_namespaces;
use crate::services::project_cache::{BucketSource, CheckedEnvelope, ProjectCache, RequestUpdate};

use crate::statsd::RelayCounters;
use crate::utils::{self, EnvelopeLimiter, ManagedEnvelope, RetryBackoff};

mod metrics;
mod state;

pub use state::{ExpiryState, ProjectFetchState, ProjectState};

/// Sender type for messages that respond with project states.
pub type ProjectSender = relay_system::BroadcastSender<ProjectFetchState>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedProjectState {
    pub disabled: bool,
    #[serde(flatten)]
    pub info: ProjectInfo, // TODO(jjbayer): Should be Option<Arc> for efficiency?
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase", remote = "ParsedProjectState")]
pub struct LimitedParsedProjectState {
    disabled: bool,
    #[serde(with = "LimitedProjectInfo")]
    #[serde(flatten)]
    info: ProjectInfo,
}

impl TryFrom<ProjectState> for ParsedProjectState {
    type Error = ();

    fn try_from(value: ProjectState) -> Result<Self, Self::Error> {
        match value {
            ProjectState::Enabled(info) => Ok(ParsedProjectState {
                disabled: false,
                info: info.as_ref().clone(),
            }),
            ProjectState::Disabled => Ok(ParsedProjectState {
                disabled: true,
                info: ProjectInfo::default(),
            }),
            ProjectState::Invalid => Err(()),
        }
    }
}

/// The project state is a cached server state of a project.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectInfo {
    /// Unique identifier of this project.
    pub project_id: Option<ProjectId>,
    /// The timestamp of when the state was last changed.
    ///
    /// This might be `None` in some rare cases like where states
    /// are faked locally.
    #[serde(default)]
    pub last_change: Option<DateTime<Utc>>,
    /// Indicates that the project is disabled.
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
}

/// Controls how we serialize a ProjectState for an external Relay
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase", remote = "ProjectInfo")]
pub struct LimitedProjectInfo {
    pub project_id: Option<ProjectId>,
    pub last_change: Option<DateTime<Utc>>,
    pub public_keys: SmallVec<[PublicKeyConfig; 1]>,
    pub slug: Option<String>,
    #[serde(with = "LimitedProjectConfig")]
    pub config: ProjectConfig,
    pub organization_id: Option<u64>,
}

impl ProjectInfo {
    /// Returns configuration options for the public key.
    pub fn get_public_key_config(&self) -> Option<&PublicKeyConfig> {
        self.public_keys.first()
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
    pub fn is_valid_project_id(&self, stated_id: Option<ProjectId>, config: &Config) -> bool {
        match (self.project_id, stated_id, config.override_project_ids()) {
            (Some(actual_id), Some(stated_id), false) => actual_id == stated_id,
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
    pub fn is_matching_key(&self, project_key: ProjectKey) -> bool {
        if let Some(key_config) = self.get_public_key_config() {
            // Always validate if we have a key config.
            key_config.public_key == project_key
        } else {
            // Loaded states must have a key config, but ignore missing and invalid states.
            self.project_id.is_none()
        }
    }

    /// Amends request `Scoping` with information from this project state.
    ///
    /// This scoping amends `RequestMeta::get_partial_scoping` by adding organization and key info.
    /// The processor must fetch the full scoping before attempting to rate limit with partial
    /// scoping.
    ///
    /// To get the own scoping of this ProjectKey without amending request information, use
    /// [`Project::scoping`] instead.
    pub fn scope_request(&self, meta: &RequestMeta) -> Scoping {
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

    /// Returns cardinality limits declared in this project state.
    #[cfg(feature = "processing")]
    pub fn get_cardinality_limits(&self) -> &[CardinalityLimit] {
        match self.config.metrics {
            ErrorBoundary::Ok(ref m) => m.cardinality_limits.as_slice(),
            _ => &[],
        }
    }

    /// Validates data in this project state and removes values that are partially invalid.
    pub fn sanitize(mut self) -> Self {
        self.config.sanitize();
        self
    }

    /// Returns `true` if the given feature is enabled for this project.
    pub fn has_feature(&self, feature: Feature) -> bool {
        self.config.features.has(feature)
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

#[derive(Debug)]
struct StateChannel {
    inner: BroadcastChannel<Arc<ProjectInfo>>,
    no_cache: bool,
}

impl StateChannel {
    pub fn new() -> Self {
        Self {
            inner: BroadcastChannel::new(),
            no_cache: false,
        }
    }

    pub fn no_cache(&mut self, no_cache: bool) -> &mut Self {
        self.no_cache = no_cache;
        self
    }
}

enum GetOrFetch<'a> {
    Cached(ProjectState),
    Scheduled(&'a mut StateChannel),
}

/// Represents either the project state or an aggregation of metrics.
///
/// We have to delay rate limiting on metrics until we have a valid project state,
/// So when we don't have one yet, we hold them in this aggregator until the project state arrives.
///
/// TODO: spool queued metrics to disk when the in-memory aggregator becomes too full.
#[derive(Debug)]
enum State {
    Cached(ProjectFetchState),
    Pending,
}

impl State {
    fn state_value(&self) -> Option<ProjectFetchState> {
        match self {
            State::Cached(state) => Some(state.clone()),
            State::Pending => None,
        }
    }

    fn new() -> Self {
        Self::Pending
    }
}

/// Structure representing organization and project configuration for a project key.
///
/// This structure no longer uniquely identifies a project. Instead, it identifies a project key.
/// Projects can define multiple keys, in which case this structure is duplicated for each instance.
#[derive(Debug)]
pub struct Project {
    backoff: RetryBackoff,
    next_fetch_attempt: Option<Instant>,
    last_updated_at: Instant,
    project_key: ProjectKey,
    config: Arc<Config>,
    state: State,
    state_channel: Option<StateChannel>,
    rate_limits: CachedRateLimits,
    last_no_cache: Instant,
    reservoir_counters: ReservoirCounters,
    metric_meta_aggregator: MetaAggregator,
    has_pending_metric_meta: bool,
}

impl Project {
    /// Creates a new `Project`.
    pub fn new(key: ProjectKey, config: Arc<Config>) -> Self {
        Project {
            backoff: RetryBackoff::new(config.http_max_retry_interval()),
            next_fetch_attempt: None,
            last_updated_at: Instant::now(),
            project_key: key,
            state: State::new(),
            state_channel: None,
            rate_limits: CachedRateLimits::new(),
            last_no_cache: Instant::now(),
            reservoir_counters: Arc::default(),
            metric_meta_aggregator: MetaAggregator::new(config.metrics_meta_locations_max()),
            has_pending_metric_meta: false,
            config,
        }
    }

    /// Returns the [`ReservoirCounters`] for the project.
    pub fn reservoir_counters(&self) -> ReservoirCounters {
        self.reservoir_counters.clone()
    }

    // TODO(jjbayer): Get rid of state_value()
    fn state_value(&self) -> Option<ProjectFetchState> {
        self.state.state_value()
    }

    /// If a reservoir rule is no longer in the sampling config, we will remove those counters.
    fn remove_expired_reservoir_rules(&self) {
        let Some(state) = &self.non_expired_state() else {
            return;
        };
        let ProjectState::Enabled(state) = state else {
            return;
        }; // TODO: util

        let Some(ErrorBoundary::Ok(config)) = state.config.sampling.as_ref() else {
            return;
        };

        // Using try_lock to not slow down the project cache service.
        if let Ok(mut guard) = self.reservoir_counters.try_lock() {
            guard.retain(|key, _| config.rules.iter().any(|rule| rule.id == *key));
        }
    }

    pub fn merge_rate_limits(&mut self, rate_limits: RateLimits) {
        self.rate_limits.merge(rate_limits);
    }

    /// Returns the project state if it is not expired.
    ///
    /// Convenience wrapper around [`expiry_state`](Self::expiry_state).
    pub fn non_expired_state(&self) -> Option<ProjectState> {
        match &self.state {
            State::Cached(state) => match state.expiry_state(&self.config) {
                ExpiryState::Updated(state) => Some(state.clone()),
                ExpiryState::Stale(state) => Some(state.clone()),
                ExpiryState::Expired => None,
            },
            State::Pending => None,
        }
    }

    /// Returns the next attempt `Instant` if backoff is initiated, or None otherwise.
    pub fn next_fetch_attempt(&self) -> Option<Instant> {
        self.next_fetch_attempt
    }

    /// The last time the project state was updated
    pub fn last_updated_at(&self) -> Instant {
        self.last_updated_at
    }

    /// Refresh the update time of the project in order to delay eviction.
    ///
    /// Called by the project cache when the project state is refreshed.
    pub fn refresh_updated_timestamp(&mut self) {
        self.last_updated_at = Instant::now();
    }

    /// Validates and inserts given [buckets](Bucket) into the metrics aggregator.
    ///
    /// The buckets will be keyed underneath this project key.
    pub fn merge_buckets(
        &mut self,
        aggregator: &Addr<Aggregator>,
        metric_outcomes: &MetricOutcomes,
        outcome_aggregator: &Addr<TrackOutcome>,
        buckets: Vec<Bucket>,
        source: BucketSource,
    ) {
        // Best effort check for rate limits and project state. Continue if there is no project state.
        let buckets = match self.check_buckets(metric_outcomes, outcome_aggregator, buckets) {
            CheckedBuckets::NoProject(buckets) => buckets,
            CheckedBuckets::Checked { buckets, .. } => buckets,
            CheckedBuckets::Dropped => return,
        };

        let buckets = filter_namespaces(buckets, source);

        aggregator.send(MergeBuckets::new(
            self.project_key,
            buckets.into_iter().collect(),
        ));
    }

    /// Returns a list of buckets back to the aggregator.
    ///
    /// This is used to return flushed buckets back to the aggregator if the project has not been
    /// loaded at the time of flush.
    ///
    /// Buckets at this stage are expected to be validated already.
    pub fn return_buckets(&self, aggregator: &Addr<Aggregator>, buckets: Vec<Bucket>) {
        aggregator.send(MergeBuckets::new(
            self.project_key,
            buckets.into_iter().collect(),
        ));
    }

    pub fn add_metric_meta(
        &mut self,
        meta: MetricMeta,
        envelope_processor: Addr<EnvelopeProcessor>,
    ) {
        let state = self.non_expired_state();
        // TODO(jjbayer): Use ExposedState here.

        // Only track metadata if custom metrics are enabled, or we don't know yet whether they are
        // enabled.
        if !state.map_or(true, |state| state.has_feature(Feature::CustomMetrics)) {
            relay_log::trace!(
                "metric meta feature flag not enabled for project {}",
                self.project_key
            );
            return;
        }

        let Some(meta) = self.metric_meta_aggregator.add(self.project_key, meta) else {
            // Nothing to do. Which means there is also no pending data.
            relay_log::trace!("metric meta aggregator already has data, nothing to send upstream");
            return;
        };

        let scoping = self.scoping();
        match scoping {
            Some(scoping) => {
                // We can only have a scoping if we also have a state, which means at this point feature
                // flags are already checked.
                envelope_processor.send(EncodeMetricMeta { scoping, meta })
            }
            None => self.has_pending_metric_meta = true,
        }
    }

    fn flush_metric_meta(&mut self, envelope_processor: &Addr<EnvelopeProcessor>) {
        if !self.has_pending_metric_meta {
            return;
        }
        let Some(state) = self.state_value() else {
            return;
        };
        let Some(scoping) = self.scoping() else {
            return;
        };

        // All relevant info has been gathered, consider us flushed.
        self.has_pending_metric_meta = false;

        if !state.has_feature(Feature::CustomMetrics) {
            relay_log::debug!(
                "clearing metric meta aggregator, because project {} does not have feature flag enabled",
                self.project_key,
            );
            // Project/Org does not have the feature, forget everything.
            self.metric_meta_aggregator.clear(self.project_key);
            return;
        }

        // Flush the entire aggregator containing all code locations for the project.
        //
        // There is the rare case when this flushes items which have already been flushed before.
        // This happens only if we temporarily lose a previously valid and loaded project state
        // and then reveive an update for the project state.
        // While this is a possible occurence the effect should be relatively limited,
        // especially since the final store does de-deuplication.
        for meta in self
            .metric_meta_aggregator
            .get_all_relevant(self.project_key)
        {
            relay_log::debug!(
                "flushing aggregated metric meta for project {}",
                self.project_key
            );
            metric!(counter(RelayCounters::ProjectStateFlushAllMetricMeta) += 1);
            envelope_processor.send(EncodeMetricMeta { scoping, meta });
        }
    }

    /// Returns `true` if backoff expired and new attempt can be triggered.
    fn can_fetch(&self) -> bool {
        self.next_fetch_attempt
            .map(|next_attempt_at| next_attempt_at <= Instant::now())
            .unwrap_or(true)
    }

    /// Triggers a debounced refresh of the project state.
    ///
    /// If the state is already being updated in the background, this method checks if the request
    /// needs to be upgraded with the `no_cache` flag to ensure a more recent update.
    fn fetch_state(
        &mut self,
        project_cache: Addr<ProjectCache>,
        no_cache: bool,
    ) -> &mut StateChannel {
        // If there is a running request and we do not need to upgrade it to no_cache, or if the
        // backoff is started and the new attempt is still somewhere in the future, skip
        // scheduling a new fetch.
        let should_fetch = !matches!(self.state_channel, Some(ref channel) if channel.no_cache || !no_cache)
            && self.can_fetch();

        let channel = self.state_channel.get_or_insert_with(StateChannel::new);

        if should_fetch {
            channel.no_cache(no_cache);
            let attempts = self.backoff.attempt() + 1;
            relay_log::debug!(
                "project {} state requested {attempts} times",
                self.project_key
            );
            project_cache.send(RequestUpdate::new(self.project_key, no_cache));
        }

        channel
    }

    fn get_or_fetch_state(
        &mut self,
        project_cache: Addr<ProjectCache>,
        mut no_cache: bool,
    ) -> GetOrFetch<'_> {
        // count number of times we are looking for the project state
        metric!(counter(RelayCounters::ProjectStateGet) += 1);

        // Allow at most 1 no_cache request per second. Gracefully degrade to cached requests.
        if no_cache {
            if self.last_no_cache.elapsed() < Duration::from_secs(1) {
                no_cache = false;
            } else {
                metric!(counter(RelayCounters::ProjectStateNoCache) += 1);
                self.last_no_cache = Instant::now();
            }
        }

        let cached_state = match &self.state {
            // Never use the cached state if `no_cache` is set.
            _ if no_cache => None,
            State::Pending => None,
            State::Cached(state) => {
                match state.expiry_state(&self.config) {
                    // There is no project state that can be used, fetch a state and return it.
                    ExpiryState::Expired => None,
                    // The project is semi-outdated, fetch new state but return old one.
                    ExpiryState::Stale(state) => Some(state.clone()),
                    // The project is not outdated, return early here to jump over fetching logic below.
                    ExpiryState::Updated(state) => return GetOrFetch::Cached(state.clone()),
                }
            }
        };

        let channel = self.fetch_state(project_cache, no_cache);

        match cached_state {
            Some(state) => GetOrFetch::Cached(state),
            None => GetOrFetch::Scheduled(channel),
        }
    }

    /// Returns the cached project state if it is valid.
    ///
    /// Depending on the state of the cache, this method takes different action:
    ///
    ///  - If the cached state is up-to-date, this method simply returns `Some`.
    ///  - If the cached state is stale, this method triggers a refresh in the background and
    ///    returns `Some`. The stale period can be configured through
    ///    [`Config::project_grace_period`].
    ///  - If there is no cached state or the cached state is fully outdated, this method triggers a
    ///    refresh in the background and returns `None`.
    ///
    /// If `no_cache` is set to true, this method always returns `None` and always triggers a
    /// background refresh.
    ///
    /// To wait for a valid state instead, use [`get_state`](Self::get_state).
    pub fn get_cached_state(
        &mut self,
        project_cache: Addr<ProjectCache>,
        no_cache: bool,
    ) -> Option<ProjectFetchState> {
        match self.get_or_fetch_state(project_cache, no_cache) {
            GetOrFetch::Cached(state) => Some(state),
            GetOrFetch::Scheduled(_) => None,
        }
    }

    /// Obtains a valid project state and passes it to the sender once ready.
    ///
    /// This first checks if the state needs to be updated. This is the case if the project state
    /// has passed its cache timeout. The `no_cache` flag forces an update. This does nothing if an
    /// update is already running in the background.
    ///
    /// Independent of updating, _stale_ states are passed to the sender immediately as long as they
    /// are in the [grace period](Config::project_grace_period).
    pub fn get_state(
        &mut self,
        project_cache: Addr<ProjectCache>,
        sender: ProjectSender,
        no_cache: bool,
    ) {
        match self.get_or_fetch_state(project_cache, no_cache) {
            GetOrFetch::Cached(state) => {
                sender.send(state);
            }

            GetOrFetch::Scheduled(channel) => {
                channel.inner.attach(sender);
            }
        }
    }

    /// Ensures the project state gets updated.
    ///
    /// This first checks if the state needs to be updated. This is the case if the project state
    /// has passed its cache timeout. The `no_cache` flag forces another update unless one is
    /// already running in the background.
    ///
    /// If an update is required, the update will start in the background and complete at a later
    /// point. Therefore, this method is useful to trigger an update early if it is already clear
    /// that the project state will be needed soon. To retrieve an updated state, use
    /// [`Project::get_state`] instead.
    pub fn prefetch(&mut self, project_cache: Addr<ProjectCache>, no_cache: bool) {
        self.get_cached_state(project_cache, no_cache);
    }

    /// Replaces the internal project state with a new one and triggers pending actions.
    ///
    /// This flushes pending envelopes from [`ValidateEnvelope`] and
    /// notifies all pending receivers from [`get_state`](Self::get_state).
    ///
    /// `no_cache` should be passed from the requesting call. Updates with `no_cache` will always
    /// take precedence.
    ///
    /// [`ValidateEnvelope`]: crate::services::project_cache::ValidateEnvelope
    #[allow(clippy::too_many_arguments)]
    pub fn update_state(
        &mut self,
        project_cache: &Addr<ProjectCache>,
        mut state: ProjectFetchState,
        envelope_processor: &Addr<EnvelopeProcessor>,
        no_cache: bool,
    ) {
        // Initiate the backoff if the incoming state is invalid. Reset it otherwise.
        if state.invalid() {
            self.next_fetch_attempt = Instant::now().checked_add(self.backoff.next_backoff());
        } else {
            self.next_fetch_attempt = None;
            self.backoff.reset();
        }

        let Some(channel) = self.state_channel.take() else {
            relay_log::error!(tags.project_key = %self.project_key, "channel is missing for the state update");
            return;
        };

        // If the channel has `no_cache` set but we are not a `no_cache` request, we have
        // been superseeded. Put it back and let the other request take precedence.
        if channel.no_cache && !no_cache {
            self.state_channel = Some(channel);
            return;
        }

        match self.expiry_state(&self.config) {
            // If the new state is invalid but the old one still usable, keep the old one.
            ExpiryState::Updated(old) | ExpiryState::Stale(old) if state.invalid() => state = old,
            // If the new state is valid or the old one is expired, always use the new one.
            _ => self.state = State::Cached(Arc::clone(&state)),
        }

        // If the state is still invalid, return back the taken channel and schedule state update.
        if state.invalid() {
            self.state_channel = Some(channel);
            let attempts = self.backoff.attempt() + 1;
            relay_log::debug!(
                "project {} state requested {attempts} times",
                self.project_key
            );

            project_cache.send(RequestUpdate::new(self.project_key, no_cache));
            return;
        }

        // Flush all waiting recipients.
        relay_log::debug!("project state {} updated", self.project_key);
        channel.inner.send(state);

        self.after_state_updated(envelope_processor);
    }

    /// Called after all state validations and after the project state is updated.
    ///
    /// See also: [`Self::update_state`].
    fn after_state_updated(&mut self, envelope_processor: &Addr<EnvelopeProcessor>) {
        self.flush_metric_meta(envelope_processor);
        // Check if the new sampling config got rid of any reservoir rules we have counters for.
        self.remove_expired_reservoir_rules();
    }

    /// Creates `Scoping` for this project if the state is loaded.
    ///
    /// Returns `Some` if the project state has been fetched and contains a project identifier,
    /// otherwise `None`.
    ///
    /// NOTE: This function does not check the expiry of the project state.
    pub fn scoping(&self) -> Option<Scoping> {
        let state = self.state_value()?;
        Some(Scoping {
            organization_id: state.organization_id.unwrap_or(0),
            project_id: state.project_id?,
            project_key: self.project_key,
            key_id: state
                .get_public_key_config()
                .and_then(|config| config.numeric_id),
        })
    }

    /// Runs the checks on incoming envelopes.
    ///
    /// See, [`crate::services::project_cache::CheckEnvelope`] for more information
    ///
    /// * checks the rate limits
    /// * validates the envelope meta in `check_request` - determines whether the given request
    ///   should be accepted or discarded
    ///
    /// IMPORTANT: If the [`ProjectState`] is invalid, the `check_request` will be skipped and only
    /// rate limites will be validated. This function **must not** be called in the main processing
    /// pipeline.
    pub fn check_envelope(
        &mut self,
        mut envelope: ManagedEnvelope,
        outcome_aggregator: Addr<TrackOutcome>,
    ) -> Result<CheckedEnvelope, DiscardReason> {
        let state = self.non_expired_state().filter(|state| !state.invalid());
        let mut scoping = envelope.scoping();

        if let Some(ref state) = state {
            scoping = state.scope_request(envelope.envelope().meta());
            envelope.scope(scoping);

            if let Err(reason) = state.check_envelope(envelope.envelope(), &self.config) {
                envelope.reject(Outcome::Invalid(reason));
                return Err(reason);
            }
        }

        let current_limits = self.rate_limits.current_limits();

        let quotas = state.as_deref().map(|s| s.get_quotas()).unwrap_or(&[]);
        let envelope_limiter = EnvelopeLimiter::new(|item_scoping, _| {
            Ok(current_limits.check_with_quotas(quotas, item_scoping))
        });

        let (enforcement, mut rate_limits) =
            envelope_limiter.enforce(envelope.envelope_mut(), &scoping)?;
        enforcement.track_outcomes(envelope.envelope(), &scoping, outcome_aggregator);
        envelope.update();

        // Special case: Expose active rate limits for all metric namespaces if there is at least
        // one metrics item in the Envelope to communicate backoff to SDKs. This is necessary
        // because `EnvelopeLimiter` cannot not check metrics without parsing item contents.
        if envelope.envelope().items().any(|i| i.ty().is_metrics()) {
            let mut metrics_scoping = scoping.item(DataCategory::MetricBucket);
            metrics_scoping.namespace = MetricNamespaceScoping::Any;
            rate_limits.merge(current_limits.check_with_quotas(quotas, metrics_scoping));
        }

        let envelope = if envelope.envelope().is_empty() {
            // Individual rate limits have already been issued above
            envelope.reject(Outcome::RateLimited(None));
            None
        } else {
            Some(envelope)
        };

        Ok(CheckedEnvelope {
            envelope,
            rate_limits,
        })
    }

    /// Drops metrics buckets if they are not allowed for this project.
    ///
    /// Reasons for dropping can be rate limits or a disabled project.
    pub fn check_buckets(
        &mut self,
        metric_outcomes: &MetricOutcomes,
        outcome_aggregator: &Addr<TrackOutcome>,
        buckets: Vec<Bucket>,
    ) -> CheckedBuckets {
        let project_info = match self.non_expired_state() {
            Some(p) => match p {
                ProjectState::Enabled(info) => info,
                ProjectState::Invalid | ProjectState::Disabled => {
                    relay_log::debug!("dropping {} buckets for disabled project", buckets.len());
                    return CheckedBuckets::Dropped;
                }
            },
            None => return CheckedBuckets::NoProject(buckets),
        };

        let Some(scoping) = self.scoping() else {
            relay_log::error!(
                tags.project_key = self.project_key.as_str(),
                "there is no scoping: dropping {} buckets",
                buckets.len(),
            );
            return CheckedBuckets::Dropped;
        };

        let mut buckets = project_info(buckets, metric_outcomes, &project_info, scoping);

        let namespaces: BTreeSet<MetricNamespace> = buckets
            .iter()
            .filter_map(|bucket| bucket.name.try_namespace())
            .collect();

        let current_limits = self.rate_limits.current_limits();
        for namespace in namespaces {
            let limits = current_limits.check_with_quotas(
                project_info.get_quotas(),
                scoping.item(DataCategory::MetricBucket),
            );

            if limits.is_limited() {
                let rejected;
                (buckets, rejected) = utils::split_off(buckets, |bucket| {
                    bucket.name.try_namespace() == Some(namespace)
                });

                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                metric_outcomes.track(scoping, &rejected, Outcome::RateLimited(reason_code));
            }
        }

        let quotas = project_info.config.quotas.clone();
        let buckets = match MetricsLimiter::create(buckets, quotas, scoping) {
            Ok(mut bucket_limiter) => {
                bucket_limiter.enforce_limits(current_limits, metric_outcomes, outcome_aggregator);
                bucket_limiter.into_buckets()
            }
            Err(buckets) => buckets,
        };

        if buckets.is_empty() {
            return CheckedBuckets::Dropped;
        }

        CheckedBuckets::Checked {
            scoping,
            project_info,
            buckets,
        }
    }
}

/// Return value of [`Project::check_buckets`].
#[derive(Debug)]
pub enum CheckedBuckets {
    /// There is no project state available for these metrics yet.
    ///
    /// The metrics should be returned to the aggregator until the project state becomes available.
    NoProject(Vec<Bucket>),
    /// The buckets have been validated and can be processed.
    Checked {
        /// Project scoping.
        scoping: Scoping,
        /// Project info.
        project_info: Arc<ProjectInfo>,
        /// List of buckets.
        buckets: Vec<Bucket>,
    },
    /// All buckets have been dropped.
    ///
    /// Can happen for multiple reasons:
    /// - The project is disabled or not valid.
    /// - All metrics have been filtered.
    Dropped,
}

#[cfg(test)]
mod tests {
    use crate::metrics::MetricStats;
    use relay_common::time::UnixTimestamp;
    use relay_metrics::BucketValue;
    use relay_test::mock_service;
    use serde_json::json;

    use super::*;

    #[test]
    fn get_state_expired() {
        for expiry in [9999, 0] {
            let config = Arc::new(
                Config::from_json_value(json!(
                    {
                        "cache": {
                            "project_expiry": expiry,
                            "project_grace_period": 0,
                            "eviction_interval": 9999 // do not evict
                        }
                    }
                ))
                .unwrap(),
            );

            // Initialize project with a state
            let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
            let mut project_state = ProjectInfo::allowed();
            project_state.project_id = Some(ProjectId::new(123));
            let mut project = Project::new(project_key, config.clone());
            project.state = State::Cached(Arc::new(project_state));

            // Direct access should always yield a state:
            assert!(project.state_value().is_some());

            if expiry > 0 {
                // With long expiry, should get a state
                assert!(project.non_expired_state().is_some());
            } else {
                // With 0 expiry, project should expire immediately. No state can be set.
                assert!(project.non_expired_state().is_none());
            }
        }
    }

    #[tokio::test]
    async fn test_stale_cache() {
        let (addr, _) = mock_service("project_cache", (), |&mut (), _| {});
        let (envelope_processor, _) = mock_service("envelope_processor", (), |&mut (), _| {});

        let config = Arc::new(
            Config::from_json_value(json!(
                {
                    "cache": {
                        "project_expiry": 100,
                        "project_grace_period": 0,
                        "eviction_interval": 9999 // do not evict
                    }
                }
            ))
            .unwrap(),
        );

        let channel = StateChannel::new();

        // Initialize project with a state.
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let mut project_state = ProjectInfo::allowed();
        project_state.project_id = Some(ProjectId::new(123));
        let mut project = Project::new(project_key, config);
        project.state_channel = Some(channel);
        project.state = State::Cached(Arc::new(project_state));

        // The project ID must be set.
        assert!(!project.state_value().unwrap().invalid());
        assert!(project.next_fetch_attempt.is_none());
        // Try to update project with errored project state.
        project.update_state(
            &addr,
            Arc::new(ProjectState::err()),
            &envelope_processor,
            false,
        );
        // Since we got invalid project state we still keep the old one meaning there
        // still must be the project id set.
        assert!(!project.state_value().unwrap().invalid());
        assert!(project.next_fetch_attempt.is_some());

        // This tests that we actually initiate the backoff and the backoff mechanism works:
        // * first call to `update_state` with invalid ProjectState starts the backoff, but since
        //   it's the first attemt, we get Duration of 0.
        // * second call to `update_state` here will bumpt the `next_backoff` Duration to somehing
        //   like ~ 1s
        // * and now, by calling `fetch_state` we test that it's a noop, since if backoff is active
        //   we should never fetch
        // * without backoff it would just panic, not able to call the ProjectCache service
        let channel = StateChannel::new();
        project.state_channel = Some(channel);
        project.update_state(
            &addr,
            Arc::new(ProjectState::err()),
            &envelope_processor,
            false,
        );
        project.fetch_state(addr, false);
    }

    fn create_project(config: Option<serde_json::Value>) -> Project {
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let mut project = Project::new(project_key, Arc::new(Config::default()));
        let mut project_state = ProjectInfo::allowed();
        project_state.project_id = Some(ProjectId::new(42));
        if let Some(config) = config {
            project_state.config = serde_json::from_value(config).unwrap();
        }
        project.state = State::Cached(Arc::new(project_state));
        project
    }

    fn create_metric(name: &str) -> Bucket {
        Bucket {
            name: name.into(),
            width: 0,
            value: BucketValue::counter(1.into()),
            timestamp: UnixTimestamp::from_secs(1000),
            tags: Default::default(),
            metadata: Default::default(),
        }
    }

    #[test]
    fn test_check_buckets_no_project() {
        let (outcome_aggregator, _) = Addr::custom();
        let (metric_stats, mut metric_stats_rx) = MetricStats::test();
        let metric_outcomes = MetricOutcomes::new(metric_stats, outcome_aggregator.clone());

        let mut project = create_project(None);
        project.state = State::Pending;
        let buckets = vec![create_metric("d:transactions/foo")];
        let cb = project.check_buckets(&metric_outcomes, &outcome_aggregator, buckets.clone());

        match cb {
            CheckedBuckets::NoProject(b) => {
                assert_eq!(b, buckets)
            }
            cb => panic!("{cb:?}"),
        }

        drop(metric_outcomes);
        assert!(metric_stats_rx.blocking_recv().is_none());
    }

    #[test]
    fn test_check_buckets_rate_limit() {
        let (outcome_aggregator, _) = Addr::custom();
        let (metric_stats, mut metric_stats_rx) = MetricStats::test();
        let metric_outcomes = MetricOutcomes::new(metric_stats, outcome_aggregator.clone());

        let mut project = create_project(None);
        let buckets = vec![create_metric("d:transactions/foo")];
        let cb = project.check_buckets(&metric_outcomes, &outcome_aggregator, buckets.clone());

        match cb {
            CheckedBuckets::Checked {
                scoping,
                project_info: _,
                buckets: b,
            } => {
                assert_eq!(scoping, project.scoping().unwrap());
                assert_eq!(b, buckets)
            }
            cb => panic!("{cb:?}"),
        }

        drop(metric_outcomes);
        assert!(metric_stats_rx.blocking_recv().is_none());
    }

    #[test]
    fn test_check_buckets_rate_limit_no_quota() {
        let (outcome_aggregator, _) = Addr::custom();
        let (metric_stats, mut metric_stats_rx) = MetricStats::test();
        let metric_outcomes = MetricOutcomes::new(metric_stats, outcome_aggregator.clone());

        let mut project = create_project(Some(json!({
            "quotas": [{
               "id": "foo",
               "categories": ["transaction"],
               "window": 3600,
               "limit": 0,
               "reasonCode": "foo",
           }]
        })));
        let cb = project.check_buckets(
            &metric_outcomes,
            &outcome_aggregator,
            vec![create_metric("d:transactions/foo")],
        );

        assert!(matches!(cb, CheckedBuckets::Dropped));

        drop(metric_outcomes);
        assert!(metric_stats_rx.blocking_recv().is_none());
    }

    #[test]
    fn test_check_buckets_rate_limit_mixed_no_quota() {
        let (outcome_aggregator, _) = Addr::custom();
        let (metric_stats, mut metric_stats_rx) = MetricStats::test();
        let metric_outcomes = MetricOutcomes::new(metric_stats, outcome_aggregator.clone());

        let mut project = create_project(Some(json!({
            "quotas": [{
               "id": "foo",
               "categories": ["transaction"],
               "window": 3600,
               "limit": 0,
               "reasonCode": "foo",
           }]
        })));
        let cb = project.check_buckets(
            &metric_outcomes,
            &outcome_aggregator,
            vec![
                create_metric("d:transactions/foo"),
                create_metric("d:profiles/foo"),
            ],
        );

        match cb {
            CheckedBuckets::Checked {
                scoping,
                project_info: _,
                buckets,
            } => {
                assert_eq!(scoping, project.scoping().unwrap());
                assert_eq!(buckets, vec![create_metric("d:profiles/foo")])
            }
            cb => panic!("{cb:?}"),
        }

        drop(metric_outcomes);
        assert!(metric_stats_rx.blocking_recv().is_none());
    }

    #[test]
    fn test_check_buckets_project_state_filter() {
        let (outcome_aggregator, _) = Addr::custom();
        let (metric_stats, mut metric_stats_rx) = MetricStats::test();
        let metric_outcomes = MetricOutcomes::new(metric_stats, outcome_aggregator.clone());

        let mut project = create_project(None);
        let cb = project.check_buckets(
            &metric_outcomes,
            &outcome_aggregator,
            vec![create_metric("d:custom/foo")],
        );

        assert!(matches!(cb, CheckedBuckets::Dropped));

        drop(metric_outcomes);
        let value = metric_stats_rx.blocking_recv().unwrap();
        let Aggregator::MergeBuckets(merge_buckets) = value else {
            panic!();
        };
        assert_eq!(merge_buckets.buckets().len(), 1);
        assert!(metric_stats_rx.blocking_recv().is_none());
    }
}
