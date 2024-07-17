use chrono::{DateTime, Utc};
use relay_base_schema::project::{ProjectId, ProjectKey};
#[cfg(feature = "processing")]
use relay_cardinality::CardinalityLimit;
use relay_config::Config;
#[cfg(feature = "processing")]
use relay_dynamic_config::ErrorBoundary;
use relay_dynamic_config::{Feature, LimitedProjectConfig, ProjectConfig};
use relay_filter::matches_any_origin;
use relay_quotas::{Quota, Scoping};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use url::Url;

use crate::envelope::Envelope;
use crate::extractors::RequestMeta;
use crate::services::outcome::DiscardReason;
use crate::services::project::PublicKeyConfig;

/// Information about an enabled project.
///
/// Contains the project config plus metadata (organization_id, project_id, etc.).
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

    /// Determines whether the given envelope should be accepted or discarded.
    ///
    /// Returns `Ok(())` if the envelope should be accepted. Returns `Err(DiscardReason)` if the
    /// envelope should be discarded, by indicating the reason. The checks preformed for this are:
    ///
    ///  - Allowed origin headers
    ///  - Disabled or unknown projects
    ///  - Disabled project keys (DSN)
    ///  - Feature flags
    pub fn check_envelope(
        &self,
        envelope: &Envelope,
        config: &Config,
    ) -> Result<(), DiscardReason> {
        // Verify that the stated project id in the DSN matches the public key used to retrieve this
        // project state.
        let meta = envelope.meta();
        if !self.is_valid_project_id(meta.project_id(), config) {
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

        // Check feature.
        if let Some(disabled_feature) = envelope
            .required_features()
            .iter()
            .find(|f| !self.has_feature(**f))
        {
            return Err(DiscardReason::FeatureDisabled(*disabled_feature));
        }

        Ok(())
    }

    /// Returns `true` if the given project ID matches this project.
    ///
    /// If the project state has not been loaded, this check is skipped because the project
    /// identifier is not yet known. Likewise, this check is skipped for the legacy store endpoint
    /// which comes without a project ID. The id is later overwritten in `check_envelope`.
    fn is_valid_project_id(&self, stated_id: Option<ProjectId>, config: &Config) -> bool {
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
    fn is_matching_key(&self, project_key: ProjectKey) -> bool {
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
    /// [`Project::scoping`](crate::services::project::Project::scoping) instead.
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
