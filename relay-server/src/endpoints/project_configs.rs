use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Query, Request};
use axum::handler::Handler;
use axum::response::{IntoResponse, Result};
use axum::{Json, RequestExt};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use relay_base_schema::project::ProjectKey;
use relay_dynamic_config::{ErrorBoundary, GlobalConfig};
use serde::{Deserialize, Serialize};

use crate::endpoints::common::ServiceUnavailable;
use crate::endpoints::forward;
use crate::extractors::SignedJson;
use crate::service::ServiceState;
use crate::services::global_config::{self, StatusResponse};
use crate::services::project::{LimitedParsedProjectState, ParsedProjectState, ProjectState};
use crate::services::project_cache::{GetCachedProjectState, GetProjectState};

/// V2 version of this endpoint.
///
/// The request is a list of [`ProjectKey`]s and the response a list of [`ProjectStateWrapper`]s
const ENDPOINT_V2: u16 = 2;

/// V3 version of this endpoint.
///
/// This version allows returning `pending` project configs, so anything in the cache can be
/// returned directly.  The pending projectconfigs will also be fetched from upstream, so
/// next time a downstream relay polls for this it is hopefully in our cache and will be
/// returned, or a further poll ensues.
const ENDPOINT_V3: u16 = 3;

/// Helper to deserialize the `version` query parameter.
#[derive(Clone, Copy, Debug, Deserialize)]
struct VersionQuery {
    #[serde(default)]
    version: u16,
}

/// The type returned for each requested project config.
///
/// Wrapper on top the project state which encapsulates information about how ProjectState
/// should be deserialized. The `Limited` deserializes using a class with a subset of the
/// original fields.
///
/// Full configs are only returned to internal relays which also requested the full config.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum ProjectStateWrapper {
    Full(ParsedProjectState),
    Limited(#[serde(with = "LimitedParsedProjectState")] ParsedProjectState),
}

impl ProjectStateWrapper {
    /// Create a wrapper which forces serialization into external or internal format
    pub fn new(state: ParsedProjectState, full: bool) -> Self {
        if full {
            Self::Full(state)
        } else {
            Self::Limited(state)
        }
    }
}

/// The response type to the V2 request.
///
/// Either the project config is returned or `None` in case the requesting Relay did not have
/// permission for the config, or the config does not exist.  The latter happens if the request was
/// made by an external relay who's public key is not configured as authorised on the project.
///
/// Version 3 also adds a list of projects whose response is pending.  A [`ProjectKey`] should never
/// be in both collections. This list is always empty before V3. If `global` is
/// enabled, version 3 also responds with [`GlobalConfig`].
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GetProjectStatesResponseWrapper {
    configs: HashMap<ProjectKey, ProjectStateWrapper>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pending: Vec<ProjectKey>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    unchanged: Vec<ProjectKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    global: Option<Arc<GlobalConfig>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    global_status: Option<StatusResponse>,
}

/// Request payload of the project config endpoint.
///
/// This is a replica of [`GetProjectStates`](crate::services::project_upstream::GetProjectStates)
/// which allows skipping invalid project keys.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetProjectStatesRequest {
    /// The list of all requested project configs.
    public_keys: Vec<ErrorBoundary<ProjectKey>>,
    /// List of revisions for all project configs.
    ///
    /// This length of this list if specified must be the same length
    /// as [`Self::public_keys`], the items are asssociated by index.
    revisions: Option<ErrorBoundary<Vec<Option<String>>>>,
    #[serde(default)]
    full_config: bool,
    #[serde(default)]
    no_cache: bool,
    #[serde(default)]
    global: bool,
}

fn into_valid_keys(
    public_keys: Vec<ErrorBoundary<ProjectKey>>,
    revisions: Option<ErrorBoundary<Vec<Option<String>>>>,
) -> impl Iterator<Item = (ProjectKey, Option<String>)> {
    let mut revisions = revisions.and_then(|e| e.ok()).unwrap_or_default();
    if !revisions.is_empty() && revisions.len() != public_keys.len() {
        // The downstream sent us a different amount of revisions than project keys,
        // this indicates an error in the downstream code. Just to be safe, discard
        // all revisions and carry on as if the downstream never sent any revisions.
        relay_log::warn!(
            "downstream sent {} project keys but {} revisions, discarding all revisions",
            public_keys.len(),
            revisions.len()
        );
        revisions.clear();
    }
    let revisions = revisions.into_iter().chain(std::iter::repeat(None));

    std::iter::zip(public_keys, revisions).filter_map(|(public_key, revision)| {
        // Skip unparsable public keys.
        // The downstream Relay will consider them `ProjectState::missing`.
        let public_key = public_key.ok()?;
        Some((public_key, revision))
    })
}

async fn inner(
    state: ServiceState,
    Query(version): Query<VersionQuery>,
    body: SignedJson<GetProjectStatesRequest>,
) -> Result<impl IntoResponse, ServiceUnavailable> {
    let SignedJson { inner, relay } = body;
    let project_cache = &state.project_cache().clone();

    let no_cache = inner.no_cache;
    let keys_len = inner.public_keys.len();

    let mut futures: FuturesUnordered<_> = into_valid_keys(inner.public_keys, inner.revisions)
        .map(|(project_key, revision)| async move {
            let state_result = if version.version >= ENDPOINT_V3 && !no_cache {
                project_cache
                    .send(GetCachedProjectState::new(project_key))
                    .await
            } else {
                project_cache
                    .send(GetProjectState::new(project_key).no_cache(no_cache))
                    .await
            };

            (project_key, revision, state_result)
        })
        .collect();

    let (global, global_status) = if inner.global {
        match state.global_config().send(global_config::Get).await? {
            global_config::Status::Ready(config) => (Some(config), Some(StatusResponse::Ready)),
            // Old relays expect to get a global config no matter what, even if it's not ready
            // yet. We therefore give them a default global config.
            global_config::Status::Pending => (
                Some(GlobalConfig::default().into()),
                Some(StatusResponse::Pending),
            ),
        }
    } else {
        (None, None)
    };

    let mut pending = Vec::with_capacity(keys_len);
    let mut unchanged = Vec::with_capacity(keys_len);
    let mut configs = HashMap::with_capacity(keys_len);

    while let Some((project_key, revision, state_result)) = futures.next().await {
        let project_info = match state_result? {
            ProjectState::Enabled(info) => info,
            ProjectState::Disabled => {
                // Don't insert project config. Downstream Relay will consider it disabled.
                continue;
            }
            ProjectState::Pending => {
                pending.push(project_key);
                continue;
            }
        };

        // Only ever omit responses when there was a valid revision in the first place.
        if revision.is_some() && project_info.rev == revision {
            unchanged.push(project_key);
            continue;
        }

        // If public key is known (even if rate-limited, which is Some(false)), it has
        // access to the project config
        let has_access = relay.internal
            || project_info
                .config
                .trusted_relays
                .contains(&relay.public_key);

        if has_access {
            let full = relay.internal && inner.full_config;
            let wrapper = ProjectStateWrapper::new(
                ParsedProjectState {
                    disabled: false,
                    info: project_info.as_ref().clone(),
                },
                full,
            );
            configs.insert(project_key, wrapper);
        } else {
            relay_log::debug!(
                relay = %relay.public_key,
                project_key = %project_key,
                "relay does not have access to project key",
            );
        };
    }

    Ok(Json(GetProjectStatesResponseWrapper {
        configs,
        pending,
        unchanged,
        global,
        global_status,
    }))
}

/// Returns `true` if the `?version` query parameter is compatible with this implementation.
fn is_compatible(Query(query): Query<VersionQuery>) -> bool {
    query.version >= ENDPOINT_V2 && query.version <= ENDPOINT_V3
}

/// Endpoint handler for the project configs endpoint.
///
/// # Version Compatibility
///
/// This endpoint checks a `?version` query parameter for compatibility. If this implementation is
/// compatible with the version requested by the client (downstream Relay), it runs the project
/// config endpoint implementation. Otherwise, the request is forwarded to the upstream.
///
/// Relays can drop compatibility with old versions of the project config endpoint, for instance the
/// initial version 1. However, Sentry's HTTP endpoint will retain compatibility for much longer to
/// support old Relay versions.
pub async fn handle(state: ServiceState, mut req: Request) -> Result<impl IntoResponse> {
    let data = req.extract_parts().await?;
    Ok(if is_compatible(data) {
        inner.call(req, state).await
    } else {
        forward::forward(state, req).await
    })
}
