use std::collections::HashMap;

use axum::extract::Query;
use axum::handler::Handler;
use axum::http::Request;
use axum::response::{IntoResponse, Result};
use axum::{Json, RequestExt};
use futures::future;
use relay_common::ProjectKey;
use relay_dynamic_config::{ErrorBoundary, GlobalConfig};
use serde::{Deserialize, Serialize};

use crate::actors::global_config::GetGlobalConfig;
use crate::actors::project::{LimitedProjectState, ProjectState};
use crate::actors::project_cache::{GetCachedProjectState, GetProjectState};
use crate::endpoints::common::ServiceUnavailable;
use crate::endpoints::forward;
use crate::extractors::SignedJson;
use crate::service::ServiceState;

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
    Full(ProjectState),
    Limited(#[serde(with = "LimitedProjectState")] ProjectState),
}

impl ProjectStateWrapper {
    /// Create a wrapper which forces serialization into external or internal format
    pub fn new(state: ProjectState, full: bool) -> Self {
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
/// be in both collections.  This list is always empty before V3.
///
/// The response may also have a [`GlobalConfig`] which should be returned if the `global_config`
/// flag is enabled on [`GetProjectStatesRequest`]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GetProjectStatesResponseWrapper {
    configs: HashMap<ProjectKey, Option<ProjectStateWrapper>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pending: Vec<ProjectKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    global_config: Option<GlobalConfig>,
}

/// Request payload of the project config endpoint.
///
/// This is a replica of [`GetProjectStates`](crate::actors::project_upstream::GetProjectStates)
/// which allows skipping invalid project keys.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetProjectStatesRequest {
    public_keys: Vec<ErrorBoundary<ProjectKey>>,
    #[serde(default)]
    full_config: bool,
    #[serde(default)]
    no_cache: bool,
    #[serde(default)]
    global_config: bool,
}

async fn inner(
    state: ServiceState,
    Query(version): Query<VersionQuery>,
    body: SignedJson<GetProjectStatesRequest>,
) -> Result<impl IntoResponse, ServiceUnavailable> {
    let SignedJson { inner, relay } = body;
    let project_cache = &state.project_cache().clone();
    let global_configuration_service = &state.global_configuration().clone();

    let no_cache = inner.no_cache;
    let keys_len = inner.public_keys.len();

    // Skip unparsable public keys. The downstream Relay will consider them `ProjectState::missing`.
    let valid_keys = inner.public_keys.into_iter().filter_map(|e| e.ok());
    let futures = valid_keys.map(|project_key| async move {
        let state_result = if version.version >= ENDPOINT_V3 && !no_cache {
            project_cache
                .send(GetCachedProjectState::new(project_key))
                .await
        } else {
            project_cache
                .send(GetProjectState::new(project_key).no_cache(no_cache))
                .await
                .map(Some)
        };

        (project_key, state_result)
    });

    let mut configs = HashMap::with_capacity(keys_len);
    let mut pending = Vec::with_capacity(keys_len);
    let global_config = if inner.global_config {
        global_configuration_service
            .send(GetGlobalConfig)
            .await
            .ok()
            .map(|gc| (*gc).clone())
    } else {
        None
    };

    for (project_key, state_result) in future::join_all(futures).await {
        let Some(project_state) = state_result? else {
            pending.push(project_key);
            continue;
        };

        // If public key is known (even if rate-limited, which is Some(false)), it has
        // access to the project config
        let has_access = relay.internal
            || project_state
                .config
                .trusted_relays
                .contains(&relay.public_key);

        if has_access {
            let full = relay.internal && inner.full_config;
            let wrapper = ProjectStateWrapper::new((*project_state).clone(), full);
            configs.insert(project_key, Some(wrapper));
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
        global_config,
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
pub async fn handle<B>(state: ServiceState, mut req: Request<B>) -> Result<impl IntoResponse>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<axum::BoxError>,
{
    let data = req.extract_parts().await?;
    Ok(if is_compatible(data) {
        inner.call(req, state).await
    } else {
        forward::forward(state, req).await
    })
}
