use std::collections::HashMap;

use axum::extract::Query;
use axum::response::Result;
use axum::routing::post;
use axum::{Json, Router};
use futures::future;
use relay_common::ProjectKey;
use relay_dynamic_config::ErrorBoundary;
use serde::{Deserialize, Serialize};

use crate::actors::project::{LimitedProjectState, ProjectState};
use crate::actors::project_cache::{GetCachedProjectState, GetProjectState, ProjectCache};
use crate::extractors::SignedJson;

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

// impl VersionQuery {
//     fn from_request(req: &actix_web::Request) -> Self {
//         let query = req.uri().query().unwrap_or("");
//         serde_urlencoded::from_str::<VersionQuery>(query).unwrap_or(VersionQuery { version: 0 })
//     }
// }

// impl<S> FromRequest<S> for VersionQuery {
//     type Config = ();
//     type Result = Self;

//     fn from_request(req: &actix_web::HttpRequest<S>, _: &Self::Config) -> Self::Result {
//         Self::from_request(req)
//     }
// }

// /// Checks for a specific `version` query parameter.
// struct VersionPredicate;

// impl<S> actix_web::pred::Predicate<S> for VersionPredicate {
//     fn check(&self, req: &actix_web::Request, _: &S) -> bool {
//         let query = VersionQuery::from_request(req);
//         query.version >= ENDPOINT_V2 && query.version <= ENDPOINT_V3
//     }
// }

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
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GetProjectStatesResponseWrapper {
    configs: HashMap<ProjectKey, Option<ProjectStateWrapper>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pending: Vec<ProjectKey>,
}

/// Request payload of the project config endpoint.
///
/// This is a replica of [`GetProjectStates`](crate::actors::project_upstream::GetProjectStates)
/// which allows skipping invalid project keys.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetProjectStatesRequest {
    public_keys: Vec<ErrorBoundary<ProjectKey>>,
    #[serde(default)]
    full_config: bool,
    #[serde(default)]
    no_cache: bool,
}

async fn get_project_configs(
    Query(version): Query<VersionQuery>,
    body: SignedJson<GetProjectStatesRequest>,
) -> Result<Json<GetProjectStatesResponseWrapper>> {
    let SignedJson { inner, relay } = body;

    let no_cache = inner.no_cache;
    let keys_len = inner.public_keys.len();

    // Skip unparsable public keys. The downstream Relay will consider them `ProjectState::missing`.
    let valid_keys = inner.public_keys.into_iter().filter_map(|e| e.ok());
    let futures = valid_keys.map(|project_key| async move {
        let state_result = if version.version >= ENDPOINT_V3 && !no_cache {
            ProjectCache::from_registry()
                .send(GetCachedProjectState::new(project_key))
                .await
        } else {
            ProjectCache::from_registry()
                .send(GetProjectState::new(project_key).no_cache(no_cache))
                .await
                .map(Some)
        };

        (project_key, state_result)
    });

    let mut configs = HashMap::with_capacity(keys_len);
    let mut pending = Vec::with_capacity(keys_len);

    for (project_key, state_result) in future::join_all(futures).await {
        let Some(project_state) = state_result.map_err(|_| ())? else { // TODO(ja): Error handling
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
                "Relay {} does not have access to project key {}",
                relay.public_key,
                project_key
            );
        };
    }

    Ok(Json(GetProjectStatesResponseWrapper { configs, pending }))
}

// pub fn configure_app(app: App<ServiceState>) -> App<ServiceState> {
//     app.resource("/api/0/relays/projectconfigs/", |r| {
//         r.name("relay-projectconfigs");
//         r.post()
//             .filter(VersionPredicate)
//             .with_async(|b, v| Box::pin(get_project_configs(b, v)).compat());

//         // Forward all unsupported versions to the upstream.
//         r.post().f(crate::endpoints::forward::forward_compat);
//     })
// }

pub fn routes<S>() -> Router<S> {
    // TODO(ja): Check version predicate.
    // TODO(ja): forward if version is incompatible.
    // r.name("relay-projectconfigs");
    Router::new()
        .route("/api/0/relays/projectconfigs/", post(get_project_configs))
        .with_state(())
}
