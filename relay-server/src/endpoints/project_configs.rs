use std::collections::HashMap;

use actix::prelude::*;
use actix_web::{Error, Json};
use futures::{future, Future};
use serde::{Deserialize, Serialize};

use relay_common::ProjectKey;

use crate::actors::project::{LimitedProjectState, ProjectState};
use crate::actors::project_cache::{
    GetCachedProjectState, GetProjectState, ProjectCache, UpdateProjectState,
};
use crate::extractors::SignedJson;
use crate::project_states_version;
use crate::service::ServiceApp;
use crate::utils::ErrorBoundary;

/// V2 version of this endpoint.
///
/// The request is a list of [`ProjectKey`]s and the response a list of [`ProjectStateWrapper`]s
const ENDPOINT_V2: u16 = 2;

/// V3 version of this endpoint.
///
/// This version allows returning "pending" project configs, so anything in the cache can be
/// returned directly.  The pending projectconfigs will also be fetched from upstream, so
/// next time a downstream relay polls for this it is hopefully in our cache and will be
/// returned, or a further poll ensues.
const ENDPOINT_V3: u16 = project_states_version!();

/// Helper to deserialize the `version` query parameter.
#[derive(Debug, Deserialize)]
struct VersionQuery {
    version: u16,
}

/// Checks for a specific `version` query parameter.
struct VersionPredicate(u16);

impl<S> actix_web::pred::Predicate<S> for VersionPredicate {
    fn check(&self, req: &actix_web::Request, _: &S) -> bool {
        let query = req.uri().query().unwrap_or("");
        serde_urlencoded::from_str::<VersionQuery>(query)
            .map(|query| query.version == self.0)
            .unwrap_or(false)
    }
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
/// Either the project config is returned or `None` in case the requesting relay did not
/// have permission for the config, or the config does not exist.  The latter happens if the
/// request was made by an external relay who's public key is not configured as authorised
/// on the project.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GetProjectStatesResponseWrapper {
    configs: HashMap<ProjectKey, Option<ProjectStateWrapper>>,
}

/// The response type to the V3 request.
///
/// See [`GetProjectStatesResponseWrapper`] for details of the V2 request, this version also
/// adds a list of projects who's response is pending.  A [`ProjectKey`] should never be in
/// both collections.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GetProjectStatesResponseWrapperWithPending {
    configs: HashMap<ProjectKey, Option<ProjectStateWrapper>>,
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

fn get_project_configs(
    body: SignedJson<GetProjectStatesRequest>,
) -> ResponseFuture<Json<GetProjectStatesResponseWrapper>, Error> {
    let relay = body.relay;
    let full = relay.internal && body.inner.full_config;
    let no_cache = body.inner.no_cache;

    // Skip unparsable public keys. The downstream Relay will consider them `ProjectState::missing`.
    let valid_keys = body.inner.public_keys.into_iter().filter_map(|e| e.ok());

    let futures = valid_keys.map(move |project_key| {
        let relay = relay.clone();
        // TODO: This somewhere returns a relay_server::utils::actix::Response and we need
        // to only get the Ready variants and return pending for the Future variants.
        ProjectCache::from_registry()
            .send(GetProjectState::new(project_key).no_cache(no_cache))
            .map_err(Error::from)
            .map(move |project_state| {
                let project_state = project_state.ok()?;
                // If public key is known (even if rate-limited, which is Some(false)), it has
                // access to the project config
                if relay.internal
                    || project_state
                        .config
                        .trusted_relays
                        .contains(&relay.public_key)
                {
                    Some((*project_state).clone())
                } else {
                    relay_log::debug!(
                        "Relay {} does not have access to project key {}",
                        relay.public_key,
                        project_key
                    );
                    None
                }
            })
            .map(move |project_state| (project_key, project_state))
    });

    Box::new(future::join_all(futures).map(move |mut project_states| {
        let configs = project_states
            .drain(..)
            .filter(|(_, state)| !state.as_ref().map_or(false, |s| s.invalid()))
            .map(|(key, state)| (key, state.map(|s| ProjectStateWrapper::new(s, full))))
            .collect();

        Json(GetProjectStatesResponseWrapper { configs })
    }))
}

enum StateResult {
    Cached(ProjectKey, Option<ProjectState>),
    Pending(ProjectKey),
}

fn get_project_configs_with_pending(
    body: SignedJson<GetProjectStatesRequest>,
) -> ResponseFuture<Json<GetProjectStatesResponseWrapperWithPending>, Error> {
    let relay = body.relay;
    let full = relay.internal && body.inner.full_config;
    let no_cache = body.inner.no_cache;

    // Skip unparsable public keys. The downstream Relay will consider them `ProjectState::missing`.
    let valid_keys = body.inner.public_keys.into_iter().filter_map(|e| e.ok());

    let futures = valid_keys.map(move |project_key| {
        let relay = relay.clone();
        if !no_cache {
            ProjectCache::from_registry().do_send(UpdateProjectState::new(project_key, no_cache));
            // TODO: maybe future::Either to fix this up?
            future::ok(StateResult::Pending(project_key))
        } else {
            ProjectCache::from_registry()
                .send(GetCachedProjectState::new(project_key))
                .map_err(Error::from)
                .map(move |maybe_state| {
                    match maybe_state {
                        Some(project_state) => {
                            // If public key is known (even if rate-limited, which is
                            // Some(false)), it has access to the project config
                            let project_state = if relay.internal
                                || project_state
                                    .config
                                    .trusted_relays
                                    .contains(&relay.public_key)
                            {
                                Some((*project_state).clone())
                            } else {
                                relay_log::debug!(
                                    "Relay {} does not have access to project key {}",
                                    relay.public_key,
                                    project_key
                                );
                                None
                            };
                            StateResult::Cached(project_key, project_state)
                        }
                        None => {
                            ProjectCache::from_registry()
                                .do_send(UpdateProjectState::new(project_key, no_cache));
                            StateResult::Pending(project_key)
                        }
                    }
                })
        }
    });

    Box::new(future::join_all(futures).map(move |mut project_states| {
        let mut configs = HashMap::with_capacity(body.inner.public_keys.len());
        let mut pending = Vec::with_capacity(body.inner.public_keys.len());
        for state_result in project_states {
            match state_result {
                StateResult::Cached(project_key, project_state) => {
                    if project_state.map_or(false, |s| s.invalid()) {
                        continue;
                    }
                    let project_state = ProjectStateWrapper::new(project_state, full);
                    configs.insert(project_key, project_state);
                }
                StateResult::Pending(project_key) => pending.push(project_key),
            }
        }
        Json(GetProjectStatesResponseWrapperWithPending { configs, pending })
    }))
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/projectconfigs/", |r| {
        r.name("relay-projectconfigs");
        r.post()
            .filter(VersionPredicate(ENDPOINT_V2))
            .with(get_project_configs);
        r.post()
            .filter(VersionPredicate(ENDPOINT_V3))
            .with(get_project_configs_with_pending);

        // Forward all unsupported versions to the upstream.
        r.post().f(crate::endpoints::forward::forward_upstream);
    })
}
