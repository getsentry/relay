use std::collections::HashMap;

use actix::prelude::*;
use actix_web::{Error, Json};
use futures::{future, Future};
use serde::{Deserialize, Serialize};

use relay_common::ProjectKey;

use crate::actors::project::{LimitedProjectState, ProjectState};
use crate::actors::project_cache::{GetProjectState, ProjectCache};
use crate::extractors::SignedJson;
use crate::service::ServiceApp;
use crate::utils::ErrorBoundary;

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

/// Wrapper on top the project state which encapsulates information about how ProjectState
/// should be deserialized. The `Limited` deserializes using a class with a subset of the
/// original fields.
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GetProjectStatesResponseWrapper {
    configs: HashMap<ProjectKey, Option<ProjectStateWrapper>>,
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

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/projectconfigs/", |r| {
        r.name("relay-projectconfigs");
        r.post()
            .filter(VersionPredicate(crate::project_states_version!()))
            .with(get_project_configs);

        // Forward all unsupported versions to the upstream.
        r.post().f(crate::endpoints::forward::forward_upstream);
    })
}
