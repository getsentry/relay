use std::collections::HashMap;

use actix::prelude::*;
use actix_web::{Error, Json};
use futures::{future, Future};
use serde::Serialize;

use relay_quotas::ProjectKey;

use crate::actors::project::{GetProjectState, LimitedProjectState, ProjectState};
use crate::actors::project_cache::GetProject;
use crate::actors::project_upstream::GetProjectStates;
use crate::extractors::{CurrentServiceState, SignedJson};
use crate::service::ServiceApp;

/// Wrapper on top the project state which encapsulates information about how ProjectState
/// should be deserialized
#[derive(Debug, Clone, Serialize)]
// the wrapper will always deserialize as an internal ProjectState
// we can manually force it to serialize back into an external ProjectState by
// converting it to an external variant with (to_external)
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
    configs_v2: HashMap<ProjectKey, Option<ProjectStateWrapper>>,
}

#[allow(clippy::needless_pass_by_value)]
fn get_project_configs(
    state: CurrentServiceState,
    body: SignedJson<GetProjectStates>,
) -> ResponseFuture<Json<GetProjectStatesResponseWrapper>, Error> {
    let relay = body.relay;
    let full = relay.internal && body.inner.full_config;

    let futures = body.inner.projects_v2.into_iter().map(move |request_id| {
        let relay = relay.clone();
        state
            .project_cache()
            .send(GetProject {
                id: request_id.project_id,
                public_key: request_id.public_key,
            })
            .map_err(Error::from)
            .and_then(|project| project.send(GetProjectState).map_err(Error::from))
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
                    log::debug!(
                        "Public key {} does not have access to project {}",
                        relay.public_key,
                        request_id
                    );
                    None
                }
            })
            .map(move |project_state| (request_id.public_key, project_state))
    });

    Box::new(future::join_all(futures).map(move |mut project_states| {
        let configs_v2 = project_states
            .drain(..)
            .filter(|(_, state)| !state.as_ref().map_or(false, |s| s.invalid()))
            .map(|(key, state)| (key, state.map(|s| ProjectStateWrapper::new(s, full))))
            .collect();

        Json(GetProjectStatesResponseWrapper { configs_v2 })
    }))
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/projectconfigs/", |r| {
        r.name("relay-projectconfigs");
        r.post().with(get_project_configs);
    })
}
