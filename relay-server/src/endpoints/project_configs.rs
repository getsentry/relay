use std::collections::HashMap;

use actix::prelude::*;
use actix_web::{Error, Json};
use futures::{future, Future};
use serde::Serialize;

use relay_common::ProjectId;

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
    pub fn to_wrapped(ps: ProjectState, is_internal: bool) -> Self {
        if is_internal {
            Self::Full(ps)
        } else {
            Self::Limited(ps)
        }
    }
}

#[derive(Debug, Serialize)]
struct GetProjectStatesResponseWrapper {
    configs: HashMap<ProjectId, Option<ProjectStateWrapper>>,
}

#[allow(clippy::needless_pass_by_value)]
fn get_project_configs(
    state: CurrentServiceState,
    body: SignedJson<GetProjectStates>,
) -> ResponseFuture<Json<GetProjectStatesResponseWrapper>, Error> {
    let relay_info = body.relay_info;
    let is_internal = relay_info.internal && body.inner.is_full_config();

    let futures = body.inner.projects.into_iter().map(move |project_id| {
        let relay_info = relay_info.clone();
        state
            .project_cache()
            .send(GetProject { id: project_id })
            .map_err(Error::from)
            .and_then(|project| project.send(GetProjectState).map_err(Error::from))
            .map(move |project_state| {
                let project_state = project_state.ok()?;
                // If public key is known (even if rate-limited, which is Some(false)), it has
                // access to the project config
                if project_state
                    .config
                    .trusted_relays
                    .contains(&relay_info.public_key)
                {
                    Some((*project_state).clone())
                } else {
                    log::debug!(
                        "Public key {} does not have access to project {}",
                        relay_info.public_key,
                        project_id
                    );
                    None
                }
            })
            .map(move |project_state| (project_id, project_state))
    });

    Box::new(future::join_all(futures).map(move |mut project_states| {
        let configs = project_states
            .drain(..)
            .filter(|(_, state)| !state.as_ref().map_or(false, |s| s.invalid()))
            .map(|(id, state)| match state {
                None => (id, None),
                Some(state) => (
                    id,
                    Some(ProjectStateWrapper::to_wrapped(state, is_internal)),
                ),
            })
            .collect();

        Json(GetProjectStatesResponseWrapper { configs })
    }))
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/projectconfigs/", |r| {
        r.name("relay-projectconfigs");
        r.post().with(get_project_configs);
    })
}
