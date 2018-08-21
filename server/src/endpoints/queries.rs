use std::rc::Rc;

use actix_web::http::Method;
use actix_web::Error;
use actix_web::Json;

use futures::{future, Future};

use semaphore_aorta::ApiErrorResponse;
use semaphore_aorta::RelayId;

use actors::keys::{GetPublicKeys, GetPublicKeysResult};
use actors::project::{GetProject, GetProjectState, GetProjectStates, GetProjectStatesResponse};
use extractors::CurrentServiceState;
use extractors::SignedJson;
use service::ServiceApp;

#[cfg_attr(feat = "cargo-clippy", allow(needless_pass_by_value))]
fn get_public_keys(
    (state, body): (CurrentServiceState, SignedJson<GetPublicKeys>),
) -> Box<Future<Item = Json<GetPublicKeysResult>, Error = Error>> {
    let res = state.key_manager().send(body.into_inner());

    Box::new(
        res.map_err(Error::from)
            .and_then(|x| x.map_err(Error::from).map(|x| Json(x))),
    )
}

#[cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value))]
fn get_project_configs(
    (state, body): (CurrentServiceState, SignedJson<GetProjectStates>),
) -> Box<Future<Item = Json<GetProjectStatesResponse>, Error = Error>> {
    let public_key = Rc::new(format!("{}", body.public_key()));
    let futures = body
        .into_inner()
        .projects
        .into_iter()
        .map(move |project_id| {
            let public_key = public_key.clone();
            state
                .project_manager()
                .send(GetProject {
                    id: project_id.clone(),
                })
                .map_err(Error::from)
                .and_then(|project| project.send(GetProjectState).map_err(Error::from))
                .map(move |project_state| {
                    let project_state = project_state.ok()?;
                    // If public key is known (even if rate-limited, which is Some(false)), it has
                    // access to the project config
                    if project_state.public_keys.contains_key(public_key.as_str()) {
                        Some((*project_state).clone())
                    } else {
                        None
                    }
                })
                .map(move |project_state| (project_id, project_state))
        });

    Box::new(future::join_all(futures).map(|mut project_states| {
        Json(GetProjectStatesResponse {
            configs: project_states.drain(..).collect(),
        })
    }))
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/publickeys/", |r| {
        r.method(Method::POST).with(get_public_keys);
    }).resource("/api/0/relays/projectconfigs/", |r| {
        r.method(Method::POST).with(get_project_configs);
    })
}
