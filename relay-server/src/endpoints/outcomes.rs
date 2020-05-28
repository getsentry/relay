use actix::ResponseFuture;
use actix_web::{HttpResponse, ResponseError};
use failure::Fail;
use futures::prelude::*;
use serde::Deserialize;

use crate::actors::outcome::OutcomePayload;
use crate::extractors::{CurrentServiceState, SignedJson};
use crate::service::ServiceApp;

#[derive(Deserialize, Debug)]
pub struct SendOutcomes {
    pub outcomes: Vec<OutcomePayload>,
}

fn send_outcomes(
    state: CurrentServiceState,
    body: SignedJson<SendOutcomes>,
) -> ResponseFuture<HttpResponse, OutcomeProcessingError> {
    if !body.relay.internal {
        return Box::new(futures::future::ok(HttpResponse::Unauthorized().into()));
    }
    if !state.config().emit_outcomes() {
        return Box::new(futures::future::ok(HttpResponse::Forbidden().into()));
    }
    let requests: Vec<_> = body
        .inner
        .outcomes
        .iter()
        .map(|o| state.outcome_producer().send(o.clone()))
        .collect();

    Box::new(
        futures::future::join_all(requests)
            .map_err(|_| OutcomeProcessingError)
            .map(|_| HttpResponse::Ok().into()),
    )
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/outcomes", |r| {
        r.name("relay-outcomes");
        r.post().with(send_outcomes);
    })
}

#[derive(Fail, Debug)]
#[fail(display = "An error occurred.")]
struct OutcomeProcessingError;

impl ResponseError for OutcomeProcessingError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::InternalServerError().into()
    }
}
