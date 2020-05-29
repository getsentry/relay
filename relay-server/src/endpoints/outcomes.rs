use actix_web::HttpResponse;
use serde::{Deserialize, Serialize};

use crate::actors::outcome::TrackRawOutcome;
use crate::extractors::{CurrentServiceState, SignedJson};
use crate::service::ServiceApp;

/// Defines the structure of the HTTP outcomes requests
#[derive(Deserialize, Debug, Default)]
#[serde(default)]
struct SendOutcomes {
    pub outcomes: Vec<TrackRawOutcome>,
}

/// Defines the structure of the HTTP outcomes responses for successful requests
#[derive(Serialize, Debug)]
struct OutcomesResponse {
    // nothing yet, future features will go here
}

fn send_outcomes(state: CurrentServiceState, body: SignedJson<SendOutcomes>) -> HttpResponse {
    if !body.relay.internal || !state.config().emit_outcomes() {
        return HttpResponse::Forbidden().finish();
    }
    for outcome in body.inner.outcomes {
        state.outcome_producer().do_send(outcome);
    }
    HttpResponse::Accepted().json(OutcomesResponse {})
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/outcomes", |r| {
        r.name("relay-outcomes");
        r.post().with(send_outcomes);
    })
}
