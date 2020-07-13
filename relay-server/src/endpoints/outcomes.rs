use actix_web::HttpResponse;

use crate::actors::outcome::{SendOutcomes, SendOutcomesResponse};
use crate::extractors::{CurrentServiceState, SignedJson};
use crate::service::ServiceApp;

fn send_outcomes(state: CurrentServiceState, body: SignedJson<SendOutcomes>) -> HttpResponse {
    if !body.relay.internal || !state.config().emit_outcomes() {
        return HttpResponse::Forbidden().finish();
    }
    for outcome in body.inner.outcomes {
        state.outcome_producer().do_send(outcome);
    }
    HttpResponse::Accepted().json(SendOutcomesResponse {})
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/outcomes/", |r| {
        r.name("relay-outcomes");
        r.post().with(send_outcomes);
    })
}
