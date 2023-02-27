use actix_web::{App, HttpResponse};
use relay_config::EmitOutcomes;

use crate::actors::outcome::{OutcomeProducer, SendOutcomes, SendOutcomesResponse};
use crate::extractors::{CurrentServiceState, SignedJson};
use crate::service::ServiceState;

fn send_outcomes(state: CurrentServiceState, body: SignedJson<SendOutcomes>) -> HttpResponse {
    if !body.relay.internal || state.config().emit_outcomes() != EmitOutcomes::AsOutcomes {
        return HttpResponse::Forbidden().finish();
    }

    let producer = OutcomeProducer::from_registry();
    for outcome in body.inner.outcomes {
        producer.send(outcome);
    }

    HttpResponse::Accepted().json(SendOutcomesResponse {})
}

pub fn configure_app(app: App<ServiceState>) -> App<ServiceState> {
    app.resource("/api/0/relays/outcomes/", |r| {
        r.name("relay-outcomes");
        r.post().with(send_outcomes);
    })
}
