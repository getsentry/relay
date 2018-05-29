//! A simple healthcheck endpoint for the relay.
use actix_web::{http::Method, HttpResponse};

use extractors::CurrentTroveState;
use service::ServiceApp;

#[derive(Serialize)]
struct HealthcheckResponse {
    is_healthy: bool,
}

fn healthcheck(state: CurrentTroveState) -> HttpResponse {
    let resp = HealthcheckResponse {
        is_healthy: state.is_healthy(),
    };

    if resp.is_healthy {
        HttpResponse::Ok()
    } else {
        HttpResponse::ServiceUnavailable()
    }.json(resp)
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/relay/healthcheck/", |r| {
        r.method(Method::GET).with(healthcheck);
    })
}
