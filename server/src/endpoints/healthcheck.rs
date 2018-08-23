//! A simple healthcheck endpoint for the relay.
use actix::ResponseFuture;

use actix_web::{http::Method, Error, HttpResponse};

use futures::Future;

use extractors::CurrentServiceState;
use service::ServiceApp;

use actors::upstream::IsAuthenticated;

#[derive(Serialize)]
struct HealthcheckResponse {
    is_healthy: bool,
}

#[cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value))]
fn healthcheck(state: CurrentServiceState) -> ResponseFuture<HttpResponse, Error> {
    Box::new(
        state
            .upstream_relay()
            .send(IsAuthenticated)
            .map_err(|_| ())
            .and_then(move |is_authenticated| {
                if is_authenticated {
                    Ok(HttpResponse::Ok().json(HealthcheckResponse { is_healthy: true }))
                } else {
                    Err(())
                }
            })
            .or_else(|_| {
                Ok(HttpResponse::ServiceUnavailable()
                    .json(HealthcheckResponse { is_healthy: false }))
            }),
    )
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/relay/healthcheck/", |r| {
        r.method(Method::GET).with(healthcheck);
    })
}
