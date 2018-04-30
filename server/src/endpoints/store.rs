//! Handles event store requests.
use actix_web::{HttpResponse, Json, ResponseError, http::Method};
use uuid::Uuid;

use service::ServiceApp;
use extractors::ProjectRequest;
use middlewares::ForceJson;

use smith_aorta::{ApiErrorResponse, EventV7, EventVariant};

#[derive(Serialize)]
struct StoreResponse {
    id: Uuid,
}

#[derive(Fail, Debug)]
#[fail(display = "event submission rejected (invalid or disabled public key)")]
struct StoreRejected;

impl ResponseError for StoreRejected {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::Forbidden().json(&ApiErrorResponse::from_fail(self))
    }
}

fn store(mut request: ProjectRequest<EventV7>) -> Result<Json<StoreResponse>, StoreRejected> {
    let trove_state = request.trove_state();
    let mut event = EventVariant::SentryV7(request.take_payload().unwrap());
    let event_id = event.ensure_id().unwrap_or_else(Uuid::nil);
    let project_state = trove_state.get_or_create_project_state(request.project_id());

    if project_state.handle_event(request.auth().public_key().into(), event) {
        Ok(Json(StoreResponse { id: event_id }))
    } else {
        Err(StoreRejected)
    }
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource(r"/api/{project:\d+}/store/", |r| {
        r.middleware(ForceJson);
        r.method(Method::POST).with(store);
    })
}
