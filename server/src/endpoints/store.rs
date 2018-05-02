//! Handles event store requests.
use actix_web::{HttpResponse, Json, ResponseError, http::Method};
use actix_web::middleware::cors::Cors;
use uuid::Uuid;

use service::ServiceApp;
use extractors::{Event, ProjectRequest};
use middlewares::ForceJson;

use smith_aorta::ApiErrorResponse;

#[derive(Serialize)]
struct StoreResponse {
    id: Option<Uuid>,
}

#[derive(Fail, Debug)]
#[fail(display = "event submission rejected")]
struct StoreRejected;

impl ResponseError for StoreRejected {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::Forbidden().json(&ApiErrorResponse::from_fail(self))
    }
}

fn store(mut request: ProjectRequest<Event>) -> Result<Json<StoreResponse>, StoreRejected> {
    let (event, meta) = request.take_payload().into_inner();
    let event_id = event.id();
    let project_state = request.get_or_create_project_state();

    if project_state.handle_event(request.auth().public_key().into(), event, meta) {
        Ok(Json(StoreResponse { id: event_id }))
    } else {
        Err(StoreRejected)
    }
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    Cors::for_app(app)
        .allowed_methods(vec!["POST"])
        .allowed_headers(vec![
            "x-sentry-auth",
            "x-requested-with",
            "origin",
            "accept",
            "content-type",
            "authentication",
        ])
        .max_age(3600)
        .resource(r"/api/{project:\d+}/store/", |r| {
            r.middleware(ForceJson);
            r.method(Method::POST).with(store);
        })
        .register()
}
