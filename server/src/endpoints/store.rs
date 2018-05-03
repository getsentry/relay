//! Handles event store requests.
use std::sync::Arc;

use actix_web::{HttpResponse, Json, ResponseError, http::Method};
use actix_web::middleware::cors::Cors;
use uuid::Uuid;

use service::ServiceApp;
use extractors::{IncomingEvent, IncomingForeignEvent, ProjectRequest};
use middlewares::ForceJson;

use smith_aorta::{ApiErrorResponse, ProjectState, StoreChangeset};

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

fn store(
    changeset: StoreChangeset,
    state: Arc<ProjectState>,
) -> Result<Json<StoreResponse>, StoreRejected> {
    let event_id = changeset.event.id();
    if state.store_changeset(changeset) {
        Ok(Json(StoreResponse { id: event_id }))
    } else {
        Err(StoreRejected)
    }
}

fn store_json_event(
    mut request: ProjectRequest<IncomingEvent>,
) -> Result<Json<StoreResponse>, StoreRejected> {
    store(
        request.take_payload().into(),
        request.get_or_create_project_state(),
    )
}

fn store_foreign_event(
    mut request: ProjectRequest<IncomingForeignEvent>,
) -> Result<Json<StoreResponse>, StoreRejected> {
    store(
        request.take_payload().into(),
        request.get_or_create_project_state(),
    )
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    let app = Cors::for_app(app)
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
            r.method(Method::POST).with(store_json_event);
        })
        .register();

    let app = Cors::for_app(app)
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
        .resource(r"/api/{project:\d+}/{store_type:[a-z][a-z0-9-]*}/", |r| {
            r.method(Method::POST).with(store_foreign_event);
        })
        .register();

    app
}
